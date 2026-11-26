use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sestring::SeString;

const START_BYTE: u8 = 0x02;
const END_BYTE: u8 = 0x03;

pub const MAX_SESTRING_BYTES: usize = 4096;
pub const MAX_SESTRING_BASE64_LEN: usize = MAX_SESTRING_BYTES.div_ceil(3) * 4;

fn read_sestring_integer(bytes: &[u8], pos: &mut usize) -> Result<u32, String> {
    let marker = *bytes
        .get(*pos)
        .ok_or_else(|| "unexpected EOF reading packed integer".to_string())?;
    *pos += 1;

    if marker < 0xD0 {
        let value = marker
            .checked_sub(1)
            .ok_or_else(|| "invalid packed integer marker 0x00".to_string())?;
        return Ok(u32::from(value));
    }

    // sestring-0.3.0 computes `(marker + 1) & 0b1111`, which overflows on 0xFF.
    let shifted = marker
        .checked_add(1)
        .ok_or_else(|| "invalid packed integer marker 0xFF".to_string())?;
    let flags = shifted & 0b1111;
    let mut ret = [0u8; 4];
    for i in (0..4).rev() {
        if (flags & (1 << i)) != 0 {
            ret[i] = *bytes
                .get(*pos)
                .ok_or_else(|| "unexpected EOF reading packed integer body".to_string())?;
            *pos += 1;
        }
    }

    Ok(u32::from_le_bytes(ret))
}

fn validate_utf8_field<'a>(
    chunk_slice: &'a [u8],
    pos: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a str, String> {
    let field_end = pos
        .checked_add(len)
        .filter(|&end| end <= chunk_slice.len())
        .ok_or_else(|| format!("truncated sestring {field} field"))?;
    let text = std::str::from_utf8(&chunk_slice[*pos..field_end])
        .map_err(|e| format!("invalid utf-8 in sestring {field}: {e}"))?;
    *pos = field_end;
    Ok(text)
}

fn advance_within_chunk(
    chunk_slice: &[u8],
    pos: &mut usize,
    count: usize,
    context: &str,
) -> Result<(), String> {
    *pos = pos
        .checked_add(count)
        .filter(|&end| end <= chunk_slice.len())
        .ok_or_else(|| format!("truncated sestring {context}"))?;
    Ok(())
}

fn validate_chunk(
    bytes: &[u8],
    start_byte_pos: usize,
    item_names: &mut Vec<Option<String>>,
) -> Result<usize, String> {
    let mut pos = start_byte_pos + 1;
    let chunk_type = *bytes
        .get(pos)
        .ok_or_else(|| "truncated sestring chunk type".to_string())?;
    pos += 1;

    let chunk_len = read_sestring_integer(bytes, &mut pos)? as usize;
    let start = pos;
    let end = start
        .checked_add(chunk_len)
        .ok_or_else(|| "sestring chunk length overflow".to_string())?;
    if end >= bytes.len() {
        return Err("truncated sestring chunk body".to_string());
    }
    if bytes[end] != END_BYTE {
        return Err(format!("missing 0x03 terminator at offset {end}"));
    }

    let chunk_slice = &bytes[..end];
    match chunk_type {
        // NewLine | SeHyphen: no body bytes consumed.
        0x10 | 0x1F => {}
        // Emphasis | UiForeground | UiGlow | Icon: single packed integer.
        0x1A | 0x48 | 0x49 | 0x12 => {
            read_sestring_integer(chunk_slice, &mut pos)?;
        }
        // AutoTranslate: u8 group + packed integer key.
        0x2E => {
            advance_within_chunk(chunk_slice, &mut pos, 1, "auto-translate group")?;
            read_sestring_integer(chunk_slice, &mut pos)?;
        }
        // Interactable: u8 sub_type + subtype-specific payload.
        0x27 => {
            let sub_type = *chunk_slice
                .get(pos)
                .ok_or_else(|| "truncated interactable subtype".to_string())?;
            pos += 1;

            match sub_type {
                // PlayerName: 1 unk + server_id + 2 unk + name_len + utf8 name.
                0x01 => {
                    advance_within_chunk(chunk_slice, &mut pos, 1, "player payload header")?;
                    read_sestring_integer(chunk_slice, &mut pos)?;
                    advance_within_chunk(chunk_slice, &mut pos, 2, "player payload separator")?;
                    let name_len = read_sestring_integer(chunk_slice, &mut pos)? as usize;
                    validate_utf8_field(chunk_slice, &mut pos, name_len, "player name")?;
                }
                // ItemLink: id + optional (3 unk + name_len + utf8 name).
                0x03 => {
                    read_sestring_integer(chunk_slice, &mut pos)?;
                    let item_name = if (pos - start).saturating_add(3) < chunk_len {
                        advance_within_chunk(chunk_slice, &mut pos, 3, "item payload separator")?;
                        let name_len = read_sestring_integer(chunk_slice, &mut pos)? as usize;
                        Some(
                            validate_utf8_field(chunk_slice, &mut pos, name_len, "item name")?
                                .to_string(),
                        )
                    } else {
                        None
                    };
                    item_names.push(item_name);
                }
                // MapPositionLink: packed(territory, map) + raw_x + raw_y + 2 trailer bytes.
                0x04 => {
                    read_sestring_integer(chunk_slice, &mut pos)?;
                    read_sestring_integer(chunk_slice, &mut pos)?;
                    read_sestring_integer(chunk_slice, &mut pos)?;
                    advance_within_chunk(chunk_slice, &mut pos, 2, "map link trailer")?;
                }
                // QuestLink: id + 65536 (checked against u32 overflow in sestring-0.3.0).
                0x05 => {
                    let raw_id = read_sestring_integer(chunk_slice, &mut pos)?;
                    raw_id
                        .checked_add(65_536)
                        .ok_or_else(|| "quest payload id overflow".to_string())?;
                }
                // Status: single packed integer.
                0x09 => {
                    read_sestring_integer(chunk_slice, &mut pos)?;
                }
                // DalamudLink: u8 plugin_len + utf8 plugin + packed integer command.
                0x0F => {
                    let plugin_len = *chunk_slice
                        .get(pos)
                        .ok_or_else(|| "truncated dalamud link plugin length".to_string())?;
                    pos += 1;
                    validate_utf8_field(
                        chunk_slice,
                        &mut pos,
                        usize::from(plugin_len),
                        "dalamud plugin",
                    )?;
                    read_sestring_integer(chunk_slice, &mut pos)?;
                }
                // LinkTerminator (0xCF) | Unknown: RawPayload reads bytes[post_start..end].
                _ => {}
            }
        }
        // Unknown: RawPayload reads bytes[post_start..end].
        _ => {}
    }

    if pos > end {
        return Err(format!(
            "sestring payload (type 0x{chunk_type:02x}) overran chunk_len ({chunk_len})"
        ));
    }

    Ok(end + 1)
}

fn validate_sestring_bytes_and_extract_items(bytes: &[u8]) -> Result<Vec<Option<String>>, String> {
    let mut item_names = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        if bytes[pos] != START_BYTE {
            let end = bytes[pos..]
                .iter()
                .position(|&b| b == START_BYTE)
                .map_or(bytes.len(), |offset| pos + offset);
            std::str::from_utf8(&bytes[pos..end])
                .map_err(|e| format!("invalid utf-8 in text payload: {e}"))?;
            pos = end;
        } else {
            pos = validate_chunk(bytes, pos, &mut item_names)?;
        }
    }

    Ok(item_names)
}

pub fn deserialize<'de, D>(de: D) -> Result<SeString, D::Error>
where
    D: Deserializer<'de>,
{
    let b64 = String::deserialize(de)?;
    if b64.len() > MAX_SESTRING_BASE64_LEN {
        return Err(serde::de::Error::custom(format!(
            "invalid sestring: base64 length {} exceeds max {}",
            b64.len(),
            MAX_SESTRING_BASE64_LEN
        )));
    }
    let bytes = base64::decode(&b64)
        .map_err(|e| serde::de::Error::custom(format!("invalid base64: {:?}", e)))?;
    if bytes.len() > MAX_SESTRING_BYTES {
        return Err(serde::de::Error::custom(format!(
            "invalid sestring: decoded length {} exceeds max {}",
            bytes.len(),
            MAX_SESTRING_BYTES
        )));
    }
    let item_names = validate_sestring_bytes_and_extract_items(&bytes)
        .map_err(|e| serde::de::Error::custom(format!("invalid sestring: {e}")))?;
    let mut sestring = SeString::parse(&bytes)
        .map_err(|e| serde::de::Error::custom(format!("invalid sestring: {:?}", e)))?;
    let mut item_name_iter = item_names.into_iter();
    for payload in &mut sestring.0 {
        if let sestring::Payload::Item(item) = payload {
            if let Some(validated_name) = item_name_iter.next().flatten() {
                if item.name.is_none() {
                    item.name = Some(validated_name);
                }
            }
        }
    }
    Ok(sestring)
}

pub fn serialize<S>(sestring: &SeString, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = sestring.encode();
    base64::encode(&bytes).serialize(ser)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use sestring::payload::{
        AutoTranslatePayload, DalamudLinkPayload, EmphasisPayload, IconPayload, ItemPayload,
        MapLinkPayload, NewLinePayload, PlayerPayload, QuestPayload, RawPayload, SeHyphenPayload,
        StatusPayload, TextPayload, UiForegroundPayload, UiGlowPayload,
    };
    use sestring::Payload;

    #[derive(Debug, Deserialize)]
    struct Wrapper {
        #[serde(with = "super")]
        value: SeString,
    }

    fn deserialize_b64(b64: &str) -> Result<SeString, serde_json::Error> {
        let json = serde_json::json!({ "value": b64 });
        serde_json::from_value::<Wrapper>(json).map(|w| w.value)
    }

    fn deserialize_bytes(bytes: &[u8]) -> Result<SeString, serde_json::Error> {
        deserialize_b64(&base64::encode(bytes))
    }

    #[test]
    fn valid_sestrings_with_all_payload_types_deserialize() {
        let original = SeString(vec![
            Payload::Text(TextPayload::from("Hello ")),
            Payload::Player(PlayerPayload {
                server_id: 73,
                name: "Elora Abyssinian".to_string(),
            }),
            Payload::Item(ItemPayload {
                id: 4873,
                hq: true,
                name: None,
            }),
            Payload::Emphasis(EmphasisPayload::enable()),
            Payload::NewLine(NewLinePayload),
            Payload::SeHyphen(SeHyphenPayload),
            Payload::MapLink(MapLinkPayload {
                territory_type: 155,
                map: 10,
                raw_x: 1234,
                raw_y: -5678,
            }),
            Payload::Quest(QuestPayload(66000)),
            Payload::Status(StatusPayload(42)),
            Payload::DalamudLink(DalamudLinkPayload {
                plugin: "TestPlugin".to_string(),
                command: 7,
            }),
            Payload::AutoTranslate(AutoTranslatePayload { group: 2, key: 123 }),
            Payload::UiForeground(UiForegroundPayload(500)),
            Payload::UiGlow(UiGlowPayload(501)),
            Payload::Icon(IconPayload(15)),
            Payload::Raw(RawPayload(vec![39, 7, 207, 1, 1, 1, 255, 1])),
        ]);

        let encoded = original.encode();
        let parsed = deserialize_bytes(&encoded).expect("valid SeString must deserialize");
        assert_eq!(parsed, original);
    }

    #[test]
    fn valid_item_payload_with_name_at_start_and_after_text_deserializes() {
        let item_at_start = SeString(vec![Payload::Item(ItemPayload {
            id: 4873,
            hq: false,
            name: Some("Potion".to_string()),
        })]);
        assert_eq!(
            deserialize_bytes(&item_at_start.encode()).expect("item at start should pass"),
            item_at_start
        );

        let item_after_long_text = SeString(vec![
            Payload::Text(TextPayload::from("Looking for someone with ")),
            Payload::Item(ItemPayload {
                id: 4873,
                hq: false,
                name: Some("Potion".to_string()),
            }),
            Payload::Item(ItemPayload {
                id: 4874,
                hq: true,
                name: None,
            }),
            Payload::Item(ItemPayload {
                id: 4875,
                hq: false,
                name: Some("A".to_string()),
            }),
        ]);
        let encoded = item_after_long_text.encode();
        // Verify sestring-0.3.0 itself drops item names at wire offset > 20, while our deserializer preserves them.
        let raw_parsed = SeString::parse(&encoded).expect("raw parse succeeds");
        assert!(matches!(
            &raw_parsed.0[1],
            Payload::Item(item) if item.name.is_none()
        ));
        let decoded =
            deserialize_bytes(&encoded).expect("item after >20 byte prefix should preserve name");
        assert_eq!(decoded, item_after_long_text);

        // Invalid UTF-8 inside an item name at wire offset > 20 must also be rejected.
        let mut invalid_utf8_after_long_text = encoded;
        let potion_pos = invalid_utf8_after_long_text
            .windows(6)
            .position(|w| w == b"Potion")
            .unwrap();
        invalid_utf8_after_long_text[potion_pos] = 0xFF;
        assert!(deserialize_bytes(&invalid_utf8_after_long_text).is_err());
    }

    #[test]
    fn rejects_oversized_base64_and_decoded_wire_payloads() {
        let oversized_b64 = "A".repeat(MAX_SESTRING_BASE64_LEN + 4);
        assert!(deserialize_b64(&oversized_b64).is_err());

        let too_many_bytes = vec![b'a'; MAX_SESTRING_BYTES + 1];
        assert!(deserialize_bytes(&too_many_bytes).is_err());

        let max_bytes = vec![b'a'; MAX_SESTRING_BYTES];
        assert!(deserialize_bytes(&max_bytes).is_ok());
    }

    #[test]
    fn rejects_huge_chunk_len_without_oom_or_panic() {
        // 0x02, Raw/Unknown(0x99), packed int 0xFE 0xFF 0xFF 0xFF 0xFF (= u32::MAX ~4GB), 0x03
        let payload = [0x02, 0x99, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0x03];
        assert!(deserialize_bytes(&payload).is_err());

        // PlayerPayload with valid small chunk_len but 4GB inner name_len
        let player_huge_name = [
            0x02, 0x27, 0x0C, // chunk_len = 11
            0x01, // PlayerName
            0x01, // unk
            0x02, // server_id = 1
            0x01, 0xFF, // unk
            0xFE, 0xFF, 0xFF, 0xFF, 0xFF, // name_len = u32::MAX
            0x03,
        ];
        assert!(deserialize_bytes(&player_huge_name).is_err());
    }

    #[test]
    fn rejects_packed_int_marker_underflow_and_overflow_and_chunk_len_underflow() {
        // marker 0x00 causes `marker - 1` underflow in sestring-0.3.0
        assert!(deserialize_bytes(&[0x02, 0x10, 0x00, 0x03]).is_err());
        // marker 0xFF causes `marker + 1` overflow in sestring-0.3.0
        assert!(deserialize_bytes(&[0x02, 0x10, 0xFF, 0x03]).is_err());
        // Emphasis with chunk_len = 0 causes `chunk_len - total_read` underflow in sestring-0.3.0
        assert!(deserialize_bytes(&[0x02, 0x1A, 0x01, 0x03]).is_err());
        // Interactable with chunk_len = 0 causes `0 - 1` underflow in sestring-0.3.0
        assert!(deserialize_bytes(&[0x02, 0x27, 0x01, 0x03]).is_err());
        // Quest id >= u32::MAX - 65535 causes `+ OFFSET` overflow in sestring-0.3.0
        assert!(
            deserialize_bytes(&[0x02, 0x27, 0x07, 0x05, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0x03])
                .is_err()
        );
    }

    #[test]
    fn rejects_bad_terminator_and_truncated_chunks() {
        // Wrong terminator (0x04 instead of 0x03)
        assert!(deserialize_bytes(&[0x02, 0x10, 0x01, 0x04]).is_err());
        // Truncated before terminator
        assert!(deserialize_bytes(&[0x02, 0x10, 0x01]).is_err());
        // Bare 0x02
        assert!(deserialize_bytes(&[0x02]).is_err());
        // Chunk claiming 10 bytes when only 2 exist
        assert!(deserialize_bytes(&[0x02, 0x10, 0x0B, 0x00, 0x03]).is_err());
        // Invalid UTF-8 in text
        assert!(deserialize_bytes(&[0xFF, 0xFE]).is_err());
    }
}

