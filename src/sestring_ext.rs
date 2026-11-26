use crate::ffxiv::Language;
use sestring::{Payload, SeString};

pub trait SeStringExt {
    fn full_text(&self, lang: &Language) -> String;
}

impl SeStringExt for SeString {
    fn full_text(&self, lang: &Language) -> String {
        self.0
            .iter()
            .flat_map(|payload| match payload {
                Payload::Text(t) => Some(&*t.0),
                Payload::NewLine(_) => Some("\n"),
                Payload::SeHyphen(_) => Some("–"),
                Payload::Item(item) => item.name.as_deref().filter(|name| !name.is_empty()),
                Payload::AutoTranslate(at) => crate::ffxiv::AUTO_TRANSLATE
                    .get(&(u32::from(at.group), at.key))
                    .map(|text| text.text(lang)),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::SeStringExt;
    use crate::ffxiv::Language;
    use sestring::payload::{ItemPayload, NewLinePayload, SeHyphenPayload, TextPayload};
    use sestring::{Payload, SeString};

    #[test]
    fn full_text_renders_newline_se_hyphen_and_non_empty_item_name() {
        let sestring = SeString(vec![
            Payload::Text(TextPayload::from("Line 1")),
            Payload::NewLine(NewLinePayload),
            Payload::Text(TextPayload::from("A")),
            Payload::SeHyphen(SeHyphenPayload),
            Payload::Text(TextPayload::from("B ")),
            Payload::Item(ItemPayload {
                id: 4873,
                hq: false,
                name: Some("Potion".to_string()),
            }),
            Payload::Item(ItemPayload {
                id: 4874,
                hq: false,
                name: Some(String::new()),
            }),
            Payload::Item(ItemPayload {
                id: 4875,
                hq: false,
                name: None,
            }),
        ]);

        assert_eq!(
            sestring.full_text(&Language::English),
            "Line 1\nA–B Potion"
        );
    }
}
