//! FFLogs API Zone/Encounter ID 매핑
//!
//! FFXIV Duty ID를 FFLogs의 Zone/Encounter ID로 매핑합니다.
//! 고난이도 컨텐츠(Savage, Ultimate, Extreme)만 매핑합니다.
//!
//! NOTE: FFLogs Zone/Encounter ID는 게임 데이터가 아니라 FFLogs의 자체 식별자입니다.
//! 참고: FFLogs 플러그인의 Configuration.cs에서 Encounter ID 확인

use std::collections::{BTreeSet, HashMap};

/// Tomestone 진행률 파싱 방식
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TomestoneProgressKind {
    PhaseProgress,
    BossPercentage,
}

impl TomestoneProgressKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PhaseProgress => "phase_progress",
            Self::BossPercentage => "boss_percentage",
        }
    }
}

/// Tomestone 인카운터 메타데이터
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TomestoneEncounterMetadata {
    pub expansion: &'static str,
    pub activity_category: &'static str,
    pub clears_category: &'static str,
    pub zone_slug: &'static str,
    pub encounter_slug: &'static str,
    pub encounter_names: &'static [&'static str],
    pub secondary_encounter_slug: Option<&'static str>,
    pub secondary_encounter_names: &'static [&'static str],
    pub progress_kind: TomestoneProgressKind,
}

/// FFLogs Encounter 정보
#[derive(Debug, Clone, Copy)]
pub struct FFLogsEncounter {
    /// FFLogs Zone ID (레이드 티어)
    pub zone_id: u32,
    /// FFLogs Encounter ID (개별 보스)
    pub encounter_id: u32,
    /// Difficulty ID (101=Savage, 100=Normal/Ult/Ext)
    pub difficulty_id: Option<u32>,
    /// Secondary Encounter ID (for split bosses like P2)
    pub secondary_encounter_id: Option<u32>,
    /// Whether this duty is an Ultimate encounter
    pub is_ultimate: bool,
    /// Tomestone slug/category metadata for this encounter
    pub tomestone: Option<TomestoneEncounterMetadata>,
}

const STANDARD_DIFFICULTY_ID: u32 = 100;
const SAVAGE_DIFFICULTY_ID: u32 = 101;

const fn tomestone_meta(
    expansion: &'static str,
    activity_category: &'static str,
    clears_category: &'static str,
    zone_slug: &'static str,
    encounter_slug: &'static str,
    encounter_names: &'static [&'static str],
    progress_kind: TomestoneProgressKind,
) -> TomestoneEncounterMetadata {
    TomestoneEncounterMetadata {
        expansion,
        activity_category,
        clears_category,
        zone_slug,
        encounter_slug,
        encounter_names,
        secondary_encounter_slug: None,
        secondary_encounter_names: &[],
        progress_kind,
    }
}

const fn tomestone_split_meta(
    expansion: &'static str,
    activity_category: &'static str,
    clears_category: &'static str,
    zone_slug: &'static str,
    encounter_slug: &'static str,
    encounter_names: &'static [&'static str],
    secondary_encounter_slug: &'static str,
    secondary_encounter_names: &'static [&'static str],
    progress_kind: TomestoneProgressKind,
) -> TomestoneEncounterMetadata {
    TomestoneEncounterMetadata {
        expansion,
        activity_category,
        clears_category,
        zone_slug,
        encounter_slug,
        encounter_names,
        secondary_encounter_slug: Some(secondary_encounter_slug),
        secondary_encounter_names,
        progress_kind,
    }
}

const fn encounter(
    zone_id: u32,
    encounter_id: u32,
    difficulty_id: u32,
    secondary_encounter_id: Option<u32>,
    is_ultimate: bool,
    tomestone: TomestoneEncounterMetadata,
) -> FFLogsEncounter {
    FFLogsEncounter {
        zone_id,
        encounter_id,
        difficulty_id: Some(difficulty_id),
        secondary_encounter_id,
        is_ultimate,
        tomestone: Some(tomestone),
    }
}

const fn ult(
    zone_id: u32,
    encounter_id: u32,
    expansion: &'static str,
    encounter_slug: &'static str,
    encounter_names: &'static [&'static str],
) -> FFLogsEncounter {
    encounter(
        zone_id,
        encounter_id,
        STANDARD_DIFFICULTY_ID,
        None,
        true,
        tomestone_meta(
            expansion,
            "ultimates",
            "ultimate",
            "ultimates",
            encounter_slug,
            encounter_names,
            TomestoneProgressKind::PhaseProgress,
        ),
    )
}

const fn sav(
    zone_id: u32,
    encounter_id: u32,
    zone_slug: &'static str,
    encounter_slug: &'static str,
    encounter_names: &'static [&'static str],
) -> FFLogsEncounter {
    encounter(
        zone_id,
        encounter_id,
        SAVAGE_DIFFICULTY_ID,
        None,
        false,
        tomestone_meta(
            "dawntrail",
            "raids",
            "savage",
            zone_slug,
            encounter_slug,
            encounter_names,
            TomestoneProgressKind::BossPercentage,
        ),
    )
}

const fn ext(
    zone_id: u32,
    encounter_id: u32,
    encounter_slug: &'static str,
    encounter_names: &'static [&'static str],
) -> FFLogsEncounter {
    encounter(
        zone_id,
        encounter_id,
        STANDARD_DIFFICULTY_ID,
        None,
        false,
        tomestone_meta(
            "dawntrail",
            "trials",
            "extreme",
            "trials-extreme",
            encounter_slug,
            encounter_names,
            TomestoneProgressKind::BossPercentage,
        ),
    )
}

const fn chaotic(
    zone_id: u32,
    encounter_id: u32,
    encounter_slug: &'static str,
    encounter_names: &'static [&'static str],
) -> FFLogsEncounter {
    encounter(
        zone_id,
        encounter_id,
        STANDARD_DIFFICULTY_ID,
        None,
        false,
        tomestone_meta(
            "dawntrail",
            "chaotic",
            "chaotic",
            "chaotic",
            encounter_slug,
            encounter_names,
            TomestoneProgressKind::BossPercentage,
        ),
    )
}

#[allow(clippy::too_many_arguments)]
const fn sav_split(
    zone_id: u32,
    encounter_id: u32,
    secondary_encounter_id: u32,
    zone_slug: &'static str,
    encounter_slug: &'static str,
    encounter_names: &'static [&'static str],
    secondary_encounter_slug: &'static str,
    secondary_encounter_names: &'static [&'static str],
) -> FFLogsEncounter {
    encounter(
        zone_id,
        encounter_id,
        SAVAGE_DIFFICULTY_ID,
        Some(secondary_encounter_id),
        false,
        tomestone_split_meta(
            "dawntrail",
            "raids",
            "savage",
            zone_slug,
            encounter_slug,
            encounter_names,
            secondary_encounter_slug,
            secondary_encounter_names,
            TomestoneProgressKind::BossPercentage,
        ),
    )
}

lazy_static::lazy_static! {
    /// FFXIV Duty ID -> FFLogs Encounter 매핑
    ///
    /// NOTE: Duty ID는 duties.rs 파일에서 정의된 값을 사용합니다.
    /// FFLogs Zone/Encounter ID는 FFLogs 플러그인에서 확인
    pub static ref DUTY_TO_FFLOGS: HashMap<u16, FFLogsEncounter> = {
        let mut m = HashMap::new();

        // =================================================================
        // Dawntrail (7.0) - AAC Light-heavyweight Tier (Savage) - M1~M4
        // Zone ID: 62
        // duties.rs: 986, 988, 990, 992
        // FFLogs: EncounterId 93, 94, 95, 96
        // =================================================================
        m.insert(986, sav(62, 93, "aac-light-heavyweight-savage", "black-cat", &["black-cat"])); // AAC Light-heavyweight M1 (Savage) / M1S
        m.insert(988, sav(62, 94, "aac-light-heavyweight-savage", "honey-b.-lovely", &["honey-b.-lovely", "honey-b-lovely"])); // AAC Light-heavyweight M2 (Savage) / M2S
        m.insert(990, sav(62, 95, "aac-light-heavyweight-savage", "brute-bomber", &["brute-bomber"])); // AAC Light-heavyweight M3 (Savage) / M3S
        m.insert(992, sav(62, 96, "aac-light-heavyweight-savage", "wicked-thunder", &["wicked-thunder"])); // AAC Light-heavyweight M4 (Savage) / M4S

        // =================================================================
        // Dawntrail (7.2) - AAC Cruiserweight Tier (Savage) - M5~M8
        // Zone ID: 68
        // duties.rs: 1020, 1022, 1024, 1026
        // FFLogs: EncounterId 97, 98, 99, 100
        // =================================================================
        m.insert(1020, sav(68, 97, "aac-cruiserweight-savage", "dancing-green", &["dancing-green"])); // AAC Cruiserweight M1 (Savage) / M5S
        m.insert(1022, sav(68, 98, "aac-cruiserweight-savage", "sugar-riot", &["sugar-riot"])); // AAC Cruiserweight M2 (Savage) / M6S
        m.insert(1024, sav(68, 99, "aac-cruiserweight-savage", "brute-abombinator", &["brute-abombinator"])); // AAC Cruiserweight M3 (Savage) / M7S
        m.insert(1026, sav(68, 100, "aac-cruiserweight-savage", "howling-blade", &["howling-blade"])); // AAC Cruiserweight M4 (Savage) / M8S

        // =================================================================
        // Dawntrail (7.4) - AAC Heavyweight Tier (Savage) - M9~M12
        // Zone ID: 73
        // duties.rs: 1069, 1071, 1073, 1075
        // FFLogs: EncounterId 101, 102, 103, 104 (105 is P2)
        // =================================================================
        m.insert(1069, sav(73, 101, "aac-heavyweight-savage", "vamp-fatale", &["vamp-fatale"])); // AAC Heavyweight M1 (Savage) / M9S - Vamp Fatale
        m.insert(1071, sav(73, 102, "aac-heavyweight-savage", "red-hot-deep-blue", &["red-hot-deep-blue"])); // AAC Heavyweight M2 (Savage) / M10S - Red Hot and Deep Blue
        m.insert(1073, sav(73, 103, "aac-heavyweight-savage", "the-tyrant", &["the-tyrant"])); // AAC Heavyweight M3 (Savage) / M11S - The Tyrant
        m.insert(
            1075,
            sav_split(
                73,
                104,
                105,
                "aac-heavyweight-savage",
                "lindwurm",
                &["lindwurm"],
                "lindwurm-ii",
                &["lindwurm-ii"],
            ),
        ); // AAC Heavyweight M4 (Savage) / M12S - The Lindwurm (P1 & P2)

        // =================================================================
        // Dawntrail (7.5) - Current Extreme Trial
        // Zone ID: 72
        // duties.rs: 1116
        // FFLogs: Enuo = 1084
        // =================================================================
        m.insert(1116, ext(72, 1084, "enuo", &["enuo", "the-unmaking-extreme"])); // The Unmaking (Extreme) / 극 에누오

        // =================================================================
        // Dawntrail (7.1) - Chaotic Alliance Raid
        // Zone ID: 66
        // duties.rs: 1010
        // FFLogs: Cloud of Darkness = 2061
        // =================================================================
        m.insert(
            1010,
            chaotic(
                66,
                2061,
                "the-cloud-of-darkness-chaotic",
                &[
                    "the-cloud-of-darkness-chaotic",
                    "cloud-of-darkness-chaotic",
                    "the-cloud-of-darkness",
                    "cloud-of-darkness",
                ],
            ),
        ); // The Cloud of Darkness (Chaotic) / 멸 암흑의 구름

        // =================================================================
        // Ultimates (Dawntrail - Zone 59 Legacy)
        // duties.rs: 280, 539, 694, 788, 908, 1006, 1094
        // FFLogs: Zone 59 with ids 1073-1077, Zone 65 with 1079, Zone 76 with 1085
        // =================================================================
        // 절바하 - Duty 280
        m.insert(280, ult(59, 1073, "stormblood", "the-unending-coil-of-bahamut-ultimate", &["the-unending-coil-of-bahamut-ultimate"])); // The Unending Coil of Bahamut (Ultimate)
        // 절신 - Duty 539
        m.insert(539, ult(59, 1074, "stormblood", "the-weapons-refrain-ultimate", &["the-weapons-refrain-ultimate"])); // The Weapon's Refrain (Ultimate)
        // 절알렉 - Duty 694
        m.insert(694, ult(59, 1075, "shadowbringers", "the-epic-of-alexander-ultimate", &["the-epic-of-alexander-ultimate"])); // The Epic of Alexander (Ultimate)
        // 절용시 - Duty 788
        m.insert(788, ult(59, 1076, "endwalker", "dragonsongs-reprise-ultimate", &["dragonsongs-reprise-ultimate"])); // Dragonsong's Reprise (Ultimate)
        // 절오메가 - Duty 908
        m.insert(908, ult(59, 1077, "endwalker", "the-omega-protocol-ultimate", &["the-omega-protocol-ultimate"])); // The Omega Protocol (Ultimate)
        // 절미래 (절에덴) - Duty 1006, Zone 65
        m.insert(1006, ult(65, 1079, "dawntrail", "futures-rewritten-ultimate", &["futures-rewritten-ultimate"])); // Futures Rewritten (Ultimate)
        // 절요성난무 - Duty 1094, Zone 76
        m.insert(1094, ult(76, 1085, "dawntrail", "dancing-mad-ultimate", &["dancing-mad-ultimate"])); // Dancing Mad (Ultimate)

        m
    };

    /// FFLogs Zone ID -> Zone 정보
    pub static ref FFLOGS_ZONES: HashMap<u32, FFLogsZone> = {
        let mut m = HashMap::new();
        m.insert(62, FFLogsZone { name: "AAC Light-heavyweight (Savage)", partition: 1 });
        m.insert(68, FFLogsZone { name: "AAC Cruiserweight (Savage)", partition: 1 });
        m.insert(73, FFLogsZone { name: "AAC Heavyweight (Savage)", partition: 1 });
        m.insert(72, FFLogsZone { name: "Trials III (Extreme)", partition: 1 });
        m.insert(66, FFLogsZone { name: "Alliance Raids (Chaotic)", partition: 1 });
        m.insert(65, FFLogsZone { name: "Futures Rewritten (Ultimate)", partition: 1 });
        m.insert(76, FFLogsZone { name: "Dancing Mad (Ultimate)", partition: 1 });
        m.insert(59, FFLogsZone { name: "Ultimates (Legacy)", partition: 1 });
        m
    };
}

/// FFLogs Zone 정보
#[derive(Debug, Clone, Copy)]
pub struct FFLogsZone {
    pub name: &'static str,
    pub partition: u32,
}

/// Duty ID로 FFLogs Encounter 조회
pub fn get_fflogs_encounter(duty_id: u16) -> Option<&'static FFLogsEncounter> {
    DUTY_TO_FFLOGS.get(&duty_id)
}

/// 해당 Duty가 FFLogs 조회 대상인지 확인
pub fn is_fflogs_supported(duty_id: u16) -> bool {
    DUTY_TO_FFLOGS.contains_key(&duty_id)
}

/// Returns whether a FFLogs zone/encounter pair represents an Ultimate duty.
pub fn is_ultimate_encounter(zone_id: u32, encounter_id: u32) -> bool {
    DUTY_TO_FFLOGS.values().any(|entry| {
        entry.is_ultimate
            && entry.zone_id == zone_id
            && (entry.encounter_id == encounter_id
                || entry.secondary_encounter_id == Some(encounter_id))
    })
}

/// Returns all sorted unique encounter IDs (primary + secondary) in `(zone_id, difficulty_id)`.
pub fn zone_encounters_for(zone_id: u32, difficulty_id: Option<u32>) -> Vec<u32> {
    let mut encounters = BTreeSet::new();
    for entry in DUTY_TO_FFLOGS.values() {
        if entry.zone_id == zone_id && entry.difficulty_id == difficulty_id {
            if entry.encounter_id != 0 {
                encounters.insert(entry.encounter_id);
            }
            if let Some(secondary) = entry.secondary_encounter_id.filter(|&id| id != 0) {
                encounters.insert(secondary);
            }
        }
    }
    encounters.into_iter().collect()
}

/// FFLogs percentile 색상 클래스 반환
pub fn percentile_color_class(percentile: f32) -> &'static str {
    match percentile as u32 {
        100 => "parse-gold",
        99 => "parse-pink",
        95..=98 => "parse-orange",
        75..=94 => "parse-purple",
        50..=74 => "parse-blue",
        25..=49 => "parse-green",
        _ => "parse-gray",
    }
}

/// FFLogs percentile RGB 색상 반환
pub fn percentile_color(percentile: f32) -> &'static str {
    match percentile as u32 {
        100 => "#E5CC80",
        99 => "#E268A8",
        95..=98 => "#FF8000",
        75..=94 => "#A335EE",
        50..=74 => "#0070FF",
        25..=49 => "#1EFF00",
        _ => "#666666",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffxiv::DUTIES;
    use std::collections::HashSet;

    #[derive(Debug, Clone, Copy)]
    struct ExpectedSupportedDuty {
        duty_id: u16,
        zone_id: u32,
        encounter_id: u32,
        difficulty_id: Option<u32>,
        secondary_encounter_id: Option<u32>,
        zone_name: &'static str,
        is_ultimate: bool,
    }

    const SUPPORTED_HIGH_END_DUTIES: &[ExpectedSupportedDuty] = &[
        ExpectedSupportedDuty {
            duty_id: 986,
            zone_id: 62,
            encounter_id: 93,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 988,
            zone_id: 62,
            encounter_id: 94,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 990,
            zone_id: 62,
            encounter_id: 95,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 992,
            zone_id: 62,
            encounter_id: 96,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1020,
            zone_id: 68,
            encounter_id: 97,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1022,
            zone_id: 68,
            encounter_id: 98,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1024,
            zone_id: 68,
            encounter_id: 99,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1026,
            zone_id: 68,
            encounter_id: 100,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1069,
            zone_id: 73,
            encounter_id: 101,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1071,
            zone_id: 73,
            encounter_id: 102,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1073,
            zone_id: 73,
            encounter_id: 103,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1075,
            zone_id: 73,
            encounter_id: 104,
            difficulty_id: Some(101),
            secondary_encounter_id: Some(105),
            zone_name: "AAC Heavyweight (Savage)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1116,
            zone_id: 72,
            encounter_id: 1084,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Trials III (Extreme)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 1010,
            zone_id: 66,
            encounter_id: 2061,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Alliance Raids (Chaotic)",
            is_ultimate: false,
        },
        ExpectedSupportedDuty {
            duty_id: 280,
            zone_id: 59,
            encounter_id: 1073,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
            is_ultimate: true,
        },
        ExpectedSupportedDuty {
            duty_id: 539,
            zone_id: 59,
            encounter_id: 1074,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
            is_ultimate: true,
        },
        ExpectedSupportedDuty {
            duty_id: 694,
            zone_id: 59,
            encounter_id: 1075,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
            is_ultimate: true,
        },
        ExpectedSupportedDuty {
            duty_id: 788,
            zone_id: 59,
            encounter_id: 1076,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
            is_ultimate: true,
        },
        ExpectedSupportedDuty {
            duty_id: 908,
            zone_id: 59,
            encounter_id: 1077,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
            is_ultimate: true,
        },
        ExpectedSupportedDuty {
            duty_id: 1006,
            zone_id: 65,
            encounter_id: 1079,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Futures Rewritten (Ultimate)",
            is_ultimate: true,
        },
        ExpectedSupportedDuty {
            duty_id: 1094,
            zone_id: 76,
            encounter_id: 1085,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Dancing Mad (Ultimate)",
            is_ultimate: true,
        },
    ];

    #[test]
    fn supported_high_end_duties_have_complete_fflogs_mapping() {
        let expected_duty_ids = SUPPORTED_HIGH_END_DUTIES
            .iter()
            .map(|expected| expected.duty_id)
            .collect::<HashSet<_>>();
        let actual_duty_ids = DUTY_TO_FFLOGS.keys().copied().collect::<HashSet<_>>();
        assert_eq!(actual_duty_ids, expected_duty_ids);

        for expected in SUPPORTED_HIGH_END_DUTIES {
            let encounter = get_fflogs_encounter(expected.duty_id)
                .expect("supported high-end duty should map to FFLogs");
            assert_eq!(encounter.zone_id, expected.zone_id);
            assert_eq!(encounter.encounter_id, expected.encounter_id);
            assert_eq!(encounter.difficulty_id, expected.difficulty_id);
            assert_eq!(
                encounter.secondary_encounter_id,
                expected.secondary_encounter_id
            );
            assert_eq!(encounter.is_ultimate, expected.is_ultimate);
            assert_eq!(
                is_ultimate_encounter(encounter.zone_id, encounter.encounter_id),
                expected.is_ultimate
            );
            assert!(encounter.encounter_id > 0);
            if let Some(secondary_encounter_id) = encounter.secondary_encounter_id {
                assert_ne!(secondary_encounter_id, encounter.encounter_id);
            }

            let tomestone = encounter
                .tomestone
                .expect("supported high-end duty should embed Tomestone metadata");
            assert!(!tomestone.expansion.is_empty());
            assert!(!tomestone.activity_category.is_empty());
            assert!(!tomestone.clears_category.is_empty());
            assert!(!tomestone.zone_slug.is_empty());
            assert!(!tomestone.encounter_slug.is_empty());
            assert!(!tomestone.encounter_names.is_empty());
            if encounter.secondary_encounter_id.is_some() {
                assert!(tomestone.secondary_encounter_slug.is_some());
                assert!(!tomestone.secondary_encounter_names.is_empty());
            }

            let zone = FFLOGS_ZONES
                .get(&encounter.zone_id)
                .expect("supported high-end duty should reference a known FFLogs zone");
            assert_eq!(zone.name, expected.zone_name);
            assert_eq!(zone.partition, 1);
        }
    }

    #[test]
    fn supported_high_end_duties_are_present_in_generated_catalog() {
        for expected in SUPPORTED_HIGH_END_DUTIES {
            DUTIES
                .get(&u32::from(expected.duty_id))
                .expect("supported high-end duty should exist in generated duties");
        }
    }

    #[test]
    fn current_high_end_duty_ids_are_all_mapped_except_explicit_unreal_allowlist() {
        const UNREAL_TRIAL_ALLOWLIST: &[u32] = &[1118]; // Shinryu's Domain (Unreal)

        for &duty_id in ffxiv_data::duties::HIGH_END_DUTY_IDS {
            if UNREAL_TRIAL_ALLOWLIST.contains(&duty_id) {
                let info = DUTIES
                    .get(&duty_id)
                    .expect("allowlisted Unreal trial should exist in DUTIES");
                assert!(
                    info.name.en.ends_with("(Unreal)"),
                    "allowlisted duty {duty_id} ({}) must remain an Unreal trial",
                    info.name.en
                );
                continue;
            }

            let duty_u16 = u16::try_from(duty_id).expect("high-end duty ID must fit in u16");
            let encounter = DUTY_TO_FFLOGS.get(&duty_u16).unwrap_or_else(|| {
                panic!("HIGH_END_DUTY_IDS duty {duty_id} is missing from DUTY_TO_FFLOGS")
            });
            assert!(
                FFLOGS_ZONES.contains_key(&encounter.zone_id),
                "HIGH_END_DUTY_IDS duty {duty_id} references unmapped FFLogs zone {}",
                encounter.zone_id
            );
        }
    }

    #[test]
    fn zone_encounters_for_returns_sorted_unique_primary_and_secondary_ids() {
        assert_eq!(zone_encounters_for(62, Some(101)), vec![93, 94, 95, 96]);
        assert_eq!(zone_encounters_for(68, Some(101)), vec![97, 98, 99, 100]);
        assert_eq!(
            zone_encounters_for(73, Some(101)),
            vec![101, 102, 103, 104, 105]
        );
        assert_eq!(
            zone_encounters_for(59, Some(100)),
            vec![1073, 1074, 1075, 1076, 1077]
        );
        assert_eq!(zone_encounters_for(65, Some(100)), vec![1079]);
        assert_eq!(zone_encounters_for(76, Some(100)), vec![1085]);
        assert_eq!(zone_encounters_for(72, Some(100)), vec![1084]);
        assert_eq!(zone_encounters_for(66, Some(100)), vec![2061]);
        assert!(zone_encounters_for(73, Some(100)).is_empty());
        assert!(zone_encounters_for(62, None).is_empty());
    }

    #[test]
    fn current_extreme_mapping_tracks_enuo_not_hell_on_rails() {
        assert!(!is_fflogs_supported(1077));

        let enuo = get_fflogs_encounter(1116).expect("Enuo Extreme should be FFLogs-supported");
        assert_eq!(enuo.zone_id, 72);
        assert_eq!(enuo.encounter_id, 1084);
        assert_eq!(enuo.difficulty_id, Some(100));
        assert_eq!(enuo.secondary_encounter_id, None);
        assert!(!enuo.is_ultimate);

        let zone = FFLOGS_ZONES
            .get(&enuo.zone_id)
            .expect("Enuo Extreme FFLogs zone should be known");
        assert_eq!(zone.name, "Trials III (Extreme)");
        assert_eq!(zone.partition, 1);
    }

    #[test]
    fn chaotic_alliance_mapping_tracks_cloud_of_darkness() {
        let chaotic = get_fflogs_encounter(1010)
            .expect("Cloud of Darkness Chaotic should be FFLogs-supported");
        assert_eq!(chaotic.zone_id, 66);
        assert_eq!(chaotic.encounter_id, 2061);
        assert_eq!(chaotic.difficulty_id, Some(100));
        assert_eq!(chaotic.secondary_encounter_id, None);
        assert!(!chaotic.is_ultimate);

        let zone = FFLOGS_ZONES
            .get(&chaotic.zone_id)
            .expect("Cloud of Darkness Chaotic FFLogs zone should be known");
        assert_eq!(zone.name, "Alliance Raids (Chaotic)");
        assert_eq!(zone.partition, 1);
    }
}
