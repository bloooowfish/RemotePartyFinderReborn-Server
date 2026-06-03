//! FFLogs API Zone/Encounter ID 매핑
//!
//! FFXIV Duty ID를 FFLogs의 Zone/Encounter ID로 매핑합니다.
//! 고난이도 컨텐츠(Savage, Ultimate, Extreme)만 매핑합니다.
//!
//! NOTE: FFLogs Zone/Encounter ID는 게임 데이터가 아니라 FFLogs의 자체 식별자입니다.
//! 참고: FFLogs 플러그인의 Configuration.cs에서 Encounter ID 확인

use std::collections::HashMap;

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
}

const STANDARD_DIFFICULTY_ID: u32 = 100;
const SAVAGE_DIFFICULTY_ID: u32 = 101;

const fn encounter(
    zone_id: u32,
    encounter_id: u32,
    difficulty_id: u32,
    secondary_encounter_id: Option<u32>,
) -> FFLogsEncounter {
    FFLogsEncounter {
        zone_id,
        encounter_id,
        difficulty_id: Some(difficulty_id),
        secondary_encounter_id,
    }
}

const fn ult(zone_id: u32, encounter_id: u32) -> FFLogsEncounter {
    encounter(zone_id, encounter_id, STANDARD_DIFFICULTY_ID, None)
}

const fn sav(zone_id: u32, encounter_id: u32) -> FFLogsEncounter {
    encounter(zone_id, encounter_id, SAVAGE_DIFFICULTY_ID, None)
}

const fn ext(zone_id: u32, encounter_id: u32) -> FFLogsEncounter {
    encounter(zone_id, encounter_id, STANDARD_DIFFICULTY_ID, None)
}

const fn chaotic(zone_id: u32, encounter_id: u32) -> FFLogsEncounter {
    encounter(zone_id, encounter_id, STANDARD_DIFFICULTY_ID, None)
}

const fn sav_split(
    zone_id: u32,
    encounter_id: u32,
    secondary_encounter_id: u32,
) -> FFLogsEncounter {
    encounter(
        zone_id,
        encounter_id,
        SAVAGE_DIFFICULTY_ID,
        Some(secondary_encounter_id),
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
        m.insert(986, sav(62, 93)); // AAC Light-heavyweight M1 (Savage) / M1S
        m.insert(988, sav(62, 94)); // AAC Light-heavyweight M2 (Savage) / M2S
        m.insert(990, sav(62, 95)); // AAC Light-heavyweight M3 (Savage) / M3S
        m.insert(992, sav(62, 96)); // AAC Light-heavyweight M4 (Savage) / M4S

        // =================================================================
        // Dawntrail (7.2) - AAC Cruiserweight Tier (Savage) - M5~M8
        // Zone ID: 68
        // duties.rs: 1020, 1022, 1024, 1026
        // FFLogs: EncounterId 97, 98, 99, 100
        // =================================================================
        m.insert(1020, sav(68, 97)); // AAC Cruiserweight M1 (Savage) / M5S
        m.insert(1022, sav(68, 98)); // AAC Cruiserweight M2 (Savage) / M6S
        m.insert(1024, sav(68, 99)); // AAC Cruiserweight M3 (Savage) / M7S
        m.insert(1026, sav(68, 100)); // AAC Cruiserweight M4 (Savage) / M8S

        // =================================================================
        // Dawntrail (7.4) - AAC Heavyweight Tier (Savage) - M9~M12
        // Zone ID: 73
        // duties.rs: 1069, 1071, 1073, 1075
        // FFLogs: EncounterId 101, 102, 103, 104 (105 is P2)
        // =================================================================
        m.insert(1069, sav(73, 101)); // AAC Heavyweight M1 (Savage) / M9S - Vamp Fatale
        m.insert(1071, sav(73, 102)); // AAC Heavyweight M2 (Savage) / M10S - Red Hot and Deep Blue
        m.insert(1073, sav(73, 103)); // AAC Heavyweight M3 (Savage) / M11S - The Tyrant
        m.insert(1075, sav_split(73, 104, 105)); // AAC Heavyweight M4 (Savage) / M12S - The Lindwurm (P1 & P2)

        // =================================================================
        // Dawntrail (7.5) - Current Extreme Trial
        // Zone ID: 72
        // duties.rs: 1116
        // FFLogs: Enuo = 1084
        // =================================================================
        m.insert(1116, ext(72, 1084)); // The Unmaking (Extreme) / 극 에누오

        // =================================================================
        // Dawntrail (7.1) - Chaotic Alliance Raid
        // Zone ID: 66
        // duties.rs: 1010
        // FFLogs: Cloud of Darkness = 2061
        // =================================================================
        m.insert(1010, chaotic(66, 2061)); // The Cloud of Darkness (Chaotic) / 멸 암흑의 구름

        // =================================================================
        // Ultimates (Dawntrail - Zone 59 Legacy)
        // duties.rs: 280, 539, 694, 788, 908, 1006, 1094
        // FFLogs: Zone 59 with ids 1073-1077, Zone 65 with 1079, Zone 76 with 1085
        // =================================================================
        // 절바하 - Duty 280
        m.insert(280, ult(59, 1073)); // The Unending Coil of Bahamut (Ultimate)
        // 절신 - Duty 539
        m.insert(539, ult(59, 1074)); // The Weapon's Refrain (Ultimate)
        // 절알렉 - Duty 694
        m.insert(694, ult(59, 1075)); // The Epic of Alexander (Ultimate)
        // 절용시 - Duty 788
        m.insert(788, ult(59, 1076)); // Dragonsong's Reprise (Ultimate)
        // 절오메가 - Duty 908
        m.insert(908, ult(59, 1077)); // The Omega Protocol (Ultimate)
        // 절미래 (절에덴) - Duty 1006, Zone 65
        m.insert(1006, ult(65, 1079)); // Futures Rewritten (Ultimate)
        // 절요성난무 - Duty 1094, Zone 76
        m.insert(1094, ult(76, 1085)); // Dancing Mad (Ultimate)

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
    matches!(
        (zone_id, encounter_id),
        (59, 1073..=1077) | (65, 1079) | (76, 1085)
    )
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
    }

    const SUPPORTED_HIGH_END_DUTIES: &[ExpectedSupportedDuty] = &[
        ExpectedSupportedDuty {
            duty_id: 986,
            zone_id: 62,
            encounter_id: 93,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 988,
            zone_id: 62,
            encounter_id: 94,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 990,
            zone_id: 62,
            encounter_id: 95,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 992,
            zone_id: 62,
            encounter_id: 96,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Light-heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1020,
            zone_id: 68,
            encounter_id: 97,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1022,
            zone_id: 68,
            encounter_id: 98,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1024,
            zone_id: 68,
            encounter_id: 99,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1026,
            zone_id: 68,
            encounter_id: 100,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Cruiserweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1069,
            zone_id: 73,
            encounter_id: 101,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1071,
            zone_id: 73,
            encounter_id: 102,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1073,
            zone_id: 73,
            encounter_id: 103,
            difficulty_id: Some(101),
            secondary_encounter_id: None,
            zone_name: "AAC Heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1075,
            zone_id: 73,
            encounter_id: 104,
            difficulty_id: Some(101),
            secondary_encounter_id: Some(105),
            zone_name: "AAC Heavyweight (Savage)",
        },
        ExpectedSupportedDuty {
            duty_id: 1116,
            zone_id: 72,
            encounter_id: 1084,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Trials III (Extreme)",
        },
        ExpectedSupportedDuty {
            duty_id: 1010,
            zone_id: 66,
            encounter_id: 2061,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Alliance Raids (Chaotic)",
        },
        ExpectedSupportedDuty {
            duty_id: 280,
            zone_id: 59,
            encounter_id: 1073,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
        },
        ExpectedSupportedDuty {
            duty_id: 539,
            zone_id: 59,
            encounter_id: 1074,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
        },
        ExpectedSupportedDuty {
            duty_id: 694,
            zone_id: 59,
            encounter_id: 1075,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
        },
        ExpectedSupportedDuty {
            duty_id: 788,
            zone_id: 59,
            encounter_id: 1076,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
        },
        ExpectedSupportedDuty {
            duty_id: 908,
            zone_id: 59,
            encounter_id: 1077,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Ultimates (Legacy)",
        },
        ExpectedSupportedDuty {
            duty_id: 1006,
            zone_id: 65,
            encounter_id: 1079,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Futures Rewritten (Ultimate)",
        },
        ExpectedSupportedDuty {
            duty_id: 1094,
            zone_id: 76,
            encounter_id: 1085,
            difficulty_id: Some(100),
            secondary_encounter_id: None,
            zone_name: "Dancing Mad (Ultimate)",
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
            assert!(encounter.encounter_id > 0);
            if let Some(secondary_encounter_id) = encounter.secondary_encounter_id {
                assert_ne!(secondary_encounter_id, encounter.encounter_id);
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
    fn current_extreme_mapping_tracks_enuo_not_hell_on_rails() {
        assert!(!is_fflogs_supported(1077));

        let enuo = get_fflogs_encounter(1116).expect("Enuo Extreme should be FFLogs-supported");
        assert_eq!(enuo.zone_id, 72);
        assert_eq!(enuo.encounter_id, 1084);
        assert_eq!(enuo.difficulty_id, Some(100));
        assert_eq!(enuo.secondary_encounter_id, None);

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

        let zone = FFLOGS_ZONES
            .get(&chaotic.zone_id)
            .expect("Cloud of Darkness Chaotic FFLogs zone should be known");
        assert_eq!(zone.name, "Alliance Raids (Chaotic)");
        assert_eq!(zone.partition, 1);
    }
}
