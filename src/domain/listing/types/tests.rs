use std::fs;
use std::path::Path;

use mongodb::bson;
use serde_json::{json, Value};
use sestring::SeString;

use super::{
    ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
    ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
};

fn sample_listing() -> PartyFinderListing {
    PartyFinderListing {
        id: 166_086,
        content_id_lower: 40_205_661,
        name: SeString::parse(b"Yuki Coffee").unwrap(),
        description: SeString::parse(b"Test Description").unwrap(),
        created_world: 44,
        home_world: 44,
        current_world: 44,
        category: DutyCategory::HighEndDuty,
        duty: 1010,
        duty_type: DutyType::Normal,
        pf_category_high_end: false,
        cfc_excel_high_end: false,
        client_high_end_duty: false,
        beginners_welcome: false,
        seconds_remaining: 1667,
        min_item_level: 0,
        num_parties: 3,
        slots_available: 24,
        last_server_restart: 1_774_336_035,
        objective: ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::DUTY_COMPLETE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: vec![
            PartyFinderSlot {
                accepting: JobFlags::all() - JobFlags::GLADIATOR,
            },
            PartyFinderSlot {
                accepting: JobFlags::all() - JobFlags::GLADIATOR,
            },
            PartyFinderSlot {
                accepting: JobFlags::all() - JobFlags::GLADIATOR,
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
        ],
        jobs_present: vec![0, 0, 0, 0, 0, 0, 0, 0],
        member_content_ids: vec![
            1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 7, 8, 9, 0, 10,
        ],
        member_jobs: vec![
            37, 0, 24, 0, 22, 0, 31, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 28, 34, 22, 0, 38,
        ],
        detail_slot_flags: Vec::new(),
        leader_content_id: 5,
    }
}

#[test]
fn objective_flags_match_client_structs_values() {
    assert_eq!(ObjectiveFlags::NONE.bits(), 1);
    assert_eq!(ObjectiveFlags::DUTY_COMPLETION.bits(), 2);
    assert_eq!(ObjectiveFlags::PRACTICE.bits(), 4);
    assert_eq!(ObjectiveFlags::LOOT.bits(), 8);
}

#[test]
fn sanitized_live_high_end_sample_decodes_client_category() {
    let sample: Value = serde_json::from_value(json!({
        "time_left_seconds": 2317,
        "updated_at_timestamp": 1778067936,
        "listing": {
            "duty_id": 1116,
            "duty_type": 2,
            "category": 64,
            "high_end": true
        }
    }))
    .expect("sanitized live listing sample should be valid JSON");

    let raw_category = sample
        .pointer("/listing/category")
        .and_then(Value::as_u64)
        .expect("sanitized live listing sample should include listing.category")
        as u32;

    assert_eq!(
        DutyCategory::from_u32(raw_category),
        Some(DutyCategory::HighEndDuty)
    );
}

#[test]
fn duty_category_from_u32_matches_client_struct_values() {
    let categories = [
        DutyCategory::None,
        DutyCategory::DutyRoulette,
        DutyCategory::Dungeon,
        DutyCategory::Guildhest,
        DutyCategory::Trial,
        DutyCategory::Raid,
        DutyCategory::HighEndDuty,
        DutyCategory::PvP,
        DutyCategory::GoldSaucer,
        DutyCategory::Fate,
        DutyCategory::TreasureHunt,
        DutyCategory::TheHunt,
        DutyCategory::GatheringForay,
        DutyCategory::DeepDungeon,
        DutyCategory::FieldOperation,
        DutyCategory::VariantAndCriterionDungeon,
    ];

    for category in categories {
        assert_eq!(
            DutyCategory::from_u32(category as u32),
            Some(category),
            "raw category value {} should decode as {:?}",
            category as u32,
            category
        );
    }
}

#[test]
fn captured_live_listing_samples_decode_client_category_values() {
    let sample_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("output")
        .join("listings-live-samples")
        .join("listings");

    if !sample_dir.is_dir() {
        return;
    }

    let mut checked = 0usize;
    let mut saw_high_end = false;

    for entry in
        fs::read_dir(&sample_dir).expect("live listing sample directory should be readable")
    {
        let entry = entry.expect("live listing sample entry should be readable");
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let text =
            fs::read_to_string(entry.path()).expect("live listing sample should be readable");
        let sample: Value =
            serde_json::from_str(&text).expect("live listing sample should be valid JSON");
        let raw_category = sample
            .pointer("/listing/category")
            .and_then(Value::as_u64)
            .expect("live listing sample should include listing.category")
            as u32;
        let decoded = DutyCategory::from_u32(raw_category)
            .expect("live listing sample category should be a known client value");

        assert_eq!(
            decoded as u32,
            raw_category,
            "live listing sample {} decoded category {:?} from raw value {}",
            entry.file_name().to_string_lossy(),
            decoded,
            raw_category
        );

        checked += 1;
        saw_high_end |= raw_category == DutyCategory::HighEndDuty as u32;
    }

    assert!(checked > 0, "expected at least one live listing sample");
    assert!(
        saw_high_end,
        "expected captured live listing samples to include high-end duty category"
    );
}

#[test]
fn slots_use_member_jobs_overlay_for_alliance_listings() {
    let listing = sample_listing();

    let slots = listing.slots();

    assert_eq!(slots.len(), 24);
    assert!(slots[0].is_ok());
    assert!(slots[1].is_err());
    assert!(slots[8].is_ok());
    assert!(slots[17].is_err());
    assert!(slots[18].is_ok());
    assert!(slots[23].is_ok());
}

#[test]
fn slots_filled_counts_member_jobs_overlay_for_alliance_listings() {
    let listing = sample_listing();

    assert_eq!(listing.slots_filled(), 10);
}

#[test]
fn slots_use_detail_slot_flags_for_empty_alliance_slots_beyond_compact_listing_slots() {
    let mut listing = sample_listing();
    listing.slots = Vec::new();
    listing.detail_slot_flags = vec!["0x0000000000000000".to_string(); 24];
    listing.detail_slot_flags[9] = "0x0000000020422040".to_string();
    listing.detail_slot_flags[22] = "0x00000000D99DDAB4".to_string();

    let slots = listing.slots();

    assert_eq!(
        slots[9],
        Err(("healer".to_string(), "CNJ WHM SCH AST SGE".to_string()))
    );
    assert_eq!(
        slots[22],
        Err((
            "dps".to_string(),
            "PGL LNC ARC THM MNK DRG BRD BLM ACN SMN ROG NIN MCH SAM RDM DNC RPR VPR PCT"
                .to_string()
        ))
    );
}

#[test]
fn slots_fall_back_to_compact_slots_when_detail_slot_flag_is_invalid() {
    let mut listing = sample_listing();
    listing.slots = vec![
        PartyFinderSlot {
            accepting: JobFlags::empty(),
        };
        24
    ];
    listing.slots[9] = PartyFinderSlot {
        accepting: JobFlags::DANCER,
    };
    listing.detail_slot_flags = vec!["0x0000000000000000".to_string(); 24];
    listing.detail_slot_flags[9] = "not-a-flag".to_string();

    let slots = listing.slots();

    assert_eq!(slots[9], Err(("dps".to_string(), "DNC".to_string())));
}

#[test]
fn joinable_roles_include_empty_alliance_slots_from_detail_slot_flags() {
    let mut listing = sample_listing();
    listing.slots = Vec::new();
    listing.detail_slot_flags = vec!["0x0000000000000000".to_string(); 24];
    listing.detail_slot_flags[9] = "0x0000000020422040".to_string();

    let joinable = JobFlags::from_bits_truncate(listing.joinable_roles());

    assert!(joinable.contains(JobFlags::SAGE));
    assert!(joinable.contains(JobFlags::WHITE_MAGE));
    assert!(!joinable.contains(JobFlags::GUNBREAKER));
}

#[test]
fn joinable_roles_exclude_occupied_jobs_from_detail_slot_flags_when_unique_job_is_enforced() {
    let mut listing = sample_listing();
    listing.search_area |= SearchAreaFlags::ONE_PLAYER_PER_JOB;
    listing.slots = Vec::new();
    listing.detail_slot_flags = vec!["0x0000000000000000".to_string(); 24];
    listing.detail_slot_flags[9] = "0x0000000028000000".to_string();

    let joinable = JobFlags::from_bits_truncate(listing.joinable_roles());

    assert!(joinable.contains(JobFlags::SAGE));
    assert!(!joinable.contains(JobFlags::DANCER));
}

#[test]
fn high_end_ignores_generated_duty_metadata_without_client_signals() {
    let mut listing = sample_listing();
    listing.category = DutyCategory::Raid;

    assert!(!listing.pf_category_high_end);
    assert!(!listing.cfc_excel_high_end);
    assert!(!listing.client_high_end_duty);
    assert!(!listing.high_end());
}

#[test]
fn high_end_uses_uploaded_category_for_legacy_clients_without_telemetry() {
    let listing = sample_listing();

    assert_eq!(listing.category, DutyCategory::HighEndDuty);
    assert!(!listing.pf_category_high_end);
    assert!(!listing.cfc_excel_high_end);
    assert!(!listing.client_high_end_duty);
    assert!(listing.high_end());
}

#[test]
fn high_end_uses_any_uploaded_client_signal_without_generated_metadata() {
    let cases: [fn(&mut PartyFinderListing); 3] = [
        |listing| listing.client_high_end_duty = true,
        |listing| listing.pf_category_high_end = true,
        |listing| listing.cfc_excel_high_end = true,
    ];

    for mark_high_end in cases {
        let mut listing = sample_listing();
        listing.duty = 55;
        listing.duty_type = DutyType::Other;

        mark_high_end(&mut listing);

        assert!(listing.high_end());
    }
}

#[test]
fn client_high_end_signals_default_to_false_when_absent() {
    let listing: PartyFinderListing = serde_json::from_value(json!({
        "id": 166086,
        "content_id_lower": 40205661,
        "name": "WWFtYSBDb2ZmZWU=",
        "description": "VGVzdCBEZXNjcmlwdGlvbg==",
        "created_world": 44,
        "home_world": 44,
        "current_world": 44,
        "category": 64,
        "duty": 1010,
        "duty_type": 2,
        "beginners_welcome": false,
        "seconds_remaining": 1667,
        "min_item_level": 0,
        "num_parties": 1,
        "slots_available": 8,
        "last_server_restart": 1774336035,
        "objective": 2,
        "conditions": 2,
        "duty_finder_settings": 0,
        "loot_rules": 0,
        "search_area": 1,
        "slots": [],
        "jobs_present": []
    }))
    .expect("client high-end telemetry fields should be optional");

    assert!(!listing.pf_category_high_end);
    assert!(!listing.cfc_excel_high_end);
    assert!(!listing.client_high_end_duty);
}

#[test]
fn client_high_end_signals_are_preserved_when_uploaded() {
    let mut listing = sample_listing();
    listing.pf_category_high_end = true;
    listing.cfc_excel_high_end = false;
    listing.client_high_end_duty = true;

    let serialized = serde_json::to_value(&listing).expect("listing should serialize");
    let stored = bson::to_document(&listing).expect("listing should serialize to BSON");

    assert_eq!(serialized["pf_category_high_end"], true);
    assert_eq!(serialized["cfc_excel_high_end"], false);
    assert_eq!(serialized["client_high_end_duty"], true);
    assert_eq!(stored.get_bool("pf_category_high_end"), Ok(true));
    assert_eq!(stored.get_bool("cfc_excel_high_end"), Ok(false));
    assert_eq!(stored.get_bool("client_high_end_duty"), Ok(true));
}
