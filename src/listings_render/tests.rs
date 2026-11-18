use std::collections::HashMap;

use chrono::Utc;

use crate::fflogs::PhaseProgress;

use super::{
    bounded_listing_lookup, build_listing_members_and_leader_parse, lookup_fflogs_displays,
    resolve_member_player, should_lookup_member_fflogs_displays, ProgressDisplay, RenderableMember,
};

fn sample_member(name: &str, home_world: u16) -> RenderableMember {
    RenderableMember {
        job_id: 19,
        player: crate::player::Player {
            content_id: 1,
            name: name.to_string(),
            home_world,
            current_world: home_world,
            last_seen: Utc::now(),
            seen_count: 1,
            account_id: "-1".to_string(),
        },
        parse: Default::default(),
        progress: Default::default(),
        slot_index: 0,
        party_index: 0,
        identity_fallback: false,
    }
}

#[tokio::test]
async fn bounded_listing_lookup_returns_default_on_timeout() {
    let result: Vec<i32> = bounded_listing_lookup(
        "test slow lookup",
        std::time::Duration::from_millis(5),
        async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok::<_, anyhow::Error>(vec![1, 2, 3])
        },
    )
    .await;

    assert!(result.is_empty());
}

#[test]
fn resolve_member_player_uses_existing_player_when_present() {
    let uid = 101u64;
    let mut players = HashMap::new();
    players.insert(
        uid,
        crate::player::Player {
            content_id: uid,
            name: "Known Player".to_string(),
            home_world: 73,
            current_world: 73,
            last_seen: Utc::now(),
            seen_count: 5,
            account_id: "123".to_string(),
        },
    );

    let (player, used_fallback) = resolve_member_player(&players, uid, true, "Leader", 74, 75);

    assert!(!used_fallback);
    assert_eq!(player.name, "Known Player");
    assert_eq!(player.home_world, 73);
}

#[test]
fn resolve_member_player_falls_back_to_leader_metadata_when_missing() {
    let players: HashMap<u64, crate::player::Player> = HashMap::new();

    let (player, used_fallback) = resolve_member_player(&players, 202, true, "Leader Name", 79, 80);

    assert!(used_fallback);
    assert_eq!(player.name, "Leader Name");
    assert_eq!(player.home_world, 79);
    assert_eq!(player.current_world, 80);
}

#[test]
fn resolve_member_player_keeps_unknown_for_non_leader_missing_player() {
    let players: HashMap<u64, crate::player::Player> = HashMap::new();

    let (player, used_fallback) =
        resolve_member_player(&players, 303, false, "Leader Name", 79, 80);

    assert!(!used_fallback);
    assert_eq!(player.name, "Unknown Member");
    assert_eq!(player.home_world, 0);
}

#[test]
fn identity_upsert_updates_player_lookup_used_by_listing_render() {
    let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let now = observed_at + chrono::TimeDelta::minutes(1);
    let identity = crate::player::UploadableCharacterIdentity {
        content_id: 5150,
        name: "Resolved Member".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at,
    };

    let merged = crate::player::merge_identity_into_player(None, None, &identity, now);
    let mut players = HashMap::new();
    players.insert(merged.player.content_id, merged.player.clone());

    let (player, used_fallback) =
        resolve_member_player(&players, identity.content_id, false, "Leader", 79, 79);

    assert!(merged.applied_incoming_identity);
    assert!(!used_fallback);
    assert_eq!(player.name, "Resolved Member");
    assert_eq!(player.home_world, 74);
}

#[test]
fn member_link_is_generated_for_known_name_and_world() {
    let member = sample_member("Sayo Shijima", 73);
    let link = member
        .fflogs_character_url()
        .expect("expected FFLogs profile link for known member");

    assert!(link.starts_with("https://www.fflogs.com/character/"));
    assert!(link.ends_with("/Sayo%20Shijima"));
}

#[test]
fn member_link_is_not_generated_for_unknown_or_placeholder_names() {
    let unknown = sample_member("Unknown Member", 73);
    assert!(unknown.fflogs_character_url().is_none());

    let placeholder = sample_member("Party Leader", 73);
    assert!(placeholder.fflogs_character_url().is_none());
}

#[test]
fn member_link_is_not_generated_for_fallback_identity() {
    let mut fallback = sample_member("Sayo Shijima", 73);
    fallback.identity_fallback = true;

    assert!(fallback.fflogs_character_url().is_none());
}

#[test]
fn member_fflogs_display_lookup_requires_known_name_and_world() {
    assert!(should_lookup_member_fflogs_displays(
        &sample_member("Sayo Shijima", 73).player
    ));
    assert!(!should_lookup_member_fflogs_displays(
        &sample_member("Unknown Member", 73).player
    ));
    assert!(!should_lookup_member_fflogs_displays(
        &sample_member("Party Leader", 73).player
    ));
    assert!(!should_lookup_member_fflogs_displays(
        &sample_member("Sayo Shijima", 0).player
    ));
}

#[test]
fn progress_hides_boss_hp_when_percentile_exists_and_shows_clear_count() {
    let progress = ProgressDisplay::new(
        Some(23.0),
        None,
        Some(95),
        None,
        Some(7),
        None,
        Vec::new(),
        Vec::new(),
        false,
        false,
    );

    assert_eq!(progress.final_boss_percentage, None);
    assert_eq!(progress.final_clear_count, Some(7));
}

#[test]
fn progress_shows_boss_hp_when_not_cleared() {
    let progress = ProgressDisplay::new(
        Some(17.12),
        None,
        None,
        None,
        None,
        None,
        Vec::new(),
        Vec::new(),
        false,
        false,
    );

    assert_eq!(progress.final_boss_percentage, Some(17.12));
    assert_eq!(progress.final_clear_count, None);
}

#[test]
fn progress_hides_boss_hp_when_phase_progress_exists() {
    let phase_progress = vec![PhaseProgress {
        phase_key: "p2".to_string(),
        phase_name: "P2".to_string(),
        order: 2,
        boss_percentage: Some(75.71),
        display_text: "P2 75.71%".to_string(),
        source: Some("tomestone_api".to_string()),
    }];
    let progress = ProgressDisplay::new(
        Some(76.45),
        None,
        None,
        None,
        None,
        None,
        phase_progress.clone(),
        Vec::new(),
        false,
        false,
    );

    assert_eq!(progress.final_boss_percentage, None);
    assert_eq!(progress.phase_progress, phase_progress);
}

#[test]
fn split_progress_labels_first_boss_hp_when_second_boss_has_no_data() {
    let progress = ProgressDisplay::new(
        Some(35.2),
        None,
        None,
        None,
        None,
        None,
        Vec::new(),
        Vec::new(),
        true,
        false,
    );
    assert_eq!(progress.final_boss_percentage, Some(35.2));
    assert_eq!(progress.final_boss_phase, Some(1));
    assert_eq!(progress.final_clear_count, None);
}

#[test]
fn split_progress_ignores_invalid_second_boss_hp_for_first_boss_fallback() {
    for invalid_boss in [f32::NAN, f32::INFINITY, 0.0, -1.0, 100.1] {
        let progress = ProgressDisplay::new(
            Some(35.2),
            Some(invalid_boss),
            None,
            None,
            None,
            Some(0),
            Vec::new(),
            Vec::new(),
            true,
            false,
        );
        assert_eq!(progress.final_boss_percentage, Some(35.2));
        assert_eq!(progress.final_boss_phase, Some(1));
    }
}

#[test]
fn split_progress_keeps_second_boss_hp_clear_and_percentile_priority() {
    for (boss, percentile, clears, expected_boss, expected_clears) in [
        (Some(72.0), None, None, Some(72.0), None),
        (None, Some(95), None, None, None),
        (None, None, Some(4), None, Some(4)),
    ] {
        let progress = ProgressDisplay::new(
            Some(35.2),
            boss,
            None,
            percentile,
            None,
            clears,
            Vec::new(),
            Vec::new(),
            true,
            false,
        );
        assert_eq!(progress.final_boss_percentage, expected_boss);
        assert_eq!(progress.final_boss_phase, expected_boss.map(|_| 2));
        assert_eq!(progress.final_clear_count, expected_clears);
    }
}

#[test]
fn split_progress_ignores_all_first_boss_data_when_second_boss_hp_exists() {
    let progress = ProgressDisplay::new(
        Some(1.0),
        Some(90.0),
        Some(99),
        None,
        Some(8),
        None,
        vec![PhaseProgress {
            phase_key: "p1".to_string(),
            phase_name: "P1".to_string(),
            order: 1,
            boss_percentage: Some(1.0),
            display_text: "P1 1.00%".to_string(),
            source: Some("tomestone_api".to_string()),
        }],
        Vec::new(),
        true,
        false,
    );
    assert_eq!(progress.final_boss_percentage, Some(90.0));
    assert_eq!(progress.final_boss_phase, Some(2));
    assert_eq!(progress.final_clear_count, None);
    assert!(progress.phase_progress.is_empty());
}

#[test]
fn split_progress_keeps_second_boss_phase_priority() {
    let phases = vec![PhaseProgress {
        phase_key: "p2".to_string(),
        phase_name: "P2".to_string(),
        order: 2,
        boss_percentage: Some(75.71),
        display_text: "P2 75.71%".to_string(),
        source: Some("tomestone_api".to_string()),
    }];
    let progress = ProgressDisplay::new(
        Some(35.2),
        None,
        None,
        None,
        None,
        None,
        Vec::new(),
        phases.clone(),
        true,
        false,
    );
    assert_eq!(progress.final_boss_percentage, None);
    assert_eq!(progress.final_boss_phase, None);
    assert_eq!(progress.phase_progress, phases);
}

#[test]
fn split_progress_never_promotes_first_boss_clear_to_full_clear() {
    for (percentile, clears) in [(Some(95), None), (None, Some(3))] {
        let progress = ProgressDisplay::new(
            Some(35.2),
            None,
            percentile,
            None,
            clears,
            None,
            Vec::new(),
            Vec::new(),
            true,
            false,
        );
        assert_eq!(progress.final_boss_percentage, None);
        assert_eq!(progress.final_boss_phase, None);
        assert_eq!(progress.final_clear_count, None);
    }
}

#[test]
fn split_progress_suppresses_hidden_first_boss_hp() {
    let progress = ProgressDisplay::new(
        Some(35.2),
        None,
        None,
        None,
        None,
        None,
        Vec::new(),
        Vec::new(),
        true,
        true,
    );
    assert_eq!(progress.final_boss_percentage, None);
    assert_eq!(progress.final_boss_phase, None);
}

#[test]
fn progress_rejects_invalid_first_and_final_boss_hp() {
    for boss in [f32::NAN, f32::INFINITY, -1.0, 0.0, 100.1] {
        for split in [false, true] {
            let progress = ProgressDisplay::new(
                Some(boss),
                None,
                None,
                None,
                None,
                None,
                Vec::new(),
                Vec::new(),
                split,
                false,
            );
            assert_eq!(progress.final_boss_percentage, None);
            assert_eq!(progress.final_boss_phase, None);
        }
    }
}

#[test]
fn is_member_leader_respects_explicit_leader_content_id_before_slot_zero_fallback() {
    assert!(!super::is_member_leader(202, 101, 0));
    assert!(super::is_member_leader(202, 202, 1));
    assert!(super::is_member_leader(0, 101, 0));
    assert!(!super::is_member_leader(0, 202, 1));
}

fn sample_render_listing(
    jobs_present: Vec<u8>,
    member_jobs: Vec<u8>,
    member_content_ids: Vec<i64>,
    leader_content_id: u64,
) -> crate::listing::PartyFinderListing {
    use crate::listing::{
        ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, LootRuleFlags,
        ObjectiveFlags, SearchAreaFlags,
    };
    use sestring::SeString;

    crate::listing::PartyFinderListing {
        id: 1001,
        content_id_lower: 101,
        name: SeString::parse(b"Leader Player").unwrap(),
        description: SeString::parse(b"Test").unwrap(),
        created_world: 73,
        home_world: 73,
        current_world: 73,
        category: DutyCategory::HighEndDuty,
        duty: 1010,
        duty_type: DutyType::Normal,
        pf_category_high_end: false,
        cfc_excel_high_end: false,
        client_high_end_duty: false,
        beginners_welcome: false,
        seconds_remaining: 1800,
        min_item_level: 0,
        num_parties: 1,
        slots_available: 8,
        last_server_restart: 1_700_000_000,
        objective: ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::NONE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: Vec::new(),
        jobs_present,
        member_content_ids,
        member_jobs,
        detail_slot_flags: Vec::new(),
        leader_content_id,
    }
}

fn sample_parse_doc(content_id: i64, percentile: f32) -> crate::mongo::ParseCacheDoc {
    let mut encounters = HashMap::new();
    encounters.insert(
        "101".to_string(),
        crate::fflogs::EncounterParse {
            percentile,
            job_id: 0,
            boss_percentage: None,
            clear_count: Some(2),
            phase_progress: Vec::new(),
            tomestone_fetched_at: None,
        },
    );
    crate::mongo::ParseCacheDoc {
        content_id,
        zones: HashMap::from([(
            "73:101".to_string(),
            crate::fflogs::ZoneCache {
                fetched_at: Utc::now(),
                fflogs_fetched_at: Some(Utc::now()),
                tomestone_fetched_at: None,
                estimated: false,
                matched_server: Some("Adamantoise".to_string()),
                source_kind: Some("fflogs_api".to_string()),
                hidden: false,
                fflogs_hidden: false,
                encounters,
            },
        )]),
    }
}

#[test]
fn reconcile_omits_empty_slots_and_suppresses_mismatched_member_parses() {
    let now = Utc::now();
    // Slot 0: matched PLD (19)
    // Slot 1: jobs_present == 0 -> empty even though stale detail has cid=202, job=24
    // Slot 2: jobs_present == 33 (SGE), stale member_jobs == 22 (DRG), cid=303 -> display job 33, suppress cid=303 parse
    let listing = sample_render_listing(
        vec![19, 0, 33, 0, 0, 0, 0, 0],
        vec![19, 24, 22, 0, 0, 0, 0, 0],
        vec![101, 202, 303, 0, 0, 0, 0, 0],
        101,
    );

    let players = HashMap::from([
        (
            101_u64,
            crate::player::Player {
                content_id: 101,
                name: "Leader Player".to_string(),
                home_world: 73,
                current_world: 73,
                last_seen: now,
                seen_count: 1,
                account_id: "-1".to_string(),
            },
        ),
        (
            202_u64,
            crate::player::Player {
                content_id: 202,
                name: "Ghost Left".to_string(),
                home_world: 73,
                current_world: 73,
                last_seen: now,
                seen_count: 1,
                account_id: "-1".to_string(),
            },
        ),
        (
            303_u64,
            crate::player::Player {
                content_id: 303,
                name: "Stale Occupant".to_string(),
                home_world: 73,
                current_world: 73,
                last_seen: now,
                seen_count: 1,
                account_id: "-1".to_string(),
            },
        ),
    ]);
    let parse_docs = HashMap::from([
        (101_u64, sample_parse_doc(101, 95.0)),
        (202_u64, sample_parse_doc(202, 88.0)),
        (303_u64, sample_parse_doc(303, 99.0)),
    ]);

    let (members, leader_parse, _) = build_listing_members_and_leader_parse(
        &listing,
        &players,
        &parse_docs,
        73,
        "73:101",
        101,
        None,
        "Leader Player",
    );

    assert_eq!(members.len(), 2);
    assert_eq!(members[0].slot_index, 0);
    assert_eq!(members[0].job_id, 19);
    assert_eq!(members[0].player.name, "Leader Player");
    assert_eq!(members[0].parse.primary_percentile, Some(95));
    assert_eq!(leader_parse.primary_percentile, Some(95));

    // Slot 1 is omitted completely.
    assert!(members.iter().all(|m| m.slot_index != 1));

    // Slot 2 displays jobs_present[2] = 33 and suppresses stale cid=303 parse/identity.
    assert_eq!(members[1].slot_index, 2);
    assert_eq!(members[1].job_id, 33);
    assert_eq!(members[1].parse.primary_percentile, None);
    assert_eq!(members[1].player.name, "Unknown Member");
    assert!(members[1].fflogs_character_url().is_none());
}

#[test]
fn reconcile_omits_stale_leader_parse_when_leader_slot_is_empty_in_jobs_present() {
    let listing = sample_render_listing(
        vec![0, 0, 0, 0, 0, 0, 0, 0],
        vec![19, 0, 0, 0, 0, 0, 0, 0],
        vec![101, 0, 0, 0, 0, 0, 0, 0],
        101,
    );
    let parse_docs = HashMap::from([(101_u64, sample_parse_doc(101, 95.0))]);

    let (members, leader_parse, _) = build_listing_members_and_leader_parse(
        &listing,
        &HashMap::new(),
        &parse_docs,
        73,
        "73:101",
        101,
        None,
        "Leader Player",
    );

    assert!(members.is_empty());
    assert_eq!(leader_parse.primary_percentile, None);
}

#[test]
fn lookup_fflogs_displays_keeps_tomestone_progress_when_fflogs_is_hidden() {
    let now = Utc::now();
    let doc = crate::mongo::ParseCacheDoc {
        content_id: 500,
        zones: HashMap::from([(
            "73:101".to_string(),
            crate::fflogs::ZoneCache {
                fetched_at: now,
                fflogs_fetched_at: Some(now),
                tomestone_fetched_at: Some(now),
                estimated: false,
                matched_server: None,
                source_kind: Some("fflogs_api".to_string()),
                hidden: true,
                fflogs_hidden: true,
                encounters: HashMap::from([(
                    "101".to_string(),
                    crate::fflogs::EncounterParse {
                        percentile: -1.0,
                        job_id: 0,
                        boss_percentage: Some(27.5),
                        clear_count: None,
                        phase_progress: Vec::new(),
                        tomestone_fetched_at: Some(now),
                    },
                )]),
            },
        )]),
    };
    let parse_docs = HashMap::from([(500_u64, doc)]);

    let (parse, progress) = lookup_fflogs_displays(&parse_docs, 500, 73, "73:101", 101, None);
    assert!(parse.hidden);
    assert_eq!(parse.primary_percentile, None);
    assert_eq!(progress.final_boss_percentage, Some(27.5));
}

#[tokio::test]
async fn fetch_listing_enrichment_requires_complete_enrichment_when_stale_exists() {
    use super::{fetch_listing_enrichment, EnrichmentFallbackMode};

    let err = fetch_listing_enrichment(
        true,
        EnrichmentFallbackMode::RequireCompleteEnrichment,
        || async { Err::<Vec<crate::player::Player>, _>(anyhow::anyhow!("players error")) },
        || async { Ok::<HashMap<u64, crate::mongo::ParseCacheDoc>, anyhow::Error>(HashMap::new()) },
    )
    .await
    .expect_err("stale-preserving mode must fail when players lookup errors");
    assert!(err.to_string().contains("players error"));

    let err = fetch_listing_enrichment(
        true,
        EnrichmentFallbackMode::RequireCompleteEnrichment,
        || async { Ok::<Vec<crate::player::Player>, anyhow::Error>(Vec::new()) },
        || async { Err::<HashMap<u64, crate::mongo::ParseCacheDoc>, _>(anyhow::anyhow!("parses error")) },
    )
    .await
    .expect_err("stale-preserving mode must fail when parse docs lookup errors");
    assert!(err.to_string().contains("parses error"));

    let (players, parse_docs) = fetch_listing_enrichment(
        true,
        EnrichmentFallbackMode::AllowFallback,
        || async { Err::<Vec<crate::player::Player>, _>(anyhow::anyhow!("players error")) },
        || async { Err::<HashMap<u64, crate::mongo::ParseCacheDoc>, _>(anyhow::anyhow!("parses error")) },
    )
    .await
    .expect("cold-cache mode should fall back to empty collections");
    assert!(players.is_empty());
    assert!(parse_docs.is_empty());
}

#[tokio::test]
async fn fetch_listing_enrichment_runs_players_and_parses_concurrently_and_skips_when_empty() {
    use super::{fetch_listing_enrichment, EnrichmentFallbackMode};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let players_called = Arc::new(AtomicBool::new(false));
    let parses_called = Arc::new(AtomicBool::new(false));
    let (p, d) = fetch_listing_enrichment(
        false,
        EnrichmentFallbackMode::RequireCompleteEnrichment,
        || {
            let flag = Arc::clone(&players_called);
            async move {
                flag.store(true, Ordering::SeqCst);
                Ok(Vec::new())
            }
        },
        || {
            let flag = Arc::clone(&parses_called);
            async move {
                flag.store(true, Ordering::SeqCst);
                Ok(HashMap::new())
            }
        },
    )
    .await
    .unwrap();
    assert!(p.is_empty());
    assert!(d.is_empty());
    assert!(!players_called.load(Ordering::SeqCst));
    assert!(!parses_called.load(Ordering::SeqCst));

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let b1 = Arc::clone(&barrier);
    let b2 = Arc::clone(&barrier);
    let (players, parses) = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        fetch_listing_enrichment(
            true,
            EnrichmentFallbackMode::RequireCompleteEnrichment,
            move || async move {
                b1.wait().await;
                Ok(Vec::new())
            },
            move || async move {
                b2.wait().await;
                Ok(HashMap::new())
            },
        ),
    )
    .await
    .expect("players and parse docs must run concurrently without deadlock")
    .unwrap();
    assert!(players.is_empty());
    assert!(parses.is_empty());
}
