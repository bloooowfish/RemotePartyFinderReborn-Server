use std::collections::HashMap;

use chrono::Utc;

use crate::fflogs::PhaseProgress;

use super::{
    bounded_listing_lookup, resolve_member_player, should_lookup_member_fflogs_displays,
    ProgressDisplay, RenderableMember,
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
