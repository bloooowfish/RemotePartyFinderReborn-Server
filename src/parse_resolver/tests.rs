use std::collections::HashMap;

use chrono::Utc;

use crate::fflogs::{EncounterParse, PhaseProgress, ZoneCache};

use super::{ParseSource, resolve_parse_data};

fn zone_cache(
    hidden: bool,
    estimated: bool,
    encounters: HashMap<String, EncounterParse>,
) -> ZoneCache {
    ZoneCache {
        fetched_at: Utc::now(),
        estimated,
        matched_server: None,
        source_kind: None,
        hidden,
        encounters,
    }
}

fn encounter(
    percentile: f32,
    boss_percentage: Option<f32>,
    clear_count: Option<u32>,
) -> EncounterParse {
    EncounterParse {
        percentile,
        job_id: 0,
        boss_percentage,
        clear_count,
        phase_progress: Vec::new(),
    }
}

fn phase_progress(phase_key: &str) -> PhaseProgress {
    PhaseProgress {
        phase_key: phase_key.to_string(),
        phase_name: "Diamond Dust".to_string(),
        order: 2,
        boss_percentage: Some(52.4),
        display_text: "P2 52.4%".to_string(),
        source: Some("tomestone_api".to_string()),
    }
}

fn phase_progress_with_display(phase_key: &str, order: u8, display_text: &str) -> PhaseProgress {
    PhaseProgress {
        phase_key: phase_key.to_string(),
        phase_name: display_text
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string(),
        order,
        boss_percentage: None,
        display_text: display_text.to_string(),
        source: Some("tomestone_api".to_string()),
    }
}

fn phase_progress_with_boss_percentage(
    phase_key: &str,
    order: u8,
    boss_percentage: f32,
) -> PhaseProgress {
    let phase_name = format!("P{order}");
    PhaseProgress {
        phase_key: phase_key.to_string(),
        phase_name: phase_name.clone(),
        order,
        boss_percentage: Some(boss_percentage),
        display_text: format!("{phase_name} {boss_percentage}%"),
        source: Some("tomestone_api".to_string()),
    }
}

#[test]
fn resolve_parse_data_preserves_hidden_plugin_state_without_fallback() {
    let plugin = zone_cache(true, false, HashMap::new());

    let resolved = resolve_parse_data(Some(&plugin), 72, 123, None);

    assert_eq!(resolved.source, ParseSource::Plugin);
    assert!(resolved.hidden);
    assert_eq!(resolved.primary_percentile, None);
}

#[test]
fn resolve_parse_data_treats_zero_without_clear_count_as_no_data() {
    let plugin = zone_cache(
        false,
        false,
        HashMap::from([("123".to_string(), encounter(0.0, None, None))]),
    );

    let resolved = resolve_parse_data(Some(&plugin), 72, 123, None);

    assert_eq!(resolved.source, ParseSource::Plugin);
    assert_eq!(resolved.primary_percentile, None);
    assert_eq!(resolved.primary_color_class, "parse-none");
}

#[test]
fn resolve_parse_data_keeps_zero_percentile_when_clear_count_is_positive() {
    let plugin = zone_cache(
        false,
        false,
        HashMap::from([("123".to_string(), encounter(0.0, None, Some(1)))]),
    );

    let resolved = resolve_parse_data(Some(&plugin), 72, 123, None);

    assert_eq!(resolved.source, ParseSource::Plugin);
    assert_eq!(resolved.primary_percentile, Some(0));
    assert_eq!(resolved.primary_clear_count, Some(1));
}

#[test]
fn resolve_parse_data_truncates_percentile_instead_of_rounding_up_to_100() {
    let plugin = zone_cache(
        false,
        false,
        HashMap::from([("123".to_string(), encounter(99.9, None, Some(1)))]),
    );

    let resolved = resolve_parse_data(Some(&plugin), 72, 123, None);

    assert_eq!(resolved.primary_percentile, Some(99));
    assert_eq!(resolved.primary_color_class, "parse-pink");
}

#[test]
fn resolve_parse_data_omits_phase_progress_for_non_ultimate_encounters() {
    let mut enc = encounter(0.0, None, None);
    enc.phase_progress = vec![phase_progress("p2")];
    let plugin = zone_cache(false, false, HashMap::from([("1084".to_string(), enc)]));

    let resolved = resolve_parse_data(Some(&plugin), 72, 1084, None);

    assert!(resolved.primary_phase_progress.is_empty());
    assert!(resolved.secondary_phase_progress.is_empty());
}

#[test]
fn resolve_parse_data_hydrates_phase_progress_for_ultimate_encounters() {
    let mut primary = encounter(0.0, None, None);
    primary.phase_progress = vec![phase_progress("p2")];
    let plugin = zone_cache(false, false, HashMap::from([("1079".to_string(), primary)]));

    let resolved = resolve_parse_data(Some(&plugin), 65, 1079, None);

    assert_eq!(resolved.primary_phase_progress, vec![phase_progress("p2")]);
    assert!(resolved.secondary_phase_progress.is_empty());
}

#[test]
fn resolve_parse_data_omits_ultimate_boss_hp_when_phase_progress_is_missing() {
    let plugin = zone_cache(
        false,
        false,
        HashMap::from([("1076".to_string(), encounter(0.0, Some(1.81), None))]),
    );
    let plugin_resolved = resolve_parse_data(Some(&plugin), 59, 1076, None);

    assert_eq!(plugin_resolved.primary_boss_percentage, None);
    assert!(plugin_resolved.primary_phase_progress.is_empty());
}

#[test]
fn resolve_parse_data_ignores_zero_ultimate_phase_progress() {
    let mut primary = encounter(0.0, Some(0.0), None);
    primary.phase_progress = vec![phase_progress_with_boss_percentage("p2", 2, 0.0)];
    let plugin = zone_cache(false, false, HashMap::from([("1079".to_string(), primary)]));

    let resolved = resolve_parse_data(Some(&plugin), 65, 1079, None);

    assert_eq!(resolved.primary_boss_percentage, None);
    assert!(resolved.primary_phase_progress.is_empty());
}

#[test]
fn resolve_parse_data_filters_non_phase_progress_labels_for_ultimate_encounters() {
    let p4 = phase_progress_with_display("p4", 4, "P4 41.13%");
    let i1 = phase_progress_with_display("i1", 7, "I1 100%");
    let mut primary = encounter(0.0, None, None);
    primary.phase_progress = vec![p4.clone(), i1];
    let plugin = zone_cache(false, false, HashMap::from([("1075".to_string(), primary)]));

    let resolved = resolve_parse_data(Some(&plugin), 59, 1075, None);

    assert_eq!(resolved.primary_phase_progress, vec![p4]);
}

#[test]
fn resolve_parse_data_selects_best_phase_progress_for_ultimate_encounters() {
    let p4 = phase_progress_with_boss_percentage("p4", 4, 41.2);
    let p5_worse = phase_progress_with_boss_percentage("p5", 5, 68.71);
    let p5_best = phase_progress_with_boss_percentage("p5", 5, 67.53);
    let i1 = phase_progress_with_display("i1", 7, "I1 100%");
    let mut primary = encounter(0.0, None, None);
    primary.phase_progress = vec![p5_worse, p4, i1, p5_best.clone()];
    let plugin = zone_cache(false, false, HashMap::from([("1076".to_string(), primary)]));

    let resolved = resolve_parse_data(Some(&plugin), 59, 1076, None);

    assert_eq!(resolved.primary_phase_progress, vec![p5_best]);
}

fn mechanic_progress() -> PhaseProgress {
    PhaseProgress {
        phase_key: "mechanic-splattershed-2".to_string(),
        phase_name: "Splattershed #2".to_string(),
        order: 0,
        boss_percentage: Some(35.82),
        display_text: "Splattershed #2 35.82%".to_string(),
        source: Some("tomestone_api".to_string()),
    }
}

#[test]
fn resolve_parse_data_preserves_actual_tomestone_mechanic_label_without_fabricated_phase() {
    let mechanic = mechanic_progress();
    let mut primary = encounter(0.0, None, None);
    primary.phase_progress = vec![mechanic.clone()];
    let plugin = zone_cache(false, false, HashMap::from([("1079".to_string(), primary)]));

    let resolved = resolve_parse_data(Some(&plugin), 65, 1079, None);

    assert_eq!(resolved.primary_phase_progress, vec![mechanic]);
}

#[test]
fn resolver_rejects_malformed_or_unrelated_mechanic_progress() {
    let valid = mechanic_progress();
    let mut invalids = Vec::new();
    let mut phase = valid.clone();
    phase.phase_name.clear();
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.phase_name = "---".into();
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.source = Some("other".into());
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.order = 2;
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.phase_key = "mechanic-".into();
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.boss_percentage = Some(101.0);
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.boss_percentage = Some(0.0);
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.boss_percentage = None;
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.display_text = "Unrelated 35.82%".into();
    invalids.push(phase);
    let mut phase = valid.clone();
    phase.display_text = "Splattershed #2 75.82%".into();
    invalids.push(phase);

    for invalid in invalids {
        let mut primary = encounter(0.0, None, None);
        primary.phase_progress = vec![invalid.clone()];
        let plugin = zone_cache(false, false, HashMap::from([("1079".to_string(), primary)]));
        assert!(
            resolve_parse_data(Some(&plugin), 65, 1079, None)
                .primary_phase_progress
                .is_empty(),
            "accepted {invalid:?}"
        );
    }
}
