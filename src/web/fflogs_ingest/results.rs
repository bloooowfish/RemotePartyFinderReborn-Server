use chrono::Utc;
use std::collections::HashMap;

use super::payloads::ParseResult;

fn positive_clear_count(clear_counts: &HashMap<i32, i32>, enc_id: i32) -> Option<u32> {
    clear_counts
        .get(&enc_id)
        .copied()
        .filter(|value| *value > 0)
        .map(|value| value as u32)
}

fn percentile_has_clear_evidence(percentile: f64, clear_count: Option<u32>) -> bool {
    percentile > 0.0 || (percentile == 0.0 && clear_count.is_some())
}

fn positive_boss_percentage(value: f64) -> Option<f32> {
    if value.is_finite() && value > 0.0 {
        Some(value as f32)
    } else {
        None
    }
}

fn valid_phase_progress(phase: &crate::mongo::PhaseProgress) -> bool {
    if phase.order == 0 || phase.phase_key.starts_with("mechanic-") {
        return crate::parse_resolver::tomestone_mechanic_percentage(phase).is_some();
    }

    match phase.boss_percentage {
        Some(value) => value.is_finite() && value > 0.0,
        None => true,
    }
}

fn sanitize_phase_progress(
    phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
) -> HashMap<i32, Vec<crate::mongo::PhaseProgress>> {
    phase_progress
        .into_iter()
        .filter_map(|(enc_id, phases)| {
            let phases = phases
                .into_iter()
                .filter(valid_phase_progress)
                .collect::<Vec<_>>();
            (!phases.is_empty()).then_some((enc_id, phases))
        })
        .collect()
}

pub(super) fn build_fflogs_result_encounter_map(
    encounters: HashMap<i32, f64>,
    boss_percentages: HashMap<i32, f64>,
    clear_counts: HashMap<i32, i32>,
    phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
) -> HashMap<String, crate::mongo::EncounterParse> {
    let mut encounter_map = HashMap::new();
    let mut phase_progress = sanitize_phase_progress(phase_progress);

    for (enc_id, percentile) in encounters {
        let boss_percentage = boss_percentages
            .get(&enc_id)
            .copied()
            .and_then(positive_boss_percentage);
        let clear_count = positive_clear_count(&clear_counts, enc_id);

        if percentile_has_clear_evidence(percentile, clear_count) {
            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: percentile as f32,
                    job_id: 0,
                    boss_percentage,
                    clear_count,
                    phase_progress: phase_progress.remove(&enc_id).unwrap_or_default(),
                },
            );
        } else if let Some(boss_percentage) = boss_percentage {
            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: -1.0,
                    job_id: 0,
                    boss_percentage: Some(boss_percentage),
                    clear_count,
                    phase_progress: phase_progress.remove(&enc_id).unwrap_or_default(),
                },
            );
        }
    }

    // progress-only 데이터가 들어온 경우도 저장
    for (enc_id, boss_percentage) in boss_percentages {
        let key = enc_id.to_string();
        if encounter_map.contains_key(&key) {
            continue;
        }
        let Some(boss_percentage) = positive_boss_percentage(boss_percentage) else {
            continue;
        };
        let clear_count = positive_clear_count(&clear_counts, enc_id);
        encounter_map.insert(
            key,
            crate::mongo::EncounterParse {
                percentile: -1.0,
                job_id: 0,
                boss_percentage: Some(boss_percentage),
                clear_count,
                phase_progress: phase_progress.remove(&enc_id).unwrap_or_default(),
            },
        );
    }

    // phase-progress-only data is additive and should still create an encounter entry.
    for (enc_id, phases) in phase_progress {
        if phases.is_empty() {
            continue;
        }
        let key = enc_id.to_string();
        if encounter_map.contains_key(&key) {
            continue;
        }
        let clear_count = positive_clear_count(&clear_counts, enc_id);
        encounter_map.insert(
            key,
            crate::mongo::EncounterParse {
                percentile: -1.0,
                job_id: 0,
                boss_percentage: None,
                clear_count,
                phase_progress: phases,
            },
        );
    }

    encounter_map
}

pub(super) fn zone_cache_from_parse_result(
    res: ParseResult,
    fetched_at: chrono::DateTime<Utc>,
) -> crate::mongo::ZoneCache {
    let encounter_map = build_fflogs_result_encounter_map(
        res.encounters,
        res.boss_percentages,
        res.clear_counts,
        res.phase_progress,
    );

    crate::mongo::ZoneCache {
        fetched_at,
        estimated: res.is_estimated,
        matched_server: res.matched_server.filter(|s| !s.trim().is_empty()),
        source_kind: res.source_kind.filter(|s| !s.trim().is_empty()),
        hidden: res.is_hidden,
        encounters: encounter_map,
    }
}
