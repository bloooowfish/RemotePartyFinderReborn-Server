use chrono::Utc;
#[cfg(test)]
use std::collections::HashMap;

use super::payloads::ParseResult;
#[cfg(test)]
use crate::web::parse_ingest_common::build_result_encounter_map;
use crate::web::parse_ingest_common::{zone_cache_from_fields, ParseResultFields};

#[cfg(test)]
pub(super) fn build_fflogs_result_encounter_map(
    encounters: HashMap<i32, f64>,
    boss_percentages: HashMap<i32, f64>,
    clear_counts: HashMap<i32, i32>,
    phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
) -> HashMap<String, crate::mongo::EncounterParse> {
    build_result_encounter_map(encounters, boss_percentages, clear_counts, phase_progress)
}

pub(super) fn zone_cache_from_parse_result(
    res: ParseResult,
    fetched_at: chrono::DateTime<Utc>,
) -> crate::mongo::ZoneCache {
    zone_cache_from_fields(
        ParseResultFields {
            encounters: res.encounters,
            boss_percentages: res.boss_percentages,
            clear_counts: res.clear_counts,
            phase_progress: res.phase_progress,
            is_hidden: res.is_hidden,
            is_fflogs_hidden: res.is_hidden,
            is_estimated: res.is_estimated,
            matched_server: res.matched_server,
            source_kind: res.source_kind,
        },
        fetched_at,
    )
}
