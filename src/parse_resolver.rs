use std::collections::HashMap;

use serde::Serialize;

use crate::fflogs::{EncounterParse, PhaseProgress, ZoneCache};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseSource {
    #[default]
    None,
    Plugin,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedParseData {
    pub source: ParseSource,
    pub primary_percentile: Option<u8>,
    pub primary_color_class: String,
    pub secondary_percentile: Option<u8>,
    pub secondary_color_class: String,
    pub has_secondary: bool,
    pub hidden: bool,
    pub originally_hidden: bool,
    pub estimated: bool,
    pub primary_boss_percentage: Option<f32>,
    pub secondary_boss_percentage: Option<f32>,
    pub primary_clear_count: Option<u16>,
    pub secondary_clear_count: Option<u16>,
    pub primary_phase_progress: Vec<PhaseProgress>,
    pub secondary_phase_progress: Vec<PhaseProgress>,
}

impl ResolvedParseData {
    fn empty(has_secondary: bool) -> Self {
        Self {
            source: ParseSource::None,
            primary_percentile: None,
            primary_color_class: "parse-none".to_string(),
            secondary_percentile: None,
            secondary_color_class: "parse-none".to_string(),
            has_secondary,
            hidden: false,
            originally_hidden: false,
            estimated: false,
            primary_boss_percentage: None,
            secondary_boss_percentage: None,
            primary_clear_count: None,
            secondary_clear_count: None,
            primary_phase_progress: Vec::new(),
            secondary_phase_progress: Vec::new(),
        }
    }
}

fn hydrate_slot(
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
) -> (Option<u8>, String, Option<f32>, Option<u16>) {
    hydrate_slot_with_display_rule(encounters, encounter_id, encounter_has_display_percentile)
}

fn hydrate_slot_with_display_rule(
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
    has_display_percentile: fn(&EncounterParse) -> bool,
) -> (Option<u8>, String, Option<f32>, Option<u16>) {
    let Some(enc_parse) = encounters.get(&encounter_id.to_string()) else {
        return (None, "parse-none".to_string(), None, None);
    };

    let percentile = if has_display_percentile(enc_parse) {
        Some(enc_parse.percentile.clamp(0.0, 100.0).floor() as u8)
    } else {
        None
    };

    let color_class = percentile
        .map(|_| crate::fflogs::mapping::percentile_color_class(enc_parse.percentile).to_string())
        .unwrap_or_else(|| "parse-none".to_string());

    let boss_percentage = enc_parse
        .boss_percentage
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 100.0));
    let clear_count = enc_parse
        .clear_count
        .map(|value| value.min(u16::MAX as u32) as u16);

    (percentile, color_class, boss_percentage, clear_count)
}

fn encounter_has_positive_clear_count(encounter: &EncounterParse) -> bool {
    encounter.clear_count.is_some_and(|value| value > 0)
}

fn encounter_has_display_percentile(encounter: &EncounterParse) -> bool {
    encounter.percentile > 0.0
        || (encounter.percentile == 0.0 && encounter_has_positive_clear_count(encounter))
}

fn hydrate_phase_progress(
    encounters: &HashMap<String, EncounterParse>,
    zone_id: u32,
    encounter_id: u32,
) -> Vec<PhaseProgress> {
    if !crate::fflogs::is_ultimate_encounter(zone_id, encounter_id) {
        return Vec::new();
    }

    encounters
        .get(&encounter_id.to_string())
        .map(|encounter| select_best_phase_progress(&encounter.phase_progress))
        .unwrap_or_default()
}

fn suppress_ultimate_boss_percentage_without_phase_progress(
    zone_id: u32,
    encounter_id: u32,
    encounter_had_phase_progress: bool,
    boss_percentage: &mut Option<f32>,
) {
    if crate::fflogs::is_ultimate_encounter(zone_id, encounter_id) && !encounter_had_phase_progress
    {
        *boss_percentage = None;
    }
}

#[derive(Clone, Copy)]
struct PhaseProgressRank {
    order: u32,
    boss_percentage: f32,
}

fn select_best_phase_progress(phases: &[PhaseProgress]) -> Vec<PhaseProgress> {
    let mut best: Option<(&PhaseProgress, PhaseProgressRank)> = None;

    for phase in phases {
        let Some(rank) = parse_displayable_phase_progress(phase) else {
            continue;
        };

        if best
            .map(|(_, best_rank)| is_better_phase_progress(rank, best_rank))
            .unwrap_or(true)
        {
            best = Some((phase, rank));
        }
    }

    best.map(|(phase, _)| vec![phase.clone()])
        .unwrap_or_default()
}

fn parse_p_label_phase_progress(phase: &PhaseProgress) -> Option<PhaseProgressRank> {
    let mut parts = phase.display_text.split_whitespace();
    let label = parts.next()?;
    let percent_label = parts.next()?;

    let order = label
        .strip_prefix('P')
        .or_else(|| label.strip_prefix('p'))?
        .parse::<u32>()
        .ok()?;
    let boss_percentage = percent_label.strip_suffix('%')?.parse::<f32>().ok()?;
    if !boss_percentage.is_finite() || !(0.0..=100.0).contains(&boss_percentage) {
        return None;
    }

    Some(PhaseProgressRank {
        order,
        boss_percentage,
    })
}

fn parse_displayable_phase_progress(phase: &PhaseProgress) -> Option<PhaseProgressRank> {
    if phase.phase_key.starts_with("mechanic-") {
        return tomestone_mechanic_percentage(phase).map(|boss_percentage| PhaseProgressRank {
            order: 0,
            boss_percentage,
        });
    }

    if let Some(rank) = parse_p_label_phase_progress(phase) {
        return Some(rank);
    }

    structured_phase_percentage(phase).map(|boss_percentage| PhaseProgressRank {
        order: u32::from(phase.order),
        boss_percentage,
    })
}

pub(crate) fn is_valid_phase_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 640
        && name.chars().any(char::is_alphanumeric)
        && !name.chars().any(char::is_control)
}

fn matching_phase_display_percentage(name: &str, display_text: &str, percentage: f32) -> bool {
    if !percentage.is_finite() || !(0.0..=100.0).contains(&percentage) {
        return false;
    }
    let Some(suffix) = display_text.trim().strip_prefix(name) else {
        return false;
    };
    if !suffix.starts_with(char::is_whitespace) {
        return false;
    }
    let Some(display_percentage) = suffix
        .trim()
        .strip_suffix('%')
        .and_then(|raw| raw.parse::<f32>().ok())
    else {
        return false;
    };
    display_percentage.is_finite() && (display_percentage - percentage).abs() < 0.005
}

pub(crate) fn structured_phase_percentage(phase: &PhaseProgress) -> Option<f32> {
    if phase.order == 0 {
        return None;
    }
    let name = phase.phase_name.trim();
    if !is_valid_phase_name(name) {
        return None;
    }
    let percentage = phase.boss_percentage?;
    matching_phase_display_percentage(name, &phase.display_text, percentage).then_some(percentage)
}

// A mechanic's recurrence number is not an encounter phase. Keep its actual label
// without assigning a fabricated P-number, and admit only the explicit API shape.
pub(crate) fn tomestone_mechanic_percentage(phase: &PhaseProgress) -> Option<f32> {
    let key = phase.phase_key.strip_prefix("mechanic-")?;
    let name = phase.phase_name.trim();
    if !phase
        .source
        .as_deref()
        .is_some_and(|source| source.starts_with("tomestone"))
        || phase.order != 0
        || key.is_empty()
        || !is_valid_phase_name(name)
    {
        return None;
    }

    let percentage = phase.boss_percentage?;
    matching_phase_display_percentage(name, &phase.display_text, percentage).then_some(percentage)
}

fn is_better_phase_progress(candidate: PhaseProgressRank, current: PhaseProgressRank) -> bool {
    candidate.order > current.order
        || (candidate.order == current.order && candidate.boss_percentage < current.boss_percentage)
}

fn hydrate_resolved(
    resolved: &mut ResolvedParseData,
    encounters: &HashMap<String, EncounterParse>,
    zone_id: u32,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) {
    let (p1, p1_class, mut p1_boss, p1_clears) = hydrate_slot(encounters, encounter_id);
    let p1_phase_progress = hydrate_phase_progress(encounters, zone_id, encounter_id);
    let p1_had_phase_progress = encounters
        .get(&encounter_id.to_string())
        .is_some_and(|enc| !enc.phase_progress.is_empty());
    suppress_ultimate_boss_percentage_without_phase_progress(
        zone_id,
        encounter_id,
        p1_had_phase_progress,
        &mut p1_boss,
    );
    resolved.primary_percentile = p1;
    resolved.primary_color_class = p1_class;
    resolved.primary_boss_percentage = p1_boss;
    resolved.primary_clear_count = p1_clears;
    resolved.primary_phase_progress = p1_phase_progress;

    if let Some(sec_id) = secondary_encounter_id {
        let (p2, p2_class, mut p2_boss, p2_clears) = hydrate_slot(encounters, sec_id);
        let p2_phase_progress = hydrate_phase_progress(encounters, zone_id, sec_id);
        let p2_had_phase_progress = encounters
            .get(&sec_id.to_string())
            .is_some_and(|enc| !enc.phase_progress.is_empty());
        suppress_ultimate_boss_percentage_without_phase_progress(
            zone_id,
            sec_id,
            p2_had_phase_progress,
            &mut p2_boss,
        );
        resolved.secondary_percentile = p2;
        resolved.secondary_color_class = p2_class;
        resolved.secondary_boss_percentage = p2_boss;
        resolved.secondary_clear_count = p2_clears;
        resolved.secondary_phase_progress = p2_phase_progress;
    }
}

fn encounter_has_tomestone_data(zone_cache: &ZoneCache, enc: &EncounterParse) -> bool {
    enc.tomestone_fetched_at.is_some()
        || zone_cache.tomestone_fetched_at.is_some()
        || !enc.phase_progress.is_empty()
        || zone_cache
            .source_kind
            .as_deref()
            .is_some_and(|s| s.contains("tomestone"))
}

fn hydrate_tomestone_progress_only(
    resolved: &mut ResolvedParseData,
    zone_cache: &ZoneCache,
    zone_id: u32,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) {
    let tomestone_only: HashMap<String, EncounterParse> = zone_cache
        .encounters
        .iter()
        .filter(|(_, enc)| encounter_has_tomestone_data(zone_cache, enc))
        .map(|(k, enc)| {
            (
                k.clone(),
                EncounterParse {
                    percentile: -1.0,
                    job_id: 0,
                    boss_percentage: enc.boss_percentage,
                    clear_count: enc.clear_count,
                    phase_progress: enc.phase_progress.clone(),
                    tomestone_fetched_at: enc.tomestone_fetched_at,
                },
            )
        })
        .collect();

    if tomestone_only.is_empty() {
        return;
    }

    hydrate_resolved(
        resolved,
        &tomestone_only,
        zone_id,
        encounter_id,
        secondary_encounter_id,
    );
    // FFLogs is hidden; never leak percentiles even if a Tomestone encounter somehow had one.
    resolved.primary_percentile = None;
    resolved.primary_color_class = "parse-none".to_string();
    resolved.secondary_percentile = None;
    resolved.secondary_color_class = "parse-none".to_string();
}

pub fn resolve_parse_data(
    plugin_zone_cache: Option<&ZoneCache>,
    zone_id: u32,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> ResolvedParseData {
    let has_secondary = secondary_encounter_id.is_some();
    let mut resolved = ResolvedParseData::empty(has_secondary);

    let Some(zone_cache) = plugin_zone_cache else {
        return resolved;
    };

    resolved.source = ParseSource::Plugin;
    if zone_cache.is_fflogs_hidden() {
        resolved.hidden = true;
        hydrate_tomestone_progress_only(
            &mut resolved,
            zone_cache,
            zone_id,
            encounter_id,
            secondary_encounter_id,
        );
        return resolved;
    }

    resolved.estimated = zone_cache.estimated;
    hydrate_resolved(
        &mut resolved,
        &zone_cache.encounters,
        zone_id,
        encounter_id,
        secondary_encounter_id,
    );
    resolved
}

#[cfg(test)]
mod tests;
