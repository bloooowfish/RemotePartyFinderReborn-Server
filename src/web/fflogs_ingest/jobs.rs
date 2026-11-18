use chrono::{DateTime, Utc};
use std::{cmp::Ordering, collections::HashMap};

use super::{payloads::ParseJob, request::FflogsWorkerRegion};
use crate::web::parse_ingest_common::{
    resolve_exact_player_home_server_and_region, should_dispatch_for_worker_region,
};

pub(super) fn normalized_lease_key(
    content_id: u64,
    zone_id: u32,
    difficulty_id: i32,
) -> crate::web::FflogsLeaseKey {
    crate::web::FflogsLeaseKey {
        content_id,
        zone_id,
        difficulty_id,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ActiveFflogsFetchPriority {
    MissingCache,
    VisibleRefreshDue,
    HiddenRefreshDue,
}

#[derive(Debug)]
struct ActiveFflogsFetchCandidate {
    content_id: u64,
    priority: ActiveFflogsFetchPriority,
    fetched_at: Option<DateTime<Utc>>,
    original_index: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ActiveFflogsFetchCandidates {
    pub(super) content_ids: Vec<u64>,
    pub(super) hidden_refresh_candidates: u64,
}

pub(super) fn active_fflogs_fetch_candidates(
    member_ids: Vec<u64>,
    cached_zones: &HashMap<u64, crate::mongo::ZoneCache>,
    hidden_ttl_hours: i64,
    active_refresh_minutes: i64,
    now: DateTime<Utc>,
) -> ActiveFflogsFetchCandidates {
    let mut hidden_refresh_candidates = 0u64;
    let mut candidates = Vec::new();

    for (original_index, content_id) in member_ids.into_iter().enumerate() {
        let Some(cache) = cached_zones.get(&content_id) else {
            candidates.push(ActiveFflogsFetchCandidate {
                content_id,
                priority: ActiveFflogsFetchPriority::MissingCache,
                fetched_at: None,
                original_index,
            });
            continue;
        };

        let Some(fflogs_fetched_at) = cache.effective_fflogs_fetched_at() else {
            candidates.push(ActiveFflogsFetchCandidate {
                content_id,
                priority: ActiveFflogsFetchPriority::MissingCache,
                fetched_at: None,
                original_index,
            });
            continue;
        };

        if !crate::fflogs::cache::is_zone_cache_refresh_due_for_active_listing(
            cache,
            hidden_ttl_hours,
            active_refresh_minutes,
            now,
        ) {
            continue;
        }

        let priority = if cache.is_fflogs_hidden() {
            hidden_refresh_candidates += 1;
            ActiveFflogsFetchPriority::HiddenRefreshDue
        } else {
            ActiveFflogsFetchPriority::VisibleRefreshDue
        };

        candidates.push(ActiveFflogsFetchCandidate {
            content_id,
            priority,
            fetched_at: Some(fflogs_fetched_at),
            original_index,
        });
    }

    candidates.sort_by(|a, b| {
        let fetched_at_order = match (a.fetched_at, b.fetched_at) {
            (Some(a), Some(b)) => a.cmp(&b),
            _ => Ordering::Equal,
        };

        a.priority
            .cmp(&b.priority)
            .then(fetched_at_order)
            .then_with(|| a.original_index.cmp(&b.original_index))
    });

    ActiveFflogsFetchCandidates {
        content_ids: candidates
            .into_iter()
            .map(|candidate| candidate.content_id)
            .collect(),
        hidden_refresh_candidates,
    }
}

pub(super) struct FflogsJobAppendContext<'a> {
    pub(super) limit: usize,
    pub(super) worker_region: Option<FflogsWorkerRegion>,
    pub(super) listing_anchor_world: u16,
    pub(super) fflogs_info: &'a crate::fflogs::FFLogsEncounter,
    pub(super) difficulty_id: i32,
}

pub(super) fn append_fflogs_jobs_for_players(
    jobs: &mut Vec<ParseJob>,
    ordered_content_ids: &[u64],
    players: Vec<crate::player::Player>,
    context: FflogsJobAppendContext<'_>,
) {
    let mut players_by_content_id = HashMap::new();
    for player in players {
        players_by_content_id
            .entry(player.content_id)
            .or_insert(player);
    }

    for content_id in ordered_content_ids {
        if jobs.len() >= context.limit {
            break;
        }

        let Some(player) = players_by_content_id.remove(content_id) else {
            continue;
        };

        let Some((server, region)) = resolve_exact_player_home_server_and_region(&player) else {
            continue;
        };

        if !should_dispatch_for_worker_region(
            context.worker_region,
            context.listing_anchor_world,
            &region,
        ) {
            continue;
        }

        jobs.push(ParseJob {
            content_id: player.content_id,
            name: player.name,
            server,
            region,
            candidate_servers: Vec::new(),
            zone_id: context.fflogs_info.zone_id,
            difficulty_id: context.difficulty_id,
            encounter_id: context.fflogs_info.encounter_id,
            secondary_encounter_id: context.fflogs_info.secondary_encounter_id,
            lease_token: String::new(),
        });
    }
}
