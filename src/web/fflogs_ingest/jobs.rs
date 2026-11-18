use chrono::{DateTime, Utc};
use std::{cmp::Ordering, collections::HashMap};

use super::{payloads::ParseJob, request::FflogsWorkerRegion};
use crate::web::parse_ingest_common::{
    build_parse_job, resolve_exact_player_home_server_and_region,
    should_dispatch_for_worker_region,
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
pub(super) enum ActiveFflogsFetchPriority {
    MissingCache,
    VisibleRefreshDue,
    HiddenRefreshDue,
}

#[derive(Debug)]
pub(super) struct ActiveFflogsRankedCandidate {
    pub(super) content_id: u64,
    pub(super) priority: ActiveFflogsFetchPriority,
    pub(super) fetched_at: Option<DateTime<Utc>>,
    pub(super) slot_index: usize,
}

#[derive(Debug)]
pub(super) struct ActiveFflogsRankedCandidates {
    pub(super) candidates: Vec<ActiveFflogsRankedCandidate>,
    pub(super) hidden_refresh_candidates: u64,
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ActiveFflogsFetchCandidates {
    pub(super) content_ids: Vec<u64>,
    pub(super) hidden_refresh_candidates: u64,
}

pub(super) fn active_fflogs_ranked_candidates(
    member_slots: &[(usize, u64)],
    cached_zones: &HashMap<u64, crate::mongo::ZoneCache>,
    hidden_ttl_hours: i64,
    active_refresh_minutes: i64,
    now: DateTime<Utc>,
) -> ActiveFflogsRankedCandidates {
    let mut hidden_refresh_candidates = 0u64;
    let mut candidates = Vec::new();

    for &(slot_index, content_id) in member_slots {
        let Some(cache) = cached_zones.get(&content_id) else {
            candidates.push(ActiveFflogsRankedCandidate {
                content_id,
                priority: ActiveFflogsFetchPriority::MissingCache,
                fetched_at: None,
                slot_index,
            });
            continue;
        };

        let Some(fflogs_fetched_at) = cache.effective_fflogs_fetched_at() else {
            candidates.push(ActiveFflogsRankedCandidate {
                content_id,
                priority: ActiveFflogsFetchPriority::MissingCache,
                fetched_at: None,
                slot_index,
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

        candidates.push(ActiveFflogsRankedCandidate {
            content_id,
            priority,
            fetched_at: Some(fflogs_fetched_at),
            slot_index,
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
            .then_with(|| a.slot_index.cmp(&b.slot_index))
    });

    ActiveFflogsRankedCandidates {
        candidates,
        hidden_refresh_candidates,
    }
}

#[cfg(test)]
pub(super) fn active_fflogs_fetch_candidates(
    member_ids: Vec<u64>,
    cached_zones: &HashMap<u64, crate::mongo::ZoneCache>,
    hidden_ttl_hours: i64,
    active_refresh_minutes: i64,
    now: DateTime<Utc>,
) -> ActiveFflogsFetchCandidates {
    let member_slots = member_ids.into_iter().enumerate().collect::<Vec<_>>();
    let ranked = active_fflogs_ranked_candidates(
        &member_slots,
        cached_zones,
        hidden_ttl_hours,
        active_refresh_minutes,
        now,
    );

    ActiveFflogsFetchCandidates {
        content_ids: ranked
            .candidates
            .into_iter()
            .map(|candidate| candidate.content_id)
            .collect(),
        hidden_refresh_candidates: ranked.hidden_refresh_candidates,
    }
}

pub(super) fn try_build_fflogs_job_from_player(
    player: &crate::player::Player,
    worker_region: Option<FflogsWorkerRegion>,
    listing_anchor_world: u16,
    fflogs_info: &crate::fflogs::FFLogsEncounter,
    difficulty_id: i32,
) -> Option<ParseJob> {
    let (server, region) = resolve_exact_player_home_server_and_region(player)?;

    if !should_dispatch_for_worker_region(worker_region, listing_anchor_world, &region) {
        return None;
    }

    Some(build_parse_job(
        player,
        server,
        region,
        fflogs_info,
        difficulty_id,
    ))
}

#[cfg(test)]
pub(super) struct FflogsJobAppendContext<'a> {
    pub(super) limit: usize,
    pub(super) worker_region: Option<FflogsWorkerRegion>,
    pub(super) listing_anchor_world: u16,
    pub(super) fflogs_info: &'a crate::fflogs::FFLogsEncounter,
    pub(super) difficulty_id: i32,
}

#[cfg(test)]
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

        let Some(player) = players_by_content_id.get(content_id) else {
            continue;
        };

        if let Some(job) = try_build_fflogs_job_from_player(
            player,
            context.worker_region,
            context.listing_anchor_world,
            context.fflogs_info,
            context.difficulty_id,
        ) {
            jobs.push(job);
        }
    }
}
