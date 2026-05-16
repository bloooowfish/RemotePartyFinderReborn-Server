use chrono::{DateTime, Utc};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use super::{
    payloads::{ParseJob, ParseJobCandidateServer},
    request::FflogsWorkerRegion,
};

pub(super) fn build_candidate_servers(anchor_world_id: u16) -> Vec<ParseJobCandidateServer> {
    let mut world_ids: Vec<u32> = crate::ffxiv::WORLDS.keys().copied().collect();
    world_ids.sort_unstable();

    let (anchor_dc, anchor_region) = match crate::ffxiv::WORLDS.get(&(anchor_world_id as u32)) {
        Some(w) => (
            Some(w.data_center()),
            crate::fflogs::get_region_from_server(w.as_str()),
        ),
        None => (None, "NA"),
    };

    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    if let Some(dc) = anchor_dc {
        for wid in &world_ids {
            let Some(w) = crate::ffxiv::WORLDS.get(wid) else {
                continue;
            };
            if w.data_center() != dc {
                continue;
            }
            let server = w.as_str().to_string();
            if !seen.insert(server.clone()) {
                continue;
            }
            out.push(ParseJobCandidateServer {
                region: crate::fflogs::get_region_from_server(&server).to_string(),
                server,
            });
        }
    }

    for wid in &world_ids {
        let Some(w) = crate::ffxiv::WORLDS.get(wid) else {
            continue;
        };
        let server = w.as_str().to_string();
        let region = crate::fflogs::get_region_from_server(&server);
        if region != anchor_region {
            continue;
        }
        if !seen.insert(server.clone()) {
            continue;
        }
        out.push(ParseJobCandidateServer {
            region: region.to_string(),
            server,
        });
    }

    out
}

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

        if !crate::fflogs::cache::is_zone_cache_refresh_due_for_active_listing(
            cache,
            hidden_ttl_hours,
            active_refresh_minutes,
            now,
        ) {
            continue;
        }

        let priority = if cache.hidden {
            hidden_refresh_candidates += 1;
            ActiveFflogsFetchPriority::HiddenRefreshDue
        } else {
            ActiveFflogsFetchPriority::VisibleRefreshDue
        };

        candidates.push(ActiveFflogsFetchCandidate {
            content_id,
            priority,
            fetched_at: Some(cache.fetched_at),
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

fn region_for_world_id(world_id: u16) -> Option<&'static str> {
    crate::ffxiv::WORLDS
        .get(&(world_id as u32))
        .map(|world| crate::fflogs::get_region_from_server(world.as_str()))
}

fn worker_region_matches_region(worker_region: FflogsWorkerRegion, region: &str) -> bool {
    worker_region.as_str().eq_ignore_ascii_case(region)
}

fn should_dispatch_for_worker_region(
    worker_region: Option<FflogsWorkerRegion>,
    listing_anchor_world: u16,
    resolved_job_region: &str,
) -> bool {
    let Some(worker_region) = worker_region else {
        return true;
    };

    let Some(anchor_region) = region_for_world_id(listing_anchor_world) else {
        return false;
    };

    if worker_region == FflogsWorkerRegion::Oc {
        return anchor_region == "OC";
    }

    worker_region_matches_region(worker_region, anchor_region)
        && worker_region_matches_region(worker_region, resolved_job_region)
}

pub(super) struct FflogsJobAppendContext<'a> {
    pub(super) limit: usize,
    pub(super) exact_home_world_only: bool,
    pub(super) worker_region: Option<FflogsWorkerRegion>,
    pub(super) listing_anchor_world: u16,
    pub(super) listing_candidates: &'a [ParseJobCandidateServer],
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

        let home_world_known = player.home_world != 0
            && crate::ffxiv::WORLDS
                .get(&(player.home_world as u32))
                .is_some();

        if !home_world_known && context.exact_home_world_only {
            continue;
        }

        let (server, region, candidate_servers) = if home_world_known {
            let server = player.home_world_name().to_string();
            let region = crate::fflogs::get_region_from_server(&server).to_string();
            (server, region, Vec::new())
        } else {
            let anchor_world = if player.current_world != 0
                && crate::ffxiv::WORLDS
                    .get(&(player.current_world as u32))
                    .is_some()
            {
                player.current_world
            } else {
                context.listing_anchor_world
            };

            let candidates = if anchor_world != 0 {
                build_candidate_servers(anchor_world)
            } else {
                context.listing_candidates.to_vec()
            };

            if candidates.is_empty() {
                continue;
            }

            let server = candidates[0].server.clone();
            let region = candidates[0].region.clone();
            (server, region, candidates)
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
            candidate_servers,
            zone_id: context.fflogs_info.zone_id,
            difficulty_id: context.difficulty_id,
            encounter_id: context.fflogs_info.encounter_id,
            secondary_encounter_id: context.fflogs_info.secondary_encounter_id,
            lease_token: String::new(),
        });
    }
}
