use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;
use mongodb::{bson::doc, Collection};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
};
use uuid::Uuid;
use warp::{http::HeaderMap, Reply};

use super::ingest_guard::{CapabilityScope, GuardContext, IngestEndpoint};
use super::parse_ingest_common::{
    authorize_jobs_request, build_pending_failure_state, canonical_world_name_and_region,
    first_valid_listing_world, lease_matches, parse_processor_kind, parse_worker_region,
    payload_too_large_response, release_committed_leases, release_valid_abandoned_leases,
    should_dispatch_for_worker_region, snapshot_active_leases, upsert_failure_state_record,
    zone_cache_from_fields, AbandonMetadata, AcceptedAbandonedLease, FailureTarget,
    JobFailureStateDoc, ParseJob, ParseLeaseAbandonResponse, ParseResultFields,
    ParseResultsResponse, PendingFailureState, WorkerRegion, FAILURE_RETRY_COOLDOWN,
    FETCH_FAILED_REASON, RATE_LIMIT_DEFERRED_REASON, TOMESTONE_CHALLENGE_COOLDOWN_REASON,
    TOMESTONE_NO_DATA_REASON, TOMESTONE_RATE_LIMITED_REASON,
};
use super::{State, TomestoneLeaseEntry, TomestoneLeaseKey, TOMESTONE_LEASE_TTL_MINUTES};
use crate::listing_container::QueriedListing;
use crate::mongo::{
    get_current_listings, get_parse_docs, get_players_by_content_ids, ParseCacheDoc,
    ParseCacheProvider, ZoneCache,
};

#[cfg(test)]
pub(super) const TOMESTONE_JOB_MODE_HEADER: &str = "x-rpf-tomestone-job-mode";
pub(super) const TOMESTONE_PROCESSOR_KIND_HEADER: &str = "x-rpf-tomestone-processor-kind";
pub(super) const TOMESTONE_WORKER_REGION_HEADER: &str = "x-rpf-tomestone-worker-region";
pub(super) const DEFAULT_TOMESTONE_PROCESSOR_KIND: &str = "docker_tomestone_worker";

pub type TomestoneJob = ParseJob;

#[derive(Debug, serde::Deserialize)]
pub struct TomestoneResult {
    pub content_id: u64,
    pub zone_id: u32,
    #[serde(default)]
    pub difficulty_id: i32,
    #[serde(default)]
    pub encounter_id: u32,
    #[serde(default)]
    pub encounters: HashMap<i32, f64>,
    #[serde(default)]
    pub boss_percentages: HashMap<i32, f64>,
    #[serde(default)]
    pub clear_counts: HashMap<i32, i32>,
    #[serde(default)]
    pub phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
    #[serde(default)]
    pub tomestone_queried_encounters: Option<Vec<i32>>,
    /// Kept for Tomestone wire payload parity; server enforces `hidden = false` and `estimated = false`.
    #[allow(dead_code)]
    #[serde(default)]
    pub is_hidden: bool,
    /// Kept for Tomestone wire payload parity; server enforces `hidden = false` and `estimated = false`.
    #[allow(dead_code)]
    #[serde(default)]
    pub is_estimated: bool,
    #[serde(default)]
    pub matched_server: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub lease_token: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct AbandonTomestoneLease {
    pub content_id: u64,
    pub zone_id: u32,
    #[serde(default)]
    pub difficulty_id: i32,
    #[serde(default)]
    pub encounter_id: u32,
    #[serde(default)]
    pub lease_token: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub processor_kind: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub failure_code: Option<String>,
    #[serde(default)]
    pub failure_fingerprint: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

pub(super) fn normalized_tomestone_lease_key(
    content_id: u64,
    zone_id: u32,
    difficulty_id: i32,
    encounter_id: u32,
) -> TomestoneLeaseKey {
    TomestoneLeaseKey {
        content_id,
        zone_id,
        difficulty_id,
        encounter_id,
    }
}

pub(super) fn should_record_tomestone_failure_state(reason: &str) -> bool {
    !reason.eq_ignore_ascii_case(RATE_LIMIT_DEFERRED_REASON)
        && !reason.eq_ignore_ascii_case(TOMESTONE_RATE_LIMITED_REASON)
        && !reason.eq_ignore_ascii_case(TOMESTONE_CHALLENGE_COOLDOWN_REASON)
}

pub(super) fn tomestone_failure_state_becomes_final(
    reason: &str,
    failure_count: i64,
    threshold: u32,
) -> bool {
    if !should_record_tomestone_failure_state(reason) {
        return false;
    }
    if reason.eq_ignore_ascii_case(TOMESTONE_NO_DATA_REASON) {
        return failure_count >= threshold.max(1) as i64;
    }
    reason.eq_ignore_ascii_case(FETCH_FAILED_REASON) || failure_count >= threshold.max(1) as i64
}

fn tomestone_failure_state_id(
    key: &TomestoneLeaseKey,
    processor_kind: &str,
    failure_fingerprint: &str,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        key.content_id,
        key.zone_id,
        key.difficulty_id,
        key.encounter_id,
        processor_kind,
        failure_fingerprint
    )
}

type PendingTomestoneFailureState = PendingFailureState<TomestoneLeaseKey>;

struct AbandonedTomestoneLeaseValidation {
    accepted: Vec<AcceptedAbandonedLease<TomestoneLeaseKey>>,
    rejected_total: usize,
    reasons: HashSet<String>,
    failure_states: Vec<PendingTomestoneFailureState>,
}

fn validate_abandoned_tomestone_leases(
    leases: &HashMap<TomestoneLeaseKey, TomestoneLeaseEntry>,
    abandoned_leases: Vec<AbandonTomestoneLease>,
    client_id: &str,
    request_processor_kind: &str,
) -> AbandonedTomestoneLeaseValidation {
    let mut accepted = Vec::new();
    let mut rejected_total = 0usize;
    let mut seen = HashSet::new();
    let mut reasons = HashSet::new();
    let mut failure_states = Vec::new();

    for abandoned in abandoned_leases {
        if let Some(reason) = abandoned.reason.as_deref() {
            let reason = reason.trim();
            if !reason.is_empty() {
                reasons.insert(reason.to_string());
            }
        }

        let key = normalized_tomestone_lease_key(
            abandoned.content_id,
            abandoned.zone_id,
            abandoned.difficulty_id,
            abandoned.encounter_id,
        );
        let dedup_key = format!(
            "{}:{}:{}:{}:{}",
            key.content_id, key.zone_id, key.difficulty_id, key.encounter_id, abandoned.lease_token
        );
        if !seen.insert(dedup_key) {
            continue;
        }

        if !lease_matches(leases.get(&key), client_id, &abandoned.lease_token) {
            rejected_total += 1;
            continue;
        }

        if let Some(failure_state) = build_pending_failure_state(
            key.clone(),
            FailureTarget {
                content_id: key.content_id,
                zone_id: key.zone_id,
                difficulty_id: key.difficulty_id,
                encounter_id: Some(key.encounter_id),
            },
            AbandonMetadata {
                reason: abandoned.reason.as_deref(),
                processor_kind: abandoned.processor_kind.as_deref(),
                source_kind: abandoned.source_kind.as_deref(),
                failure_code: abandoned.failure_code.as_deref(),
                failure_fingerprint: abandoned.failure_fingerprint.as_deref(),
                message: abandoned.message.as_deref(),
            },
            request_processor_kind,
            should_record_tomestone_failure_state,
        ) {
            failure_states.push(failure_state);
        }

        accepted.push(AcceptedAbandonedLease {
            key,
            client_id: client_id.to_string(),
            lease_token: abandoned.lease_token,
        });
    }

    AbandonedTomestoneLeaseValidation {
        accepted,
        rejected_total,
        reasons,
        failure_states,
    }
}

async fn get_final_tomestone_failure_keys(
    collection: Collection<mongodb::bson::Document>,
    content_ids: &[u64],
    processor_kind: &str,
) -> anyhow::Result<HashSet<(u64, u32, i32, u32)>> {
    if content_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let ids = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let now = Utc::now();
    let mut cursor = collection
        .clone_with_type::<JobFailureStateDoc>()
        .find(
            doc! {
                "content_id": { "$in": ids },
                "processor_kind": processor_kind,
                "final_for_processor": true,
                "last_failed_at": { "$gt": now - FAILURE_RETRY_COOLDOWN },
            },
            None,
        )
        .await?;

    let mut final_keys = HashSet::new();
    while let Some(doc) = cursor.next().await.transpose()? {
        if doc.content_id >= 0
            && doc.zone_id >= 0
            && doc.last_failed_at > now - FAILURE_RETRY_COOLDOWN
            && doc.final_for_processor
        {
            if let Some(encounter_id) = doc.encounter_id.filter(|id| *id >= 0) {
                final_keys.insert((
                    doc.content_id as u64,
                    doc.zone_id as u32,
                    doc.difficulty_id,
                    encounter_id as u32,
                ));
            }
        }
    }

    Ok(final_keys)
}

async fn record_tomestone_failure_state(
    collection: Collection<mongodb::bson::Document>,
    pending: PendingTomestoneFailureState,
    threshold: u32,
) -> anyhow::Result<()> {
    let id = tomestone_failure_state_id(
        &pending.key,
        &pending.processor_kind,
        &pending.failure_fingerprint,
    );
    upsert_failure_state_record(
        collection,
        &id,
        &pending,
        threshold,
        None,
        tomestone_failure_state_becomes_final,
    )
    .await
}

async fn clear_tomestone_failure_state(
    collection: Collection<mongodb::bson::Document>,
    content_id: u64,
    zone_id: u32,
    difficulty_id: i32,
    encounter_id: u32,
) -> anyhow::Result<()> {
    collection
        .delete_many(
            doc! {
                "content_id": content_id as i64,
                "zone_id": zone_id as i32,
                "difficulty_id": difficulty_id,
                "encounter_id": encounter_id as i32,
            },
            None,
        )
        .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ActiveTomestoneFetchPriority {
    MissingCache,
    RefreshDue,
}

#[derive(Debug)]
struct ActiveTomestoneCandidate {
    content_id: u64,
    priority: ActiveTomestoneFetchPriority,
    fetched_at: Option<DateTime<Utc>>,
    original_index: usize,
}

fn is_tomestone_encounter_due(
    zone_cache: Option<&ZoneCache>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
    active_refresh_minutes: i64,
    now: DateTime<Utc>,
) -> (bool, Option<DateTime<Utc>>) {
    let Some(cache) = zone_cache else {
        return (true, None);
    };

    if cache.hidden {
        return (false, None);
    }

    let target_encounter_id = secondary_encounter_id.unwrap_or(encounter_id);
    if cache
        .encounters
        .get(&target_encounter_id.to_string())
        .is_some_and(|enc| enc.percentile > 0.0 || enc.clear_count.unwrap_or(0) > 0)
    {
        return (false, None);
    }

    let Some(tomestone_fetched_at) =
        cache.effective_tomestone_fetched_at(encounter_id, secondary_encounter_id)
    else {
        return (true, None);
    };

    let refresh_threshold = now - TimeDelta::try_minutes(active_refresh_minutes.max(1)).unwrap();
    (
        tomestone_fetched_at <= refresh_threshold,
        Some(tomestone_fetched_at),
    )
}

fn resolve_tomestone_player_server_and_region(
    player: &crate::player::Player,
    parse_doc: Option<&ParseCacheDoc>,
    zone_key: &str,
) -> Option<(String, String)> {
    if player.home_world != 0
        && crate::ffxiv::WORLDS
            .get(&(player.home_world as u32))
            .is_some()
    {
        let server = player.home_world_name().to_string();
        let region = crate::fflogs::get_region_from_server(&server).to_string();
        return Some((server, region));
    }

    let doc = parse_doc?;
    if let Some(current_zone) = crate::fflogs::get_compatible_zone(&doc.zones, zone_key) {
        if current_zone.hidden {
            return None;
        }
        if let Some(matched) = current_zone
            .matched_server
            .as_deref()
            .and_then(canonical_world_name_and_region)
        {
            return Some(matched);
        }
    }

    let mut fallback_zones = doc
        .zones
        .values()
        .filter(|zc| !zc.hidden && zc.matched_server.is_some())
        .collect::<Vec<_>>();
    fallback_zones.sort_by(|a, b| b.fetched_at.cmp(&a.fetched_at));
    fallback_zones
        .into_iter()
        .find_map(|zc| {
            zc.matched_server
                .as_deref()
                .and_then(canonical_world_name_and_region)
        })
}

struct TomestoneListingBatch {
    fflogs_info: &'static crate::fflogs::FFLogsEncounter,
    difficulty_id: i32,
    zone_key: String,
    listing_anchor_world: u16,
    member_ids: Vec<u64>,
}

struct PreparedTomestoneListing {
    fflogs_info: &'static crate::fflogs::FFLogsEncounter,
    difficulty_id: i32,
    zone_key: String,
    listing_anchor_world: u16,
    needing_fetch: Vec<u64>,
}

fn prepare_tomestone_listing_batches(
    listings: &[QueriedListing],
    active_leases: &HashSet<TomestoneLeaseKey>,
) -> (Vec<TomestoneListingBatch>, Vec<u64>) {
    let mut batches = Vec::new();
    let mut all_member_ids = Vec::new();
    let mut seen_member_ids = HashSet::new();

    for container in listings {
        if !container.listing.high_end() {
            continue;
        }

        let Some(fflogs_info) = crate::fflogs::mapping::get_fflogs_encounter(container.listing.duty)
        else {
            continue;
        };

        let difficulty_id = fflogs_info.difficulty_id.unwrap_or(0) as i32;
        let zone_key = crate::fflogs::make_zone_cache_key(fflogs_info.zone_id, difficulty_id);
        let member_ids: Vec<u64> = container
            .listing
            .member_content_ids
            .iter()
            .map(|&id| id as u64)
            .filter(|&id| {
                id != 0
                    && !active_leases.contains(&normalized_tomestone_lease_key(
                        id,
                        fflogs_info.zone_id,
                        difficulty_id,
                        fflogs_info.encounter_id,
                    ))
            })
            .collect();

        if member_ids.is_empty() {
            continue;
        }

        for &member_id in &member_ids {
            if seen_member_ids.insert(member_id) {
                all_member_ids.push(member_id);
            }
        }

        batches.push(TomestoneListingBatch {
            fflogs_info,
            difficulty_id,
            zone_key,
            listing_anchor_world: first_valid_listing_world(&container.listing),
            member_ids,
        });
    }

    (batches, all_member_ids)
}

fn prepare_tomestone_listings_needing_fetch(
    batches: Vec<TomestoneListingBatch>,
    parse_docs: &HashMap<u64, ParseCacheDoc>,
    final_failure_keys: &HashSet<(u64, u32, i32, u32)>,
    active_leases: &HashSet<TomestoneLeaseKey>,
    active_refresh_minutes: i64,
    now: DateTime<Utc>,
) -> (Vec<PreparedTomestoneListing>, Vec<u64>) {
    let mut prepared = Vec::with_capacity(batches.len());
    let mut all_needing_player_ids = Vec::new();
    let mut seen_player_ids = HashSet::new();
    let mut seen_candidate_keys = HashSet::new();

    for batch in batches {
        let mut candidates = Vec::new();
        for (original_index, content_id) in batch.member_ids.into_iter().enumerate() {
            let zone_cache = parse_docs
                .get(&content_id)
                .and_then(|doc| crate::fflogs::get_compatible_zone(&doc.zones, &batch.zone_key));
            let (is_due, fetched_at) = is_tomestone_encounter_due(
                zone_cache,
                batch.fflogs_info.encounter_id,
                batch.fflogs_info.secondary_encounter_id,
                active_refresh_minutes,
                now,
            );
            if !is_due {
                continue;
            }

            let priority = if fetched_at.is_none() {
                ActiveTomestoneFetchPriority::MissingCache
            } else {
                ActiveTomestoneFetchPriority::RefreshDue
            };
            candidates.push(ActiveTomestoneCandidate {
                content_id,
                priority,
                fetched_at,
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

        let mut needing_fetch: Vec<u64> = candidates.into_iter().map(|c| c.content_id).collect();
        needing_fetch.retain(|content_id| {
            let failure_key = (
                *content_id,
                batch.fflogs_info.zone_id,
                batch.difficulty_id,
                batch.fflogs_info.encounter_id,
            );
            let lease_key = normalized_tomestone_lease_key(
                *content_id,
                batch.fflogs_info.zone_id,
                batch.difficulty_id,
                batch.fflogs_info.encounter_id,
            );
            !final_failure_keys.contains(&failure_key)
                && !active_leases.contains(&lease_key)
                && seen_candidate_keys.insert(lease_key)
        });

        if needing_fetch.is_empty() {
            continue;
        }

        for &content_id in &needing_fetch {
            if seen_player_ids.insert(content_id) {
                all_needing_player_ids.push(content_id);
            }
        }

        prepared.push(PreparedTomestoneListing {
            fflogs_info: batch.fflogs_info,
            difficulty_id: batch.difficulty_id,
            zone_key: batch.zone_key,
            listing_anchor_world: batch.listing_anchor_world,
            needing_fetch,
        });
    }

    (prepared, all_needing_player_ids)
}

fn assemble_tomestone_jobs(
    prepared: Vec<PreparedTomestoneListing>,
    players: Vec<crate::player::Player>,
    parse_docs: &HashMap<u64, ParseCacheDoc>,
    limit: usize,
    worker_region: Option<WorkerRegion>,
) -> Vec<TomestoneJob> {
    let mut players_by_content_id = HashMap::new();
    for player in players {
        players_by_content_id
            .entry(player.content_id)
            .or_insert(player);
    }

    let mut jobs = Vec::new();
    for item in prepared {
        if jobs.len() >= limit {
            break;
        }

        for content_id in item.needing_fetch {
            if jobs.len() >= limit {
                break;
            }

            let Some(player) = players_by_content_id.get(&content_id) else {
                continue;
            };
            let Some((server, region)) = resolve_tomestone_player_server_and_region(
                player,
                parse_docs.get(&content_id),
                &item.zone_key,
            ) else {
                continue;
            };

            if !should_dispatch_for_worker_region(
                worker_region,
                item.listing_anchor_world,
                &region,
            ) {
                continue;
            }

            jobs.push(TomestoneJob {
                content_id: player.content_id,
                name: player.name.clone(),
                server,
                region,
                candidate_servers: Vec::new(),
                zone_id: item.fflogs_info.zone_id,
                difficulty_id: item.difficulty_id,
                encounter_id: item.fflogs_info.encounter_id,
                secondary_encounter_id: item.fflogs_info.secondary_encounter_id,
                lease_token: String::new(),
            });
        }
    }

    jobs
}

pub async fn contribute_tomestone_jobs_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match authorize_jobs_request(
        &state,
        IngestEndpoint::ContributeTomestoneJobs,
        CapabilityScope::TomestoneJobs,
        &headers,
        remote_addr,
        "/contribute/tomestone/jobs",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(response) => return Ok(response),
    };

    let Some(lease_ttl) = TimeDelta::try_minutes(TOMESTONE_LEASE_TTL_MINUTES) else {
        return Ok(warp::reply::json(&Vec::<TomestoneJob>::new()).into_response());
    };

    let processor_kind = parse_processor_kind(
        &headers,
        TOMESTONE_PROCESSOR_KIND_HEADER,
        DEFAULT_TOMESTONE_PROCESSOR_KIND,
    );
    let worker_region = parse_worker_region(&headers, TOMESTONE_WORKER_REGION_HEADER);
    let limit = state.tomestone_jobs_limit;
    let now = Utc::now();

    let listings = match get_current_listings(state.collection()).await {
        Ok(l) => l,
        Err(_) => return Ok(warp::reply::json(&Vec::<TomestoneJob>::new()).into_response()),
    };

    let active_leases = {
        let leases = state.tomestone_job_leases.read().await;
        snapshot_active_leases(&leases, lease_ttl, now)
    };

    let (batches, all_member_ids) = prepare_tomestone_listing_batches(&listings, &active_leases);
    if all_member_ids.is_empty() {
        return Ok(warp::reply::json(&Vec::<TomestoneJob>::new()).into_response());
    }

    let (parse_docs_res, failure_keys_res) = tokio::join!(
        get_parse_docs(state.parse_collection(), &all_member_ids),
        get_final_tomestone_failure_keys(
            state.tomestone_job_failure_collection(),
            &all_member_ids,
            &processor_kind,
        ),
    );
    let parse_docs = match parse_docs_res {
        Ok(docs) => docs,
        Err(error) => {
            tracing::warn!(error = ?error, "failed to batch load parse cache docs for Tomestone jobs");
            return Ok(warp::reply::json(&Vec::<TomestoneJob>::new()).into_response());
        }
    };
    let final_failure_keys = match failure_keys_res {
        Ok(keys) => keys,
        Err(error) => {
            tracing::warn!(
                error = ?error,
                processor_kind,
                "failed to batch load Tomestone job failure states"
            );
            HashSet::new()
        }
    };

    let (prepared, all_needing_player_ids) = prepare_tomestone_listings_needing_fetch(
        batches,
        &parse_docs,
        &final_failure_keys,
        &active_leases,
        state.tomestone_active_refresh_minutes,
        now,
    );

    let mut jobs = if all_needing_player_ids.is_empty() {
        Vec::new()
    } else {
        match get_players_by_content_ids(state.players_collection(), &all_needing_player_ids).await
        {
            Ok(players) => {
                assemble_tomestone_jobs(prepared, players, &parse_docs, limit, worker_region)
            }
            Err(error) => {
                tracing::warn!(error = ?error, "failed to batch load players for Tomestone jobs");
                Vec::new()
            }
        }
    };

    let lease_now = Utc::now();
    {
        let mut leases = state.tomestone_job_leases.write().await;
        leases.retain(|_, lease| (lease.leased_at + lease_ttl) > lease_now);

        let mut seen = HashSet::new();
        let mut leased_jobs = Vec::with_capacity(jobs.len());

        for mut job in jobs {
            let key = normalized_tomestone_lease_key(
                job.content_id,
                job.zone_id,
                job.difficulty_id,
                job.encounter_id,
            );
            if !seen.insert(key.clone()) || leases.contains_key(&key) {
                continue;
            }

            let lease_token = Uuid::new_v4().to_string();
            leases.insert(
                key,
                TomestoneLeaseEntry {
                    leased_at: lease_now,
                    client_id: guard_ctx.client_id.clone(),
                    lease_token: lease_token.clone(),
                    secondary_encounter_id: job.secondary_encounter_id,
                },
            );
            job.lease_token = lease_token;
            leased_jobs.push(job);
        }

        jobs = leased_jobs;
    }

    let dispatched_in_batch = jobs.len() as u64;
    if dispatched_in_batch > 0 {
        let dispatched_total = state
            .tomestone_jobs_dispatched_total
            .fetch_add(dispatched_in_batch, AtomicOrdering::Relaxed)
            + dispatched_in_batch;
        tracing::debug!(
            batch_jobs = dispatched_in_batch,
            total_jobs_dispatched = dispatched_total,
            "tomestone jobs dispatched",
        );
    }

    Ok(warp::reply::json(&jobs).into_response())
}

fn effective_tomestone_progress_coverage(
    lease_key: &TomestoneLeaseKey,
    secondary_encounter_id: Option<u32>,
    res: &TomestoneResult,
) -> Vec<i32> {
    if let Some(ref coverage) = res.tomestone_queried_encounters {
        return coverage.clone();
    }

    if res.encounters.is_empty()
        && res.clear_counts.is_empty()
        && res.boss_percentages.is_empty()
        && res.phase_progress.is_empty()
    {
        let mut coverage = vec![lease_key.encounter_id as i32];
        if let Some(sec_id) = secondary_encounter_id {
            coverage.push(sec_id as i32);
        }
        coverage
    } else {
        Vec::new()
    }
}

pub(crate) async fn contribute_tomestone_results_handler_with_guard(
    state: Arc<State>,
    guard_ctx: GuardContext,
    results: Vec<TomestoneResult>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let submitted_results_total = results.len();
    if submitted_results_total > state.max_tomestone_results_batch_size {
        return Ok(payload_too_large_response(
            "too many Tomestone results in request",
        ));
    }

    let mut accepted_results = Vec::with_capacity(submitted_results_total);
    {
        let leases = state.tomestone_job_leases.read().await;
        for res in results {
            let key = normalized_tomestone_lease_key(
                res.content_id,
                res.zone_id,
                res.difficulty_id,
                res.encounter_id,
            );
            let Some(lease) = leases.get(&key) else {
                continue;
            };
            if !lease_matches(Some(lease), &guard_ctx.client_id, &res.lease_token) {
                continue;
            }
            accepted_results.push((key, lease.secondary_encounter_id, res));
        }
    }

    let accepted_results_total = accepted_results.len();
    let mut success_count = 0usize;
    let mut committed_leases = Vec::with_capacity(accepted_results_total);

    for (lease_key, secondary_encounter_id, res) in accepted_results {
        let zone_key = crate::fflogs::make_zone_cache_key(res.zone_id, res.difficulty_id);
        let content_id = res.content_id;
        let progress_coverage =
            effective_tomestone_progress_coverage(&lease_key, secondary_encounter_id, &res);
        let lease_token = res.lease_token;
        let zone_cache = zone_cache_from_fields(
            ParseResultFields {
                encounters: res.encounters,
                boss_percentages: res.boss_percentages,
                clear_counts: res.clear_counts,
                phase_progress: res.phase_progress,
                is_hidden: false,
                is_estimated: false,
                matched_server: res.matched_server,
                source_kind: res.source_kind,
            },
            Utc::now(),
        );

        match crate::mongo::upsert_zone_cache(
            state.parse_collection(),
            content_id,
            &zone_key,
            &zone_cache,
            Some(&progress_coverage),
            ParseCacheProvider::Tomestone,
        )
        .await
        {
            Ok(()) => {
                success_count += 1;
                if let Err(error) = clear_tomestone_failure_state(
                    state.tomestone_job_failure_collection(),
                    lease_key.content_id,
                    lease_key.zone_id,
                    lease_key.difficulty_id,
                    lease_key.encounter_id,
                )
                .await
                {
                    tracing::warn!(
                        content_id = lease_key.content_id,
                        zone_id = lease_key.zone_id,
                        difficulty_id = lease_key.difficulty_id,
                        encounter_id = lease_key.encounter_id,
                        error = ?error,
                        "failed to clear Tomestone job failure state after commit"
                    );
                }
                committed_leases.push((lease_key, lease_token));
            }
            Err(error) => {
                tracing::warn!(
                    content_id,
                    zone_key,
                    error = ?error,
                    "failed to persist Tomestone zone cache result"
                );
            }
        }
    }

    if !committed_leases.is_empty() {
        let mut leases = state.tomestone_job_leases.write().await;
        release_committed_leases(&mut leases, &committed_leases, &guard_ctx.client_id);
    }

    if accepted_results_total > 0 {
        let results_total = state
            .tomestone_results_received_total
            .fetch_add(accepted_results_total as u64, AtomicOrdering::Relaxed)
            + accepted_results_total as u64;
        tracing::debug!(
            accepted_results_total,
            submitted_results_total,
            success_count,
            results_total,
            "tomestone results processed",
        );
    }
    if success_count > 0 {
        state.notify_listings_changed(success_count);
    }

    let rejected_results_total = submitted_results_total.saturating_sub(accepted_results_total);
    if rejected_results_total > 0 {
        state
            .tomestone_results_rejected_total
            .fetch_add(rejected_results_total as u64, AtomicOrdering::Relaxed);
    }

    let status = if rejected_results_total == 0 {
        "ok"
    } else {
        "partial"
    };

    Ok(warp::reply::json(&ParseResultsResponse {
        status,
        submitted: submitted_results_total,
        accepted: accepted_results_total,
        updated: success_count,
        rejected: rejected_results_total,
    })
    .into_response())
}

pub(crate) async fn contribute_tomestone_leases_abandon_handler_with_guard(
    state: Arc<State>,
    headers: HeaderMap,
    guard_ctx: GuardContext,
    abandoned_leases: Vec<AbandonTomestoneLease>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let submitted_total = abandoned_leases.len();
    if submitted_total > state.max_tomestone_results_batch_size {
        return Ok(payload_too_large_response(
            "too many Tomestone lease abandons in request",
        ));
    }

    let request_processor_kind = parse_processor_kind(
        &headers,
        TOMESTONE_PROCESSOR_KIND_HEADER,
        DEFAULT_TOMESTONE_PROCESSOR_KIND,
    );
    let validation = {
        let leases = state.tomestone_job_leases.read().await;
        validate_abandoned_tomestone_leases(
            &leases,
            abandoned_leases,
            &guard_ctx.client_id,
            &request_processor_kind,
        )
    };

    for failure_state in validation.failure_states {
        let processor_kind = failure_state.processor_kind.clone();
        let content_id = failure_state.key.content_id;
        let zone_id = failure_state.key.zone_id;
        let encounter_id = failure_state.key.encounter_id;
        let reason = failure_state.reason.clone();
        let failure_code = failure_state.failure_code.clone();
        if let Err(error) = record_tomestone_failure_state(
            state.tomestone_job_failure_collection(),
            failure_state,
            state.tomestone_non_fetch_failure_abandon_threshold,
        )
        .await
        {
            tracing::warn!(
                error = ?error,
                processor_kind,
                content_id,
                zone_id,
                encounter_id,
                reason,
                failure_code,
                "failed to record Tomestone job failure state"
            );
        }
    }

    let released_total = {
        let mut leases = state.tomestone_job_leases.write().await;
        release_valid_abandoned_leases(&mut leases, &validation.accepted)
    };
    let rejected_total =
        validation.rejected_total + validation.accepted.len().saturating_sub(released_total);
    let reasons = validation.reasons;

    if released_total > 0 {
        state
            .tomestone_leases_abandoned_total
            .fetch_add(released_total as u64, AtomicOrdering::Relaxed);
        tracing::debug!(
            client_id = guard_ctx.client_id,
            submitted_total,
            released_total,
            rejected_total,
            reasons = ?reasons,
            "tomestone lease abandon processed"
        );
    }

    if rejected_total > 0 {
        state
            .tomestone_leases_abandon_rejected_total
            .fetch_add(rejected_total as u64, AtomicOrdering::Relaxed);
    }

    let status = if rejected_total == 0 { "ok" } else { "partial" };
    Ok(warp::reply::json(&ParseLeaseAbandonResponse {
        status,
        submitted: submitted_total,
        released: released_total,
        rejected: rejected_total,
    })
    .into_response())
}

#[cfg(test)]
#[path = "tomestone_ingest/tests.rs"]
mod tests;
