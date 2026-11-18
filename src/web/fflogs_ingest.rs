use chrono::{DateTime, TimeDelta, Utc};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
};
use uuid::Uuid;
use warp::{
    http::HeaderMap,
    Reply,
};

use super::ingest_guard::{CapabilityScope, GuardContext, IngestEndpoint};
use super::parse_ingest_common::{
    authorize_jobs_request, count_active_client_leases, first_valid_listing_world,
    has_valid_parse_result_bounds, lease_matches, parse_results_response,
    payload_too_large_response, release_committed_leases, snapshot_active_leases,
    ParseLeaseAbandonResponse, ParseResultsResponse,
};
use super::{State, FFLOGS_LEASE_TTL_MINUTES};
use crate::listing_container::QueriedListing;
use crate::mongo::{
    get_current_listings, get_parse_docs, get_players_by_content_ids, resolve_fflogs_rank_coverage,
    ParseCacheDoc, ParseCacheProvider,
};

mod failure_state;
mod jobs;
mod payloads;
mod request;
mod results;

#[cfg(test)]
use failure_state::{
    failure_state_becomes_final, final_failure_state_blocks_fetch,
    legacy_tomestone_no_data_reset_filter, legacy_tomestone_no_data_reset_update,
    should_record_failure_state, PendingFflogsFailureState, FETCH_FAILED_REASON,
    TOMESTONE_NO_DATA_REASON, UPSTREAM_TRANSIENT_ERROR_REASON, WORKER_BATCH_LIMIT_OVERFLOW_REASON,
    WORKER_CYCLE_ABORTED_REASON, WORKER_SHUTDOWN_DEFERRED_REASON,
};
use failure_state::{
    clear_fflogs_failure_states_batch, fflogs_failure_state_clear_filter, get_final_failure_keys,
    record_failure_states_batch, release_valid_abandoned_leases, validate_abandoned_leases,
};
#[cfg(test)]
use jobs::{
    active_fflogs_fetch_candidates, append_fflogs_jobs_for_players, FflogsJobAppendContext,
};
use jobs::{
    active_fflogs_ranked_candidates, normalized_lease_key, try_build_fflogs_job_from_player,
    ActiveFflogsFetchPriority,
};
pub use payloads::{AbandonFflogsLease, ParseJob, ParseResult};
use request::{fflogs_jobs_limit, fflogs_processor_kind, fflogs_worker_region};
#[cfg(test)]
use request::{
    fflogs_exact_home_world_only, FflogsWorkerRegion, DEFAULT_FFLOGS_PROCESSOR_KIND,
    FFLOGS_JOBS_LIMIT_HEADER, FFLOGS_JOB_MODE_HEADER, FFLOGS_PROCESSOR_KIND_HEADER,
    FFLOGS_WORKER_REGION_HEADER, RATE_LIMIT_DEFERRED_REASON,
};
#[cfg(test)]
use results::build_fflogs_result_encounter_map;
use results::zone_cache_from_parse_result;

struct ListingJobBatch<'a> {
    listing: &'a crate::listing::PartyFinderListing,
    listing_updated_at: DateTime<Utc>,
    fflogs_info: &'static crate::fflogs::FFLogsEncounter,
    difficulty_id: i32,
    zone_key: String,
    member_slots: Vec<(usize, u64)>,
}

struct PreparedFflogsCandidate {
    content_id: u64,
    priority: ActiveFflogsFetchPriority,
    fetched_at: Option<DateTime<Utc>>,
    listing_updated_at: DateTime<Utc>,
    slot_index: usize,
    fflogs_info: &'static crate::fflogs::FFLogsEncounter,
    difficulty_id: i32,
    listing_anchor_world: u16,
}

struct JobSelectionInput<'a> {
    parse_docs: &'a HashMap<u64, ParseCacheDoc>,
    final_failure_keys: &'a HashSet<(u64, u32, i32)>,
    active_leases: &'a HashSet<super::FflogsLeaseKey>,
    hidden_cache_ttl_hours: i64,
    active_parse_refresh_minutes: i64,
    active_refresh_now: DateTime<Utc>,
}

fn prepare_listing_job_batches<'a>(
    listings: &'a [QueriedListing],
    active_leases: &HashSet<super::FflogsLeaseKey>,
) -> (Vec<ListingJobBatch<'a>>, Vec<u64>) {
    let mut batches = Vec::new();
    let mut all_member_ids = Vec::new();
    let mut seen_member_ids = HashSet::new();

    for container in listings {
        if !container.listing.high_end() {
            continue;
        }

        let Some(fflogs_info) = container.listing.fflogs_encounter() else {
            continue;
        };

        let difficulty_id = fflogs_info.difficulty_id.unwrap_or(0) as i32;
        let zone_key = crate::fflogs::make_zone_cache_key(fflogs_info.zone_id, difficulty_id);
        let member_slots: Vec<(usize, u64)> = container
            .listing
            .member_content_ids
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(slot_index, id)| {
                let id = id as u64;
                (id != 0
                    && !active_leases.contains(&normalized_lease_key(
                        id,
                        fflogs_info.zone_id,
                        difficulty_id,
                    )))
                .then_some((slot_index, id))
            })
            .collect();

        if member_slots.is_empty() {
            continue;
        }

        for &(_, member_id) in &member_slots {
            if seen_member_ids.insert(member_id) {
                all_member_ids.push(member_id);
            }
        }

        batches.push(ListingJobBatch {
            listing: &container.listing,
            listing_updated_at: container.updated_at,
            fflogs_info,
            difficulty_id,
            zone_key,
            member_slots,
        });
    }

    (batches, all_member_ids)
}

fn prepare_listings_needing_fetch(
    batches: Vec<ListingJobBatch<'_>>,
    input: &JobSelectionInput<'_>,
) -> (Vec<PreparedFflogsCandidate>, Vec<u64>, u64) {
    let mut raw_candidates = Vec::new();
    let mut hidden_refresh_candidates_in_batch = 0u64;

    for batch in batches {
        let mut cached_zones = HashMap::new();
        for &(_, member_id) in &batch.member_slots {
            if let Some(doc) = input.parse_docs.get(&member_id) {
                if let Some(zone_cache) =
                    crate::fflogs::get_compatible_zone(&doc.zones, &batch.zone_key)
                {
                    cached_zones.insert(member_id, zone_cache.clone());
                }
            }
        }

        let ranked = active_fflogs_ranked_candidates(
            &batch.member_slots,
            &cached_zones,
            input.hidden_cache_ttl_hours,
            input.active_parse_refresh_minutes,
            input.active_refresh_now,
        );
        hidden_refresh_candidates_in_batch += ranked.hidden_refresh_candidates;

        let listing_anchor_world = first_valid_listing_world(batch.listing);
        for candidate in ranked.candidates {
            let failure_key = (
                candidate.content_id,
                batch.fflogs_info.zone_id,
                batch.difficulty_id,
            );
            let lease_key = normalized_lease_key(
                candidate.content_id,
                batch.fflogs_info.zone_id,
                batch.difficulty_id,
            );
            if input.final_failure_keys.contains(&failure_key)
                || input.active_leases.contains(&lease_key)
            {
                continue;
            }

            raw_candidates.push(PreparedFflogsCandidate {
                content_id: candidate.content_id,
                priority: candidate.priority,
                fetched_at: candidate.fetched_at,
                listing_updated_at: batch.listing_updated_at,
                slot_index: candidate.slot_index,
                fflogs_info: batch.fflogs_info,
                difficulty_id: batch.difficulty_id,
                listing_anchor_world,
            });
        }
    }

    raw_candidates.sort_by_key(|candidate| {
        (
            candidate.priority,
            candidate.fetched_at,
            std::cmp::Reverse(candidate.listing_updated_at),
            candidate.slot_index,
        )
    });

    let mut prepared = Vec::with_capacity(raw_candidates.len());
    let mut all_needing_player_ids = Vec::new();
    let mut seen_player_ids = HashSet::new();
    let mut seen_candidate_keys = HashSet::new();

    for candidate in raw_candidates {
        let lease_key = normalized_lease_key(
            candidate.content_id,
            candidate.fflogs_info.zone_id,
            candidate.difficulty_id,
        );
        if !seen_candidate_keys.insert(lease_key) {
            continue;
        }
        if seen_player_ids.insert(candidate.content_id) {
            all_needing_player_ids.push(candidate.content_id);
        }
        prepared.push(candidate);
    }

    (
        prepared,
        all_needing_player_ids,
        hidden_refresh_candidates_in_batch,
    )
}

fn assemble_jobs_from_prepared(
    prepared: Vec<PreparedFflogsCandidate>,
    players: Vec<crate::player::Player>,
    limit: usize,
    worker_region: Option<request::FflogsWorkerRegion>,
) -> Vec<ParseJob> {
    let mut players_by_content_id = HashMap::with_capacity(players.len());
    for player in players {
        players_by_content_id
            .entry(player.content_id)
            .or_insert(player);
    }

    let mut jobs = Vec::with_capacity(limit.min(prepared.len()));
    for candidate in prepared {
        if jobs.len() >= limit {
            break;
        }

        let Some(player) = players_by_content_id.get(&candidate.content_id) else {
            continue;
        };

        if let Some(job) = try_build_fflogs_job_from_player(
            player,
            worker_region,
            candidate.listing_anchor_world,
            candidate.fflogs_info,
            candidate.difficulty_id,
        ) {
            jobs.push(job);
        }
    }
    jobs
}

/// 플러그인에게 파싱 작업 할당
pub async fn contribute_fflogs_jobs_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    path_and_query: String,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match authorize_jobs_request(
        &state,
        IngestEndpoint::ContributeFflogsJobs,
        CapabilityScope::FflogsJobs,
        &headers,
        remote_addr,
        &path_and_query,
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(response) => return Ok(response),
    };

    let Some(lease_ttl) = TimeDelta::try_minutes(FFLOGS_LEASE_TTL_MINUTES) else {
        return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response());
    };

    let processor_kind = fflogs_processor_kind(&headers);
    let worker_region = fflogs_worker_region(&headers);
    let limit = fflogs_jobs_limit(&headers, state.fflogs_jobs_limit);
    let max_client_leases = state.fflogs_jobs_limit.saturating_mul(2);
    let active_refresh_now = Utc::now();

    // Check per-client active lease cap and snapshot active leases before running DB queries.
    let active_leases = {
        let leases = state.fflogs_job_leases.read().await;
        if count_active_client_leases(&leases, &guard_ctx.client_id, lease_ttl, active_refresh_now)
            >= max_client_leases
        {
            return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response());
        }
        snapshot_active_leases(&leases, lease_ttl, active_refresh_now)
    };

    // 1. 현재 활성 파티 목록 가져오기 (1시간 이내)
    let listings = match get_current_listings(state.collection()).await {
        Ok(l) => l,
        Err(_) => return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response()),
    };

    let (batches, all_member_ids) = prepare_listing_job_batches(&listings, &active_leases);
    if all_member_ids.is_empty() {
        return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response());
    }

    let (parse_docs_res, failure_keys_res) = tokio::join!(
        get_parse_docs(state.parse_collection(), &all_member_ids),
        get_final_failure_keys(
            state.fflogs_job_failure_state_collection(),
            &all_member_ids,
            &processor_kind,
        ),
    );
    let parse_docs = match parse_docs_res {
        Ok(docs) => docs,
        Err(error) => {
            tracing::warn!(error = ?error, "failed to batch load parse cache docs for FFLogs jobs");
            return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response());
        }
    };
    let final_failure_keys = match failure_keys_res {
        Ok(keys) => keys,
        Err(error) => {
            tracing::warn!(
                error = ?error,
                processor_kind,
                "failed to batch load FFLogs job failure states"
            );
            HashSet::new()
        }
    };

    let selection_input = JobSelectionInput {
        parse_docs: &parse_docs,
        final_failure_keys: &final_failure_keys,
        active_leases: &active_leases,
        hidden_cache_ttl_hours: state.fflogs_hidden_cache_ttl_hours,
        active_parse_refresh_minutes: state.fflogs_active_parse_refresh_minutes,
        active_refresh_now,
    };
    let (prepared, all_needing_player_ids, hidden_refresh_candidates_in_batch) =
        prepare_listings_needing_fetch(batches, &selection_input);

    let mut jobs = if all_needing_player_ids.is_empty() {
        Vec::new()
    } else {
        match get_players_by_content_ids(state.players_collection(), &all_needing_player_ids).await
        {
            Ok(players) => {
                assemble_jobs_from_prepared(prepared, players, limit, worker_region)
            }
            Err(error) => {
                tracing::warn!(error = ?error, "failed to batch load players for FFLogs jobs");
                Vec::new()
            }
        }
    };

    let now = Utc::now();
    {
        let mut leases = state.fflogs_job_leases.write().await;

        // cleanup expired leases (worker crash / timeout safety)
        leases.retain(|_, lease| (lease.leased_at + lease_ttl) > now);

        let mut active_client_leases =
            count_active_client_leases(&leases, &guard_ctx.client_id, lease_ttl, now);
        let mut seen: HashSet<super::FflogsLeaseKey> = HashSet::new();
        let mut leased_jobs = Vec::with_capacity(jobs.len());

        for mut job in jobs {
            if active_client_leases >= max_client_leases {
                break;
            }

            let key = normalized_lease_key(job.content_id, job.zone_id, job.difficulty_id);

            // de-dup within this response payload
            if !seen.insert(key.clone()) {
                continue;
            }

            // skip if another worker already leased this key concurrently
            if leases.contains_key(&key) {
                continue;
            }

            let lease_token = Uuid::new_v4().to_string();
            leases.insert(
                key,
                super::FflogsLeaseEntry {
                    leased_at: now,
                    client_id: guard_ctx.client_id.clone(),
                    lease_token: lease_token.clone(),
                    processor_kind: processor_kind.clone(),
                    encounter_id: Some(job.encounter_id),
                    secondary_encounter_id: job.secondary_encounter_id,
                },
            );
            active_client_leases += 1;

            job.lease_token = lease_token;
            leased_jobs.push(job);
        }

        jobs = leased_jobs;
    }

    let dispatched_in_batch = jobs.len() as u64;
    if dispatched_in_batch > 0 {
        let dispatched_total = state
            .fflogs_jobs_dispatched_total
            .fetch_add(dispatched_in_batch, AtomicOrdering::Relaxed)
            + dispatched_in_batch;

        tracing::debug!(
            batch_jobs = dispatched_in_batch,
            total_jobs_dispatched = dispatched_total,
            "fflogs jobs dispatched",
        );
    }

    if hidden_refresh_candidates_in_batch > 0 {
        let hidden_refresh_total = state
            .fflogs_hidden_refresh_total
            .fetch_add(hidden_refresh_candidates_in_batch, AtomicOrdering::Relaxed)
            + hidden_refresh_candidates_in_batch;

        tracing::debug!(
            hidden_refresh_candidates_in_batch,
            hidden_refresh_total,
            "fflogs hidden cache refresh candidates detected",
        );
    }

    Ok(warp::reply::json(&jobs).into_response())
}

pub(crate) async fn contribute_fflogs_results_handler_with_guard(
    state: Arc<State>,
    guard_ctx: GuardContext,
    results: Vec<ParseResult>,
) -> std::result::Result<warp::reply::Response, Infallible> {

    let submitted_results_total = results.len();
    if submitted_results_total > state.max_fflogs_results_batch_size {
        return Ok(payload_too_large_response("too many FFLogs results in request"));
    }

    let Some(lease_ttl) = TimeDelta::try_minutes(FFLOGS_LEASE_TTL_MINUTES) else {
        return Ok(warp::reply::json(&ParseResultsResponse {
            status: "partial",
            submitted: submitted_results_total,
            accepted: 0,
            updated: 0,
            rejected: submitted_results_total,
        })
        .into_response());
    };
    let now = Utc::now();
    let mut accepted_results = Vec::with_capacity(submitted_results_total);
    let mut seen_results = HashSet::with_capacity(submitted_results_total);

    {
        let leases = state.fflogs_job_leases.read().await;
        for res in results {
            if !has_valid_parse_result_bounds(
                res.encounters.len(),
                res.boss_percentages.len(),
                res.clear_counts.len(),
                res.phase_progress.len(),
            ) {
                continue;
            }

            let key = normalized_lease_key(res.content_id, res.zone_id, res.difficulty_id);
            if !seen_results.insert((key.clone(), res.lease_token.clone())) {
                continue;
            }

            let Some(lease) = leases.get(&key) else {
                continue;
            };

            if !lease_matches(
                Some(lease),
                &guard_ctx.client_id,
                &res.lease_token,
                lease_ttl,
                now,
            ) {
                continue;
            }

            accepted_results.push((key, lease.encounter_id, lease.secondary_encounter_id, res));
        }
    }

    let accepted_results_total = accepted_results.len();
    let mut upsert_items = Vec::with_capacity(accepted_results_total);
    let mut lease_meta = Vec::with_capacity(accepted_results_total);

    for (lease_key, lease_encounter_id, lease_secondary_encounter_id, res) in accepted_results {
        let zone_key = crate::fflogs::make_zone_cache_key(res.zone_id, res.difficulty_id);
        let content_id = res.content_id;
        let lease_token = res.lease_token.clone();
        let progress_coverage = res.tomestone_queried_encounters.clone();
        let fflogs_coverage = resolve_fflogs_rank_coverage(
            res.fflogs_queried_encounters.as_deref(),
            res.encounters.keys().copied(),
            res.clear_counts.keys().copied(),
            lease_encounter_id,
            lease_secondary_encounter_id,
        );
        let zone_cache = zone_cache_from_parse_result(res, now);

        upsert_items.push(crate::mongo::ZoneCacheUpsertItem {
            content_id,
            zone_key: zone_key.clone(),
            zone_cache,
            progress_coverage,
            fflogs_coverage: Some(fflogs_coverage),
            provider: ParseCacheProvider::Fflogs,
        });
        lease_meta.push((lease_key, lease_token, content_id, zone_key));
    }

    let mut success_count = 0usize;
    let mut committed_leases = Vec::with_capacity(accepted_results_total);
    let mut clear_filters = Vec::with_capacity(accepted_results_total);
    let mut write_failed = false;

    match crate::mongo::upsert_zone_caches_batch(state.parse_collection(), &upsert_items).await {
        Ok(summary) => {
            for failure in &summary.failures {
                if let Some((_, _, content_id, zone_key)) = lease_meta.get(failure.item_index) {
                    tracing::warn!(
                        content_id,
                        zone_key,
                        code = failure.code,
                        error = %failure.errmsg,
                        "failed to persist FFLogs zone cache result"
                    );
                }
            }

            if let Some(wc_err) = summary.write_concern_error.as_deref() {
                write_failed = true;
                tracing::warn!(
                    accepted_results_total,
                    error = wc_err,
                    "writeConcernError while persisting FFLogs zone cache results"
                );
            } else {
                for (idx, (lease_key, lease_token, _, _)) in lease_meta.into_iter().enumerate() {
                    if summary.succeeded_indices.contains(&idx) {
                        success_count += 1;
                        clear_filters.push(fflogs_failure_state_clear_filter(
                            lease_key.content_id,
                            lease_key.zone_id,
                            lease_key.difficulty_id,
                        ));
                        committed_leases.push((lease_key, lease_token));
                    }
                }

                if accepted_results_total > 0 && success_count < accepted_results_total {
                    write_failed = true;
                }
            }

            if !clear_filters.is_empty() {
                if let Err(error) = clear_fflogs_failure_states_batch(
                    state.fflogs_job_failure_state_collection(),
                    &clear_filters,
                )
                .await
                {
                    tracing::warn!(
                        count = clear_filters.len(),
                        error = ?error,
                        "failed to batch clear FFLogs job failure state after commit"
                    );
                }
            }
        }
        Err(error) => {
            write_failed = true;
            tracing::warn!(
                accepted_results_total,
                error = ?error,
                "failed to batch persist FFLogs zone cache results"
            );
        }
    }

    // Release only leases whose DB commit succeeded and whose lease token still matches.
    if !committed_leases.is_empty() {
        let mut leases = state.fflogs_job_leases.write().await;
        release_committed_leases(&mut leases, &committed_leases, &guard_ctx.client_id);
    }

    if accepted_results_total > 0 {
        let results_total = state
            .fflogs_results_received_total
            .fetch_add(accepted_results_total as u64, AtomicOrdering::Relaxed)
            + accepted_results_total as u64;

        tracing::debug!(
            accepted_results_total,
            submitted_results_total,
            success_count,
            results_total,
            "fflogs results processed",
        );
    }
    if success_count > 0 {
        state.notify_listings_changed(success_count);
    }

    let rejected_results_total = submitted_results_total.saturating_sub(accepted_results_total);
    if rejected_results_total > 0 {
        state
            .fflogs_results_rejected_total
            .fetch_add(rejected_results_total as u64, AtomicOrdering::Relaxed);
    }

    Ok(parse_results_response(
        write_failed,
        submitted_results_total,
        accepted_results_total,
        success_count,
        rejected_results_total,
    ))
}

pub(crate) async fn contribute_fflogs_leases_abandon_handler_with_guard(
    state: Arc<State>,
    headers: HeaderMap,
    guard_ctx: GuardContext,
    abandoned_leases: Vec<AbandonFflogsLease>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let submitted_total = abandoned_leases.len();
    if submitted_total > state.max_fflogs_results_batch_size {
        return Ok(payload_too_large_response(
            "too many FFLogs lease abandons in request",
        ));
    }

    let Some(lease_ttl) = TimeDelta::try_minutes(FFLOGS_LEASE_TTL_MINUTES) else {
        return Ok(warp::reply::json(&ParseLeaseAbandonResponse {
            status: "partial",
            submitted: submitted_total,
            released: 0,
            rejected: submitted_total,
        })
        .into_response());
    };
    let now = Utc::now();
    let request_processor_kind = fflogs_processor_kind(&headers);
    let validation = {
        let leases = state.fflogs_job_leases.read().await;
        validate_abandoned_leases(
            &leases,
            abandoned_leases,
            &guard_ctx.client_id,
            &request_processor_kind,
            lease_ttl,
            now,
        )
    };

    match record_failure_states_batch(
        state.fflogs_job_failure_state_collection(),
        &validation.failure_states,
        state.fflogs_non_fetch_failure_abandon_threshold,
    )
    .await
    {
        Ok(summary) => {
            if !summary.failures.is_empty() {
                tracing::warn!(
                    failed = summary.failures.len(),
                    "failed to batch record some FFLogs job failure states"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                error = ?error,
                count = validation.failure_states.len(),
                "failed to batch record FFLogs job failure states"
            );
        }
    }

    let released_total = {
        let mut leases = state.fflogs_job_leases.write().await;
        release_valid_abandoned_leases(&mut leases, &validation.accepted, lease_ttl, now)
    };
    let rejected_total =
        validation.rejected_total + validation.accepted.len().saturating_sub(released_total);
    let reasons = validation.reasons;

    if released_total > 0 {
        state
            .fflogs_leases_abandoned_total
            .fetch_add(released_total as u64, AtomicOrdering::Relaxed);

        tracing::debug!(
            client_id = guard_ctx.client_id,
            submitted_total,
            released_total,
            rejected_total,
            reasons = ?reasons,
            "fflogs lease abandon processed"
        );
    }

    if rejected_total > 0 {
        state
            .fflogs_leases_abandon_rejected_total
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
mod tests;
