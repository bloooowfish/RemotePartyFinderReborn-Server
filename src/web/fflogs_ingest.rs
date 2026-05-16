use chrono::{TimeDelta, Utc};
use std::{
    collections::HashSet,
    convert::Infallible,
    net::SocketAddr,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
};
use uuid::Uuid;
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use super::ingest_guard::{self, CapabilityScope, IngestEndpoint};
use super::{State, FFLOGS_LEASE_TTL_MINUTES};
use crate::mongo::{get_current_listings, get_players_by_content_ids};

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
    TOMESTONE_NO_DATA_REASON,
};
use failure_state::{
    get_final_failure_content_ids, record_failure_state, release_valid_abandoned_leases,
    validate_abandoned_leases,
};
use jobs::{
    active_fflogs_fetch_candidates, append_fflogs_jobs_for_players, build_candidate_servers,
    normalized_lease_key, FflogsJobAppendContext,
};
pub use payloads::{AbandonFflogsLease, ParseJob, ParseResult};
use request::{fflogs_exact_home_world_only, fflogs_processor_kind, fflogs_worker_region};
#[cfg(test)]
use request::{
    FflogsWorkerRegion, DEFAULT_FFLOGS_PROCESSOR_KIND, FFLOGS_JOB_MODE_HEADER,
    FFLOGS_PROCESSOR_KIND_HEADER, FFLOGS_WORKER_REGION_HEADER, RATE_LIMIT_DEFERRED_REASON,
};
#[cfg(test)]
use results::build_fflogs_result_encounter_map;
use results::zone_cache_from_parse_result;

#[derive(Debug, serde::Serialize)]
struct ContributeFflogsResultsResponse {
    status: &'static str,
    submitted: usize,
    accepted: usize,
    updated: usize,
    rejected: usize,
}

#[derive(Debug, serde::Serialize)]
struct ContributeFflogsLeaseAbandonResponse {
    status: &'static str,
    submitted: usize,
    released: usize,
    rejected: usize,
}

/// 플러그인에게 파싱 작업 할당
pub async fn contribute_fflogs_jobs_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsJobs,
        &headers,
        remote_addr,
        "GET",
        "/contribute/fflogs/jobs",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::FflogsJobs,
        None,
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let exact_home_world_only = fflogs_exact_home_world_only(&headers);
    let processor_kind = fflogs_processor_kind(&headers);
    let worker_region = fflogs_worker_region(&headers);

    // 1. 현재 활성 파티 목록 가져오기 (1시간 이내)
    let listings = match get_current_listings(state.collection()).await {
        Ok(l) => l,
        Err(_) => return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response()),
    };

    let mut jobs = Vec::new();
    let mut hidden_refresh_candidates_in_batch = 0u64;
    let limit = state.fflogs_jobs_limit; // 한 번에 할당할 작업 수
    let active_refresh_now = Utc::now();

    // Zone별 플레이어 수집, background.rs 로직 재사용
    // 여기서는 간단하게 순회하며 필요한 작업 찾으면 바로 반환 (Greedy)

    // Shuffle listings to distribute load? (Optional, maybe later)

    for container in listings {
        if jobs.len() >= limit {
            break;
        }

        if !container.listing.high_end() {
            continue;
        }

        let duty_id = container.listing.duty;
        let fflogs_info = match crate::fflogs::mapping::get_fflogs_encounter(duty_id) {
            Some(info) => info,
            None => continue,
        };

        let difficulty_id = fflogs_info.difficulty_id.unwrap_or(0) as i32;
        let zone_key = crate::fflogs::make_zone_cache_key(fflogs_info.zone_id, difficulty_id);

        let member_ids: Vec<u64> = container
            .listing
            .member_content_ids
            .iter()
            .map(|&id| id as u64)
            .filter(|&id| id != 0)
            .collect();

        if member_ids.is_empty() {
            continue;
        }

        // 캐시 확인 (Batch)
        let cached_zones =
            match crate::mongo::get_zone_caches(state.parse_collection(), &member_ids, &zone_key)
                .await
            {
                Ok(map) => map,
                Err(_) => continue,
            };

        // 작업이 필요한 멤버 식별. Active PF에서는 저장 TTL보다 빠른 refresh window를 사용한다.
        let fetch_candidates = active_fflogs_fetch_candidates(
            member_ids,
            &cached_zones,
            state.fflogs_hidden_cache_ttl_hours,
            state.fflogs_active_parse_refresh_minutes,
            active_refresh_now,
        );
        hidden_refresh_candidates_in_batch += fetch_candidates.hidden_refresh_candidates;
        let mut needing_fetch = fetch_candidates.content_ids;

        if needing_fetch.is_empty() {
            continue;
        }

        let final_failure_content_ids = match get_final_failure_content_ids(
            state.fflogs_job_failure_state_collection(),
            &needing_fetch,
            fflogs_info.zone_id,
            difficulty_id,
            &processor_kind,
        )
        .await
        {
            Ok(ids) => ids,
            Err(error) => {
                tracing::warn!(
                    error = ?error,
                    processor_kind,
                    zone_id = fflogs_info.zone_id,
                    difficulty_id,
                    "failed to load FFLogs job failure states"
                );
                HashSet::new()
            }
        };
        if !final_failure_content_ids.is_empty() {
            needing_fetch.retain(|content_id| !final_failure_content_ids.contains(content_id));
        }

        if needing_fetch.is_empty() {
            continue;
        }

        // 플레이어 정보 조회 (이름/서버)
        let players =
            match get_players_by_content_ids(state.players_collection(), &needing_fetch).await {
                Ok(p) => p,
                Err(_) => continue,
            };

        // 홈월드가 없는 플레이어를 위한 후보 서버 목록(리스트 단위)
        let listing_anchor_world = [
            container.listing.current_world,
            container.listing.created_world,
            container.listing.home_world,
        ]
        .into_iter()
        .find(|wid| *wid != 0 && crate::ffxiv::WORLDS.get(&(*wid as u32)).is_some())
        .unwrap_or(0);

        let listing_candidates = if listing_anchor_world != 0 {
            build_candidate_servers(listing_anchor_world)
        } else {
            Vec::new()
        };

        append_fflogs_jobs_for_players(
            &mut jobs,
            &needing_fetch,
            players,
            FflogsJobAppendContext {
                limit,
                exact_home_world_only,
                worker_region,
                listing_anchor_world,
                listing_candidates: &listing_candidates,
                fflogs_info,
                difficulty_id,
            },
        );
    }

    // lease selected jobs to avoid duplicate dispatch across concurrent workers
    let Some(lease_ttl) = TimeDelta::try_minutes(FFLOGS_LEASE_TTL_MINUTES) else {
        return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response());
    };
    let now = Utc::now();

    {
        let mut leases = state.fflogs_job_leases.write().await;

        // cleanup expired leases (worker crash / timeout safety)
        leases.retain(|_, lease| (lease.leased_at + lease_ttl) > now);

        let mut seen: HashSet<super::FflogsLeaseKey> = HashSet::new();
        let mut leased_jobs = Vec::with_capacity(jobs.len());

        for mut job in jobs {
            let key = normalized_lease_key(job.content_id, job.zone_id, job.difficulty_id);

            // de-dup within this response payload
            if !seen.insert(key.clone()) {
                continue;
            }

            // skip if another worker already leased this key
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

/// 플러그인으로부터 파싱 결과 수신
pub async fn contribute_fflogs_results_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    results: Vec<ParseResult>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsResults,
        &headers,
        remote_addr,
        "POST",
        "/contribute/fflogs/results",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::FflogsResults,
        None,
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let submitted_results_total = results.len();
    if submitted_results_total > state.max_fflogs_results_batch_size {
        return Ok(warp::reply::with_status(
            "too many FFLogs results in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let mut accepted_results = Vec::with_capacity(submitted_results_total);
    let mut lease_keys_to_release: Vec<super::FflogsLeaseKey> = Vec::new();

    {
        let leases = state.fflogs_job_leases.read().await;
        for res in results {
            let key = normalized_lease_key(res.content_id, res.zone_id, res.difficulty_id);

            let Some(lease) = leases.get(&key) else {
                continue;
            };

            if lease.client_id != guard_ctx.client_id {
                continue;
            }

            if res.lease_token.is_empty() || lease.lease_token != res.lease_token {
                continue;
            }

            lease_keys_to_release.push(key);
            accepted_results.push(res);
        }
    }

    let accepted_results_total = accepted_results.len();
    let mut success_count = 0;

    for res in accepted_results {
        let zone_key = crate::fflogs::make_zone_cache_key(res.zone_id, res.difficulty_id);
        let content_id = res.content_id;
        let zone_cache = zone_cache_from_parse_result(res, chrono::Utc::now());

        // DB Upsert
        if crate::mongo::upsert_zone_cache(
            state.parse_collection(),
            content_id,
            &zone_key,
            &zone_cache,
        )
        .await
        .is_ok()
        {
            success_count += 1;
        }
    }

    // release leases for returned results (success/failure both) so next polling can retry quickly
    {
        let mut leases = state.fflogs_job_leases.write().await;
        for key in lease_keys_to_release {
            leases.remove(&key);
        }
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

    let status = if rejected_results_total == 0 {
        "ok"
    } else {
        "partial"
    };

    Ok(warp::reply::json(&ContributeFflogsResultsResponse {
        status,
        submitted: submitted_results_total,
        accepted: accepted_results_total,
        updated: success_count,
        rejected: rejected_results_total,
    })
    .into_response())
}

/// 플러그인으로부터 미처리 FFLogs lease 즉시 반납 수신
pub async fn contribute_fflogs_leases_abandon_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    abandoned_leases: Vec<AbandonFflogsLease>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsLeasesAbandon,
        &headers,
        remote_addr,
        "POST",
        "/contribute/fflogs/leases/abandon",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::FflogsLeasesAbandon,
        None,
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let submitted_total = abandoned_leases.len();
    if submitted_total > state.max_fflogs_results_batch_size {
        return Ok(warp::reply::with_status(
            "too many FFLogs lease abandons in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let request_processor_kind = fflogs_processor_kind(&headers);
    let validation = {
        let leases = state.fflogs_job_leases.read().await;
        validate_abandoned_leases(
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
        let reason = failure_state.reason.clone();
        let failure_code = failure_state.failure_code.clone();
        if let Err(error) = record_failure_state(
            state.fflogs_job_failure_state_collection(),
            failure_state,
            state.fflogs_non_fetch_failure_abandon_threshold,
        )
        .await
        {
            tracing::warn!(
                error = ?error,
                processor_kind,
                content_id,
                zone_id,
                reason,
                failure_code,
                "failed to record FFLogs job failure state"
            );
        }
    }

    let released_total = {
        let mut leases = state.fflogs_job_leases.write().await;
        release_valid_abandoned_leases(&mut leases, &validation.accepted)
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
    Ok(warp::reply::json(&ContributeFflogsLeaseAbandonResponse {
        status,
        submitted: submitted_total,
        released: released_total,
        rejected: rejected_total,
    })
    .into_response())
}

#[cfg(test)]
mod tests;
