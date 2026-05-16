use anyhow::Context;
use chrono::Utc;
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
};
use std::{collections::HashSet, convert::Infallible, net::SocketAddr, sync::Arc};
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use crate::listing::PartyFinderListing;
use crate::mongo::{insert_listing, upsert_character_identities, upsert_players};
use crate::player::{UploadableCharacterIdentity, UploadablePlayer};

use super::ingest_guard::{self, CapabilityClaims, CapabilityScope, IngestEndpoint};
use super::State;
pub async fn contribute_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    listing: PartyFinderListing,
) -> std::result::Result<warp::reply::Response, Infallible> {
    if let Err(error) = ingest_guard::authorize_request(
        &state,
        IngestEndpoint::Contribute,
        &headers,
        remote_addr,
        "POST",
        "/contribute",
    )
    .await
    {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    if listing.seconds_remaining > 60 * 60 {
        return Ok(
            warp::reply::with_status("invalid listing", StatusCode::BAD_REQUEST).into_response(),
        );
    }

    let upsert_result = insert_listing(state.collection(), &listing).await;
    if upsert_result.is_ok() {
        state.notify_listings_changed(1);
    }
    Ok(format!("{:#?}", upsert_result).into_response())
}

fn is_listing_acceptable(listing: &PartyFinderListing) -> bool {
    if listing.seconds_remaining > 60 * 60 {
        return false;
    }

    listing.created_world < 1_000 && listing.home_world < 1_000 && listing.current_world < 1_000
}

fn listing_expires_at_expression(seconds_remaining: i64) -> Document {
    doc! {
        "$dateAdd": {
            "startDate": "$$NOW",
            "unit": "second",
            "amount": seconds_remaining.max(0),
        }
    }
}

fn build_listing_upsert(
    listing: &PartyFinderListing,
) -> anyhow::Result<(u32, Document, Vec<Document>)> {
    let mut listing_doc = mongodb::bson::to_document(listing)?;
    listing_doc.remove("member_content_ids");
    listing_doc.remove("member_jobs");
    listing_doc.remove("detail_slot_flags");
    listing_doc.remove("leader_content_id");

    let filter = doc! {
        "listing.id": listing.id,
        "listing.last_server_restart": listing.last_server_restart,
        "listing.created_world": listing.created_world as u32,
    };
    let update = vec![doc! {
        "$set": {
            "updated_at": "$$NOW",
            "created_at": { "$ifNull": ["$created_at", "$$NOW"] },
            "expires_at": listing_expires_at_expression(listing.seconds_remaining as i64),
            "listing": {
                "$mergeObjects": [
                    "$listing",
                    listing_doc,
                ]
            },
        }
    }];

    Ok((listing.id, filter, update))
}

pub async fn contribute_multiple_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    listings: Vec<PartyFinderListing>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeMultiple,
        &headers,
        remote_addr,
        "POST",
        "/contribute/multiple",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let total = listings.len();
    if total > state.max_multiple_batch_size {
        return Ok(warp::reply::with_status(
            "too many listings in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let mut upserts = Vec::new();
    for listing in &listings {
        if !is_listing_acceptable(listing) {
            continue;
        }

        match build_listing_upsert(listing) {
            Ok(op) => upserts.push(op),
            Err(error) => {
                tracing::debug!("Failed to serialize listing {}: {:#?}", listing.id, error);
            }
        }
    }

    let upsert_total = upserts.len();
    let mut successful = 0usize;
    let mut failed_writes = 0usize;
    let mut successful_listing_ids = Vec::with_capacity(upsert_total);
    let mut writes =
        futures_util::stream::iter(upserts.into_iter().map(|(listing_id, filter, update)| {
            let collection = state.collection();
            async move {
                collection
                    .update_one(
                        filter,
                        update,
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map(|_| listing_id)
                    .map_err(|error| (listing_id, error))
            }
        }))
        .buffer_unordered(state.listing_upsert_concurrency);

    while let Some(write_result) = writes.next().await {
        match write_result {
            Ok(listing_id) => {
                successful += 1;
                successful_listing_ids.push(listing_id);
            }
            Err((listing_id, error)) => {
                failed_writes += 1;
                tracing::debug!("Failed to insert listing {}: {:#?}", listing_id, error);
            }
        }
    }

    if failed_writes > 0 {
        tracing::warn!(
            requested_total = total,
            attempted_upserts = upsert_total,
            successful_upserts = successful,
            failed_writes,
            "Processed /contribute/multiple request with insert failures"
        );
    }

    if !successful_listing_ids.is_empty() {
        let successful_listing_ids_set: HashSet<u32> =
            successful_listing_ids.iter().copied().collect();
        let changed_listings: Vec<PartyFinderListing> = listings
            .into_iter()
            .filter(|listing| successful_listing_ids_set.contains(&listing.id))
            .collect();
        if !changed_listings.is_empty() {
            let changed_count = changed_listings.len();
            state.notify_listings_changed(changed_count);
        }
    }
    let response = ContributeMultipleResponse {
        status: if failed_writes == 0 { "ok" } else { "partial" },
        requested: total,
        accepted: upsert_total,
        updated: successful,
        failed: failed_writes,
        detail_capabilities: issue_detail_capabilities(
            &state,
            &guard_ctx.client_id,
            &successful_listing_ids,
        ),
        protected_endpoints: issue_protected_endpoint_capabilities(&state, &guard_ctx.client_id),
    };
    Ok(warp::reply::json(&response).into_response())
}

pub async fn contribute_players_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    players: Vec<UploadablePlayer>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributePlayers,
        &headers,
        remote_addr,
        "POST",
        "/contribute/players",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let total = players.len();
    if total > state.max_players_batch_size {
        return Ok(warp::reply::with_status(
            "too many players in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let result = upsert_players(
        state.players_collection(),
        &players,
        state.player_upsert_concurrency,
    )
    .await;

    #[derive(Debug, serde::Serialize)]
    struct ContributePlayersResponse {
        status: &'static str,
        requested: usize,
        accepted: usize,
        updated: usize,
        invalid: usize,
        failed: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        protected_endpoints: Option<ProtectedEndpointCapabilities>,
    }

    match result {
        Ok(report) => {
            let (status_code, status) = if report.requested == 0 {
                (StatusCode::OK, "empty")
            } else if report.failed > 0 {
                (StatusCode::INTERNAL_SERVER_ERROR, "write_failure")
            } else if report.invalid > 0 {
                (StatusCode::BAD_REQUEST, "invalid_payload")
            } else {
                (StatusCode::OK, "ok")
            };

            if status_code == StatusCode::OK {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    "Processed /contribute/players request"
                );
            } else if status == "write_failure" {
                tracing::warn!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/players request with issues"
                );
            } else {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/players request with issues"
                );
            }

            if report.updated > 0 {
                state.notify_listings_changed(report.updated);
            }

            let response = ContributePlayersResponse {
                status,
                requested: report.requested,
                accepted: report.accepted,
                updated: report.updated,
                invalid: report.invalid,
                failed: report.failed,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(warp::reply::json(&response), status_code).into_response())
        }
        Err(e) => {
            tracing::error!("error upserting players: {:#?}", e);

            let response = ContributePlayersResponse {
                status: "error",
                requested: total,
                accepted: 0,
                updated: 0,
                invalid: 0,
                failed: total,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

pub async fn contribute_character_identity_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    identities: Vec<UploadableCharacterIdentity>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributePlayers,
        &headers,
        remote_addr,
        "POST",
        "/contribute/character-identity",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let total = identities.len();
    if total > state.max_players_batch_size {
        return Ok(warp::reply::with_status(
            "too many character identities in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let result = upsert_character_identities(
        state.players_collection(),
        &identities,
        state.player_upsert_concurrency,
        state.ingest_security.clock_skew_seconds,
    )
    .await;

    #[derive(Debug, serde::Serialize)]
    struct ContributeCharacterIdentityResponse {
        status: &'static str,
        requested: usize,
        accepted: usize,
        updated: usize,
        invalid: usize,
        failed: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        protected_endpoints: Option<ProtectedEndpointCapabilities>,
    }

    match result {
        Ok(report) => {
            let (status_code, status) = if report.requested == 0 {
                (StatusCode::OK, "empty")
            } else if report.failed > 0 {
                (StatusCode::INTERNAL_SERVER_ERROR, "write_failure")
            } else if report.invalid > 0 {
                (StatusCode::BAD_REQUEST, "invalid_payload")
            } else {
                (StatusCode::OK, "ok")
            };

            if status_code == StatusCode::OK {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    "Processed /contribute/character-identity request"
                );
            } else if status == "write_failure" {
                tracing::warn!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/character-identity request with issues"
                );
            } else {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/character-identity request with issues"
                );
            }

            if report.updated > 0 {
                state.notify_listings_changed(report.updated);
            }

            let response = ContributeCharacterIdentityResponse {
                status,
                requested: report.requested,
                accepted: report.accepted,
                updated: report.updated,
                invalid: report.invalid,
                failed: report.failed,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(warp::reply::json(&response), status_code).into_response())
        }
        Err(error) => {
            tracing::error!("error upserting character identities: {:#?}", error);

            let response = ContributeCharacterIdentityResponse {
                status: "error",
                requested: total,
                accepted: 0,
                updated: 0,
                invalid: 0,
                failed: total,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

/// 파티 상세 정보 (멤버 ContentId 목록)
#[derive(Debug, serde::Deserialize)]
pub struct UploadablePartyDetail {
    pub listing_id: u32,
    pub leader_content_id: u64,
    pub leader_name: String,
    pub home_world: u16,
    pub member_content_ids: Vec<u64>,
    #[serde(default)]
    pub member_jobs: Option<Vec<u8>>,
    #[serde(default)]
    pub slot_flags: Option<Vec<String>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct UploadablePartyDetailBatchItem {
    #[serde(flatten)]
    detail: UploadablePartyDetail,
    #[serde(default)]
    capability_token: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct ContributeDetailResponse {
    status: &'static str,
    matched_count: u64,
    modified_count: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
struct DetailBatchItemResponse {
    listing_id: u32,
    status: &'static str,
    matched_count: u64,
    modified_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<&'static str>,
}

#[derive(Debug, serde::Serialize)]
struct ContributeDetailBatchResponse {
    status: &'static str,
    requested: usize,
    accepted: usize,
    applied: usize,
    missing: usize,
    failed: usize,
    results: Vec<DetailBatchItemResponse>,
}

#[derive(Debug, serde::Serialize, Clone)]
struct ProtectedEndpointCapability {
    token: String,
    expires_at: i64,
}

#[derive(Debug, serde::Serialize, Clone)]
struct ProtectedEndpointCapabilities {
    fflogs_jobs: ProtectedEndpointCapability,
    fflogs_results: ProtectedEndpointCapability,
    fflogs_leases_abandon: ProtectedEndpointCapability,
}

#[derive(Debug, serde::Serialize, Clone)]
struct ListingDetailCapability {
    listing_id: u32,
    token: String,
    expires_at: i64,
}

#[derive(Debug, serde::Serialize)]
struct ContributeMultipleResponse {
    status: &'static str,
    requested: usize,
    accepted: usize,
    updated: usize,
    failed: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    detail_capabilities: Vec<ListingDetailCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protected_endpoints: Option<ProtectedEndpointCapabilities>,
}

fn build_detail_update_doc(detail: &UploadablePartyDetail) -> Document {
    let member_ids_i64 = detail
        .member_content_ids
        .iter()
        .map(|id| *id as i64)
        .collect::<Vec<_>>();

    let mut set_doc = doc! {
        "listing.member_content_ids": member_ids_i64,
        "listing.leader_content_id": detail.leader_content_id as i64,
    };

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        let member_jobs_i32 = member_jobs
            .iter()
            .map(|job| i32::from(*job))
            .collect::<Vec<_>>();
        set_doc.insert("listing.member_jobs", member_jobs_i32);
    }

    if let Some(slot_flags) = detail.slot_flags.as_ref() {
        set_doc.insert("listing.detail_slot_flags", slot_flags.clone());
    }

    set_doc
}

fn issue_capability(
    state: &State,
    client_id: &str,
    scope: CapabilityScope,
    resource_id: Option<String>,
    expires_at: i64,
) -> Option<ProtectedEndpointCapability> {
    let claims = CapabilityClaims::new(
        client_id.to_string(),
        scope,
        resource_id,
        Utc::now().timestamp(),
        expires_at,
    );

    match ingest_guard::issue_capability_token(&state.ingest_security.capability_secret, &claims) {
        Ok(token) => Some(ProtectedEndpointCapability { token, expires_at }),
        Err(error) => {
            tracing::warn!(
                ?scope,
                ?error,
                "failed to issue protected endpoint capability"
            );
            None
        }
    }
}

fn issue_protected_endpoint_capabilities(
    state: &State,
    client_id: &str,
) -> Option<ProtectedEndpointCapabilities> {
    let expires_at = Utc::now().timestamp() + state.ingest_security.capability_session_ttl_seconds;
    Some(ProtectedEndpointCapabilities {
        fflogs_jobs: issue_capability(
            state,
            client_id,
            CapabilityScope::FflogsJobs,
            None,
            expires_at,
        )?,
        fflogs_results: issue_capability(
            state,
            client_id,
            CapabilityScope::FflogsResults,
            None,
            expires_at,
        )?,
        fflogs_leases_abandon: issue_capability(
            state,
            client_id,
            CapabilityScope::FflogsLeasesAbandon,
            None,
            expires_at,
        )?,
    })
}

fn issue_detail_capabilities(
    state: &State,
    client_id: &str,
    listing_ids: &[u32],
) -> Vec<ListingDetailCapability> {
    let expires_at = Utc::now().timestamp() + state.ingest_security.capability_detail_ttl_seconds;
    let mut deduped = HashSet::new();
    let mut issued = Vec::with_capacity(listing_ids.len());

    for listing_id in listing_ids {
        if !deduped.insert(*listing_id) {
            continue;
        }

        let Some(capability) = issue_capability(
            state,
            client_id,
            CapabilityScope::Detail,
            Some(format!("listing:{listing_id}")),
            expires_at,
        ) else {
            continue;
        };

        issued.push(ListingDetailCapability {
            listing_id: *listing_id,
            token: capability.token,
            expires_at: capability.expires_at,
        });
    }

    issued
}

fn detail_payload_is_too_large(
    detail: &UploadablePartyDetail,
    limit: usize,
) -> Option<&'static str> {
    if detail.member_content_ids.len() > limit {
        return Some("too many members in request");
    }

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        if member_jobs.len() > limit {
            return Some("too many member jobs in request");
        }
    }

    if let Some(slot_flags) = detail.slot_flags.as_ref() {
        if slot_flags.len() > limit {
            return Some("too many slot flags in request");
        }
    }

    None
}

fn detail_batch_terminal_result(
    detail: &UploadablePartyDetail,
    status: &'static str,
    message: &'static str,
) -> DetailBatchItemResponse {
    DetailBatchItemResponse {
        listing_id: detail.listing_id,
        status,
        matched_count: 0,
        modified_count: 0,
        message: Some(message),
    }
}

fn guard_error_message(error: ingest_guard::GuardError) -> &'static str {
    match error {
        ingest_guard::GuardError::BadRequest(message)
        | ingest_guard::GuardError::Unauthorized(message)
        | ingest_guard::GuardError::Forbidden(message) => message,
        ingest_guard::GuardError::TooManyRequests { .. } => "too many requests",
    }
}

fn preflight_detail_batch_item(
    state: &State,
    client_id: &str,
    item: &UploadablePartyDetailBatchItem,
) -> Option<DetailBatchItemResponse> {
    if let Some(message) = detail_payload_is_too_large(&item.detail, state.max_detail_member_count)
    {
        return Some(detail_batch_terminal_result(
            &item.detail,
            "invalid",
            message,
        ));
    }

    if !state
        .ingest_security
        .require_capabilities_for_protected_endpoints
    {
        return None;
    }

    let Some(token) = item.capability_token.as_deref() else {
        return Some(detail_batch_terminal_result(
            &item.detail,
            "forbidden",
            "missing capability token",
        ));
    };
    let resource_id = format!("listing:{}", item.detail.listing_id);
    match ingest_guard::validate_capability_token(
        &state.ingest_security.capability_secret,
        token,
        CapabilityScope::Detail,
        client_id,
        Some(&resource_id),
        Utc::now().timestamp(),
    ) {
        Ok(_) => None,
        Err(error) => Some(detail_batch_terminal_result(
            &item.detail,
            "forbidden",
            guard_error_message(error),
        )),
    }
}

fn build_detail_batch_response(
    results: Vec<DetailBatchItemResponse>,
) -> ContributeDetailBatchResponse {
    let requested = results.len();
    let mut accepted = 0usize;
    let mut applied = 0usize;
    let mut missing = 0usize;
    let mut failed = 0usize;

    for result in &results {
        match result.status {
            "ok" => {
                accepted += 1;
                applied += 1;
            }
            "missing" => {
                accepted += 1;
                missing += 1;
            }
            "failed" => {
                accepted += 1;
                failed += 1;
            }
            _ => {}
        }
    }

    ContributeDetailBatchResponse {
        status: if failed == 0 { "ok" } else { "partial" },
        requested,
        accepted,
        applied,
        missing,
        failed,
        results,
    }
}

fn log_detail_payload(detail: &UploadablePartyDetail) {
    let non_zero_member_count = detail
        .member_content_ids
        .iter()
        .filter(|id| **id != 0)
        .count();
    let member_jobs_len = detail.member_jobs.as_ref().map_or(0, |jobs| jobs.len());
    let non_zero_job_count = detail
        .member_jobs
        .as_ref()
        .map_or(0, |jobs| jobs.iter().filter(|job| **job != 0).count());
    let slot_flags_len = detail.slot_flags.as_ref().map_or(0, |flags| flags.len());
    tracing::debug!(
        listing_id = detail.listing_id,
        member_count = detail.member_content_ids.len(),
        non_zero_member_count,
        member_jobs_len,
        non_zero_job_count,
        slot_flags_len,
        "received party detail payload"
    );
}

fn build_detail_leader_player_if_valid(
    detail: &UploadablePartyDetail,
) -> Option<crate::player::UploadablePlayer> {
    if detail.leader_content_id == 0
        || detail.leader_name.trim().is_empty()
        || detail.home_world >= 1000
    {
        return None;
    }

    Some(crate::player::UploadablePlayer {
        content_id: detail.leader_content_id,
        name: detail.leader_name.clone(),
        home_world: detail.home_world,
        current_world: 0,
        account_id: 0,
    })
}

fn spawn_detail_leader_upsert_if_valid(state: &Arc<State>, detail: &UploadablePartyDetail) {
    let Some(leader) = build_detail_leader_player_if_valid(detail) else {
        tracing::debug!(
            "Skipping leader upsert: ID={} Name='{}' World={}",
            detail.leader_content_id,
            detail.leader_name,
            detail.home_world,
        );

        return;
    };

    let players_collection = state.players_collection();
    let concurrency = state.player_upsert_concurrency;
    let leader_content_id = leader.content_id;

    tokio::task::spawn(async move {
        let leaders = [leader];
        let result = upsert_players(players_collection, &leaders, concurrency).await;

        match result {
            Ok(report) => {
                if report.updated == 1 && report.failed == 0 && report.invalid == 0 {
                    tracing::debug!(
                        leader_content_id,
                        "Upserted detail leader into players collection"
                    );
                } else {
                    tracing::debug!(
                        leader_content_id,
                        requested = report.requested,
                        accepted = report.accepted,
                        updated = report.updated,
                        invalid = report.invalid,
                        failed = report.failed,
                        "Detail leader upsert finished with issues"
                    );
                }
            }
            Err(error) => {
                tracing::error!(
                    leader_content_id,
                    error = ?error,
                    "Failed to upsert detail leader"
                );
            }
        }
    });
}

async fn apply_detail_update(
    state: Arc<State>,
    detail: UploadablePartyDetail,
) -> anyhow::Result<DetailBatchItemResponse> {
    log_detail_payload(&detail);
    spawn_detail_leader_upsert_if_valid(&state, &detail);

    let listing_id = detail.listing_id;
    let set_doc = build_detail_update_doc(&detail);
    let result = state
        .collection()
        .update_one(
            doc! { "listing.id": listing_id },
            doc! { "$set": set_doc },
            None,
        )
        .await
        .with_context(|| format!("failed to update listing detail {listing_id}"))?;

    tracing::debug!(
        listing_id,
        matched_count = result.matched_count,
        modified_count = result.modified_count,
        "updated listing members",
    );

    Ok(DetailBatchItemResponse {
        listing_id,
        status: if result.matched_count == 0 {
            "missing"
        } else {
            "ok"
        },
        matched_count: result.matched_count,
        modified_count: result.modified_count,
        message: None,
    })
}

pub async fn contribute_detail_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    detail: UploadablePartyDetail,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeDetail,
        &headers,
        remote_addr,
        "POST",
        "/contribute/detail",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let resource_id = format!("listing:{}", detail.listing_id);
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::Detail,
        Some(&resource_id),
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    if let Some(message) = detail_payload_is_too_large(&detail, state.max_detail_member_count) {
        return Ok(
            warp::reply::with_status(message, StatusCode::PAYLOAD_TOO_LARGE).into_response(),
        );
    }

    match apply_detail_update(Arc::clone(&state), detail).await {
        Ok(result) => {
            if result.modified_count > 0 {
                state.notify_listings_changed(result.modified_count as usize);
            }
            Ok(warp::reply::json(&ContributeDetailResponse {
                status: result.status,
                matched_count: result.matched_count,
                modified_count: result.modified_count,
            })
            .into_response())
        }
        Err(error) => {
            tracing::warn!(
                error = ?error,
                "failed to update listing members"
            );
            Ok(warp::reply::with_status(
                "failed to update listing detail",
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

pub async fn contribute_detail_batch_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    items: Vec<UploadablePartyDetailBatchItem>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let total = items.len();
    let guard_ctx = match ingest_guard::authenticate_request(
        &state,
        IngestEndpoint::ContributeDetail,
        &headers,
        remote_addr,
        "POST",
        "/contribute/detail/batch",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    if total > state.max_detail_batch_size {
        return Ok(warp::reply::with_status(
            "too many detail items in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let mut ordered_results: Vec<Option<DetailBatchItemResponse>> =
        (0..total).map(|_| None).collect();
    let mut accepted_items = Vec::with_capacity(total);

    for (index, item) in items.into_iter().enumerate() {
        if let Some(result) = preflight_detail_batch_item(&state, &guard_ctx.client_id, &item) {
            ordered_results[index] = Some(result);
        } else {
            accepted_items.push((index, item.detail));
        }
    }

    if let Err(error) = ingest_guard::reserve_rate_limit_units(
        &state,
        IngestEndpoint::ContributeDetail,
        &guard_ctx.request_identity,
        accepted_items.len().max(1),
    )
    .await
    {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let mut writes =
        futures_util::stream::iter(accepted_items.into_iter().map(|(index, detail)| {
            let state = Arc::clone(&state);
            async move {
                let listing_id = detail.listing_id;
                match apply_detail_update(state, detail).await {
                    Ok(result) => (index, result),
                    Err(error) => {
                        tracing::warn!(
                            listing_id,
                            error = ?error,
                            "failed to update listing members"
                        );
                        (
                            index,
                            DetailBatchItemResponse {
                                listing_id,
                                status: "failed",
                                matched_count: 0,
                                modified_count: 0,
                                message: Some("failed to update listing detail"),
                            },
                        )
                    }
                }
            }
        }))
        .buffer_unordered(state.listing_upsert_concurrency.max(1));

    while let Some((index, result)) = writes.next().await {
        ordered_results[index] = Some(result);
    }

    let results = ordered_results
        .into_iter()
        .map(|result| result.expect("batch item must produce exactly one result"))
        .collect::<Vec<_>>();
    let modified_count = results
        .iter()
        .map(|result| result.modified_count)
        .sum::<u64>();
    if modified_count > 0 {
        state.notify_listings_changed(modified_count as usize);
    }

    let response = build_detail_batch_response(results);
    Ok(warp::reply::json(&response).into_response())
}

#[cfg(test)]
mod tests;
