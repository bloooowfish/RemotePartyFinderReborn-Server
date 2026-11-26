use anyhow::Context;
use chrono::Utc;
use futures_util::StreamExt;
use mongodb::bson::{Document, doc};
#[cfg(test)]
use std::net::SocketAddr;
use std::{collections::HashSet, convert::Infallible, sync::Arc};
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use crate::listing::PartyFinderListing;
use crate::mongo::{insert_listing, upsert_character_identities, upsert_players};
use crate::player::{UploadableCharacterIdentity, UploadablePlayer};

use super::ingest_guard::{self, CapabilityClaims, CapabilityScope, GuardContext, IngestEndpoint};
use super::State;

#[derive(Debug, serde::Serialize)]
struct ContributeResponse {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail_capability: Option<ListingDetailCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protected_endpoints: Option<ProtectedEndpointCapabilities>,
}

pub(crate) async fn contribute_handler_with_guard(
    state: Arc<State>,
    guard_ctx: GuardContext,
    listing: PartyFinderListing,
) -> std::result::Result<warp::reply::Response, Infallible> {
    if !is_listing_acceptable(&listing) {
        return Ok(
            warp::reply::with_status("invalid listing", StatusCode::BAD_REQUEST).into_response(),
        );
    }

    match insert_listing(state.collection(), &listing).await {
        Ok(_) => {
            if let Err(error) = crate::mongo::upsert_listing_stats_history(
                state.listing_stats_history_collection(),
                &listing,
            )
            .await
            {
                tracing::warn!(
                    listing_id = listing.id,
                    created_world = listing.created_world,
                    error = ?error,
                    "failed to upsert listing_stats_history for contributed listing"
                );
            }
            state.notify_listings_changed(1);

            let expires_at =
                Utc::now().timestamp() + state.ingest_security.capability_detail_ttl_seconds;
            let response = ContributeResponse {
                status: "ok",
                detail_capability: issue_detail_capability(
                    &state,
                    &guard_ctx.client_id,
                    listing.id,
                    listing.created_world,
                    expires_at,
                ),
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };
            Ok(warp::reply::with_status(warp::reply::json(&response), StatusCode::OK).into_response())
        }
        Err(error) => {
            tracing::error!(
                listing_id = listing.id,
                created_world = listing.created_world,
                error = ?error,
                "failed to upsert listing in /contribute"
            );

            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({
                    "status": "error",
                    "error": "failed to store listing",
                })),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

const MAX_LISTING_VECTOR_LEN: usize = 48;
const MAX_SLOT_FLAG_LEN: usize = 18;
const MAX_DETAIL_OBSERVED_FUTURE_SKEW_MS: i64 = 300_000;

fn is_valid_listing_id(id: u64) -> bool {
    id != 0 && crate::mongo::content_id_to_i64(id).is_some()
}

fn is_listing_acceptable(listing: &PartyFinderListing) -> bool {
    if !is_valid_listing_id(listing.id) {
        return false;
    }

    if listing.seconds_remaining > 60 * 60 {
        return false;
    }

    if !crate::mongo::is_known_world_id(listing.created_world)
        || !crate::mongo::is_known_or_zero_world_id(listing.home_world)
        || !crate::mongo::is_known_or_zero_world_id(listing.current_world)
    {
        return false;
    }

    if crate::mongo::content_id_to_i64(listing.leader_content_id).is_none()
        || listing.member_content_ids.iter().any(|&id| id < 0)
    {
        return false;
    }

    listing.slots.len() <= MAX_LISTING_VECTOR_LEN
        && listing.jobs_present.len() <= MAX_LISTING_VECTOR_LEN
        && listing.member_content_ids.len() <= MAX_LISTING_VECTOR_LEN
        && listing.member_jobs.len() <= MAX_LISTING_VECTOR_LEN
        && listing.detail_slot_flags.len() <= MAX_LISTING_VECTOR_LEN
        && listing
            .detail_slot_flags
            .iter()
            .all(|flag| flag.len() <= MAX_SLOT_FLAG_LEN)
}

fn build_listing_upsert(
    listing: &PartyFinderListing,
) -> anyhow::Result<(u64, Document, Vec<Document>)> {
    let (filter, update) = crate::mongo::build_listing_upsert_doc(listing)?;
    Ok((listing.id, filter, update))
}

struct PreparedListingBatch<'a> {
    listings: Vec<&'a PartyFinderListing>,
    items: Vec<crate::mongo::BatchUpdateItem>,
}

fn prepare_listing_batch_upserts<'a>(
    listings: &'a [PartyFinderListing],
) -> PreparedListingBatch<'a> {
    let mut key_to_pos: std::collections::HashMap<(u64, u32, u16), usize> =
        std::collections::HashMap::with_capacity(listings.len());
    let mut prepared_listings: Vec<&'a PartyFinderListing> = Vec::with_capacity(listings.len());
    let mut items = Vec::with_capacity(listings.len());

    for listing in listings {
        if !is_listing_acceptable(listing) {
            continue;
        }

        let Ok((_, filter, update)) = build_listing_upsert(listing) else {
            tracing::debug!("Failed to serialize listing {}", listing.id);
            continue;
        };

        let item = crate::mongo::BatchUpdateItem::upsert_pipeline(filter, update);
        let key = (listing.id, listing.last_server_restart, listing.created_world);
        if let Some(&pos) = key_to_pos.get(&key) {
            prepared_listings[pos] = listing;
            items[pos] = item;
        } else {
            key_to_pos.insert(key, prepared_listings.len());
            prepared_listings.push(listing);
            items.push(item);
        }
    }

    PreparedListingBatch {
        listings: prepared_listings,
        items,
    }
}

pub(crate) async fn contribute_multiple_handler_with_guard(
    state: Arc<State>,
    guard_ctx: GuardContext,
    raw_listings: Vec<serde_json::Value>,
) -> std::result::Result<warp::reply::Response, Infallible> {

    let total = raw_listings.len();
    if total > state.max_multiple_batch_size {
        return Ok(warp::reply::with_status(
            "too many listings in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let mut listings = Vec::with_capacity(total);
    let mut deserialize_failures = 0usize;
    for raw_listing in raw_listings {
        match serde_json::from_value::<PartyFinderListing>(raw_listing) {
            Ok(listing) => listings.push(listing),
            Err(error) => {
                deserialize_failures += 1;
                tracing::debug!(
                    error = ?error,
                    "Skipping undeserializable listing in /contribute/multiple"
                );
            }
        }
    }

    let prepared = prepare_listing_batch_upserts(&listings);
    let upsert_total = prepared.items.len();
    let mut successful = 0usize;
    let failed_writes;
    let mut write_failed = false;
    let mut successful_listings: Vec<&PartyFinderListing> = Vec::new();
    let mut successful_listing_keys: Vec<(u64, u16)> = Vec::new();

    match crate::mongo::run_batched_updates(&state.collection(), &prepared.items, true).await {
        Ok(summary) => {
            for failure in &summary.failures {
                let listing_id = prepared
                    .listings
                    .get(failure.item_index)
                    .map(|l| l.id)
                    .unwrap_or_default();
                tracing::debug!(
                    listing_id,
                    code = failure.code,
                    error = %failure.errmsg,
                    "Failed to insert listing in batch"
                );
            }

            if let Some(wc_err) = summary.write_concern_error.as_deref() {
                write_failed = true;
                failed_writes = upsert_total;
                tracing::warn!(
                    requested_total = total,
                    attempted_upserts = upsert_total,
                    error = wc_err,
                    "writeConcernError in /contribute/multiple"
                );
            } else {
                for (idx, listing) in prepared.listings.iter().enumerate() {
                    if summary.succeeded_indices.contains(&idx) {
                        successful += 1;
                        successful_listings.push(*listing);
                        successful_listing_keys.push((listing.id, listing.created_world));
                    }
                }
                failed_writes = summary
                    .failures
                    .len()
                    .max(upsert_total.saturating_sub(successful));
                if failed_writes > 0 || (upsert_total > 0 && successful < upsert_total) {
                    write_failed = true;
                }
            }
        }
        Err(error) => {
            write_failed = true;
            failed_writes = upsert_total;
            tracing::warn!(
                requested_total = total,
                attempted_upserts = upsert_total,
                error = ?error,
                "Failed batched listings upsert in /contribute/multiple"
            );
        }
    }

    let total_failed = failed_writes.saturating_add(deserialize_failures);
    if total_failed > 0 {
        tracing::warn!(
            requested_total = total,
            attempted_upserts = upsert_total,
            successful_upserts = successful,
            deserialize_failures,
            failed_writes,
            "Processed /contribute/multiple request with failures"
        );
    }

    if !successful_listings.is_empty() {
        if let Err(error) = crate::mongo::upsert_listing_stats_histories_batch(
            state.listing_stats_history_collection(),
            &successful_listings,
        )
        .await
        {
            tracing::warn!(
                count = successful_listings.len(),
                error = ?error,
                "failed to batch upsert listing_stats_history in /contribute/multiple"
            );
        }
        state.notify_listings_changed(successful_listings.len());
    }
    let (status_code, status) = if write_failed {
        (StatusCode::INTERNAL_SERVER_ERROR, "write_failure")
    } else {
        (StatusCode::OK, "ok")
    };
    let response = ContributeMultipleResponse {
        status,
        requested: total,
        accepted: upsert_total,
        updated: successful,
        failed: total_failed,
        detail_capabilities: issue_detail_capabilities(
            &state,
            &guard_ctx.client_id,
            &successful_listing_keys,
        ),
        protected_endpoints: issue_protected_endpoint_capabilities(&state, &guard_ctx.client_id),
    };
    Ok(warp::reply::with_status(warp::reply::json(&response), status_code).into_response())
}

pub(crate) async fn contribute_players_handler_with_guard(
    state: Arc<State>,
    guard_ctx: GuardContext,
    players: Vec<UploadablePlayer>,
) -> std::result::Result<warp::reply::Response, Infallible> {

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

#[cfg(test)]
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

    contribute_character_identity_handler_with_guard(state, guard_ctx, identities).await
}

pub(crate) async fn contribute_character_identity_handler_with_guard(
    state: Arc<State>,
    guard_ctx: GuardContext,
    identities: Vec<UploadableCharacterIdentity>,
) -> std::result::Result<warp::reply::Response, Infallible> {

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
    #[serde(deserialize_with = "crate::listing::deserialize_listing_id")]
    pub listing_id: u64,
    pub created_world: u16,
    #[serde(default)]
    pub last_server_restart: u32,
    #[serde(default)]
    pub observed_at_unix_ms: i64,
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
    listing_id: u64,
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
    tomestone_jobs: ProtectedEndpointCapability,
    tomestone_results: ProtectedEndpointCapability,
    tomestone_leases_abandon: ProtectedEndpointCapability,
}

#[derive(Debug, serde::Serialize, Clone)]
struct ListingDetailCapability {
    listing_id: u64,
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

fn is_valid_detail_created_world(created_world: u16) -> bool {
    created_world != 0 && crate::ffxiv::WORLDS.contains_key(&u32::from(created_world))
}

fn is_valid_detail_observed_at_unix_ms(observed_at_unix_ms: i64, now_ms: i64) -> bool {
    observed_at_unix_ms >= 0
        && observed_at_unix_ms <= now_ms.saturating_add(MAX_DETAIL_OBSERVED_FUTURE_SKEW_MS)
}

fn push_or_clause(filter: &mut Document, or_branches: Vec<Document>) {
    if let Some(existing_and) = filter
        .get_mut("$and")
        .and_then(|value| value.as_array_mut())
    {
        existing_and.push(mongodb::bson::Bson::Document(doc! { "$or": or_branches }));
        return;
    }

    if let Some(existing_or) = filter.remove("$or") {
        filter.insert(
            "$and",
            vec![
                doc! { "$or": existing_or },
                doc! { "$or": or_branches },
            ],
        );
    } else {
        filter.insert("$or", or_branches);
    }
}

fn detail_update_filter_at(
    detail: &UploadablePartyDetail,
    now: chrono::DateTime<Utc>,
) -> anyhow::Result<Document> {
    let listing_id = crate::mongo::content_id_to_i64(detail.listing_id)
        .ok_or_else(|| anyhow::anyhow!("listing_id exceeds i64::MAX: {}", detail.listing_id))?;
    let mut filter = doc! {
        "listing.id": listing_id,
        "listing.created_world": u32::from(detail.created_world),
        "expires_at": { "$gte": mongodb::bson::DateTime::from_chrono(now) },
    };

    if detail.last_server_restart > 0 {
        filter.insert("listing.last_server_restart", detail.last_server_restart);
    } else {
        push_or_clause(
            &mut filter,
            vec![
                doc! { "listing.last_server_restart": 0_u32 },
                doc! { "listing.last_server_restart": mongodb::bson::Bson::Null },
                doc! { "listing.last_server_restart": { "$exists": false } },
            ],
        );
    }

    if detail.observed_at_unix_ms > 0 {
        push_or_clause(
            &mut filter,
            vec![
                doc! { "listing.detail_observed_at_unix_ms": { "$exists": false } },
                doc! { "listing.detail_observed_at_unix_ms": mongodb::bson::Bson::Null },
                doc! { "listing.detail_observed_at_unix_ms": { "$lt": detail.observed_at_unix_ms } },
            ],
        );
    }

    Ok(filter)
}

fn detail_update_filter(detail: &UploadablePartyDetail) -> anyhow::Result<Document> {
    detail_update_filter_at(detail, Utc::now())
}

fn detail_has_valid_content_ids(detail: &UploadablePartyDetail) -> bool {
    crate::mongo::content_id_to_i64(detail.leader_content_id).is_some()
        && detail
            .member_content_ids
            .iter()
            .all(|&id| crate::mongo::content_id_to_i64(id).is_some())
}

fn build_detail_update_doc(detail: &UploadablePartyDetail) -> anyhow::Result<Document> {
    let mut set_doc = Document::new();

    if detail.member_content_ids.iter().any(|id| *id != 0) {
        let member_ids_i64 = detail
            .member_content_ids
            .iter()
            .map(|id| {
                crate::mongo::content_id_to_i64(*id)
                    .ok_or_else(|| anyhow::anyhow!("member content_id exceeds i64::MAX: {id}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        set_doc.insert("listing.member_content_ids", member_ids_i64);
    }

    if detail.leader_content_id != 0 {
        let leader_id_i64 = crate::mongo::content_id_to_i64(detail.leader_content_id).ok_or_else(
            || anyhow::anyhow!("leader content_id exceeds i64::MAX: {}", detail.leader_content_id),
        )?;
        set_doc.insert("listing.leader_content_id", leader_id_i64);
    }

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        if !member_jobs.is_empty() && member_jobs.iter().any(|job| *job != 0) {
            let member_jobs_i32 = member_jobs
                .iter()
                .map(|job| i32::from(*job))
                .collect::<Vec<_>>();
            set_doc.insert("listing.member_jobs", member_jobs_i32);
        }
    }

    if let Some(slot_flags) = detail.slot_flags.as_ref() {
        if !slot_flags.is_empty() {
            set_doc.insert("listing.detail_slot_flags", slot_flags.clone());
        }
    }

    if !set_doc.is_empty() && detail.observed_at_unix_ms > 0 {
        set_doc.insert("listing.detail_observed_at_unix_ms", detail.observed_at_unix_ms);
    }

    Ok(set_doc)
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
        tomestone_jobs: issue_capability(
            state,
            client_id,
            CapabilityScope::TomestoneJobs,
            None,
            expires_at,
        )?,
        tomestone_results: issue_capability(
            state,
            client_id,
            CapabilityScope::TomestoneResults,
            None,
            expires_at,
        )?,
        tomestone_leases_abandon: issue_capability(
            state,
            client_id,
            CapabilityScope::TomestoneLeasesAbandon,
            None,
            expires_at,
        )?,
    })
}

fn detail_capability_resource_ids(listing_id: u64, created_world: u16) -> (String, String) {
    (
        format!("listing:{listing_id}:{created_world}"),
        format!("listing:{listing_id}"),
    )
}

fn issue_detail_capability(
    state: &State,
    client_id: &str,
    listing_id: u64,
    created_world: u16,
    expires_at: i64,
) -> Option<ListingDetailCapability> {
    let (resource_id, _) = detail_capability_resource_ids(listing_id, created_world);
    let capability = issue_capability(
        state,
        client_id,
        CapabilityScope::Detail,
        Some(resource_id),
        expires_at,
    )?;

    Some(ListingDetailCapability {
        listing_id,
        token: capability.token,
        expires_at: capability.expires_at,
    })
}

fn issue_detail_capabilities(
    state: &State,
    client_id: &str,
    listing_keys: &[(u64, u16)],
) -> Vec<ListingDetailCapability> {
    let expires_at = Utc::now().timestamp() + state.ingest_security.capability_detail_ttl_seconds;
    let mut deduped = HashSet::new();
    let mut issued = Vec::with_capacity(listing_keys.len());

    for &(listing_id, created_world) in listing_keys {
        if !deduped.insert((listing_id, created_world)) {
            continue;
        }

        if let Some(capability) =
            issue_detail_capability(state, client_id, listing_id, created_world, expires_at)
        {
            issued.push(capability);
        }
    }

    issued
}

fn detail_payload_is_too_large(
    detail: &UploadablePartyDetail,
    limit: usize,
) -> Option<&'static str> {
    let effective_limit = limit.min(MAX_LISTING_VECTOR_LEN);
    if detail.member_content_ids.len() > effective_limit {
        return Some("too many members in request");
    }

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        if member_jobs.len() > effective_limit {
            return Some("too many member jobs in request");
        }
    }

    if let Some(slot_flags) = detail.slot_flags.as_ref() {
        if slot_flags.len() > effective_limit {
            return Some("too many slot flags in request");
        }
        if slot_flags.iter().any(|flag| flag.len() > MAX_SLOT_FLAG_LEN) {
            return Some("slot flag exceeds max length");
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
    if !is_valid_listing_id(item.detail.listing_id) {
        return Some(detail_batch_terminal_result(
            &item.detail,
            "invalid",
            "invalid listing_id",
        ));
    }

    if !is_valid_detail_created_world(item.detail.created_world) {
        return Some(detail_batch_terminal_result(
            &item.detail,
            "invalid",
            "invalid created_world",
        ));
    }

    if !is_valid_detail_observed_at_unix_ms(
        item.detail.observed_at_unix_ms,
        Utc::now().timestamp_millis(),
    ) {
        return Some(detail_batch_terminal_result(
            &item.detail,
            "invalid",
            "invalid observed_at_unix_ms",
        ));
    }

    if let Some(message) = detail_payload_is_too_large(&item.detail, state.max_detail_member_count)
    {
        return Some(detail_batch_terminal_result(
            &item.detail,
            "invalid",
            message,
        ));
    }

    if !detail_has_valid_content_ids(&item.detail) {
        return Some(detail_batch_terminal_result(
            &item.detail,
            "invalid",
            "invalid content_id",
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
    let (composite_resource_id, legacy_resource_id) =
        detail_capability_resource_ids(item.detail.listing_id, item.detail.created_world);
    let now = Utc::now().timestamp();
    match ingest_guard::validate_capability_token(
        &state.ingest_security.capability_secret,
        token,
        CapabilityScope::Detail,
        client_id,
        Some(&composite_resource_id),
        now,
    ) {
        Ok(_) => None,
        Err(composite_err) => {
            match ingest_guard::validate_capability_token(
                &state.ingest_security.capability_secret,
                token,
                CapabilityScope::Detail,
                client_id,
                Some(&legacy_resource_id),
                now,
            ) {
                Ok(_) => None,
                Err(_) => Some(detail_batch_terminal_result(
                    &item.detail,
                    "forbidden",
                    guard_error_message(composite_err),
                )),
            }
        }
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
    let trimmed_name = detail.leader_name.trim();
    if detail.leader_content_id == 0
        || crate::mongo::content_id_to_i64(detail.leader_content_id).is_none()
        || trimmed_name.is_empty()
        || trimmed_name.chars().count() > crate::mongo::MAX_PLAYER_NAME_CHARS
        || !crate::mongo::is_known_or_zero_world_id(detail.home_world)
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

    let listing_id = detail.listing_id;
    if !is_valid_listing_id(detail.listing_id) {
        return Ok(DetailBatchItemResponse {
            listing_id,
            status: "invalid",
            matched_count: 0,
            modified_count: 0,
            message: Some("invalid listing_id"),
        });
    }

    if !is_valid_detail_created_world(detail.created_world) {
        return Ok(DetailBatchItemResponse {
            listing_id,
            status: "invalid",
            matched_count: 0,
            modified_count: 0,
            message: Some("invalid created_world"),
        });
    }

    if !is_valid_detail_observed_at_unix_ms(
        detail.observed_at_unix_ms,
        Utc::now().timestamp_millis(),
    ) {
        return Ok(DetailBatchItemResponse {
            listing_id,
            status: "invalid",
            matched_count: 0,
            modified_count: 0,
            message: Some("invalid observed_at_unix_ms"),
        });
    }

    if !detail_has_valid_content_ids(&detail) {
        return Ok(DetailBatchItemResponse {
            listing_id,
            status: "invalid",
            matched_count: 0,
            modified_count: 0,
            message: Some("invalid content_id"),
        });
    }

    let set_doc = build_detail_update_doc(&detail)?;
    if set_doc.is_empty() {
        return Ok(DetailBatchItemResponse {
            listing_id,
            status: "ok",
            matched_count: 0,
            modified_count: 0,
            message: None,
        });
    }

    let filter = detail_update_filter(&detail)?;
    let result = state
        .collection()
        .update_one(filter, doc! { "$set": set_doc }, None)
        .await
        .with_context(|| format!("failed to update listing detail {listing_id}"))?;

    if result.matched_count > 0 {
        spawn_detail_leader_upsert_if_valid(&state, &detail);
    }

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

#[cfg(test)]
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

    contribute_detail_handler_with_guard(state, headers, guard_ctx, detail).await
}

pub(crate) async fn contribute_detail_handler_with_guard(
    state: Arc<State>,
    headers: HeaderMap,
    guard_ctx: GuardContext,
    detail: UploadablePartyDetail,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let (composite_resource_id, legacy_resource_id) =
        detail_capability_resource_ids(detail.listing_id, detail.created_world);
    let cap_result = match ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::Detail,
        Some(&composite_resource_id),
    ) {
        Ok(claims) => Ok(claims),
        Err(composite_err) => ingest_guard::require_capability(
            &state,
            &headers,
            &guard_ctx.client_id,
            CapabilityScope::Detail,
            Some(&legacy_resource_id),
        )
        .map_err(|_| composite_err),
    };
    if let Err(error) = cap_result {
        if let Err(rate_err) = ingest_guard::record_capability_auth_failure(
            &state,
            IngestEndpoint::ContributeDetail,
            &guard_ctx,
        )
        .await
        {
            return Ok(ingest_guard::guard_error_reply(rate_err));
        }
        return Ok(ingest_guard::guard_error_reply(error));
    }

    if !is_valid_listing_id(detail.listing_id) {
        return Ok(
            warp::reply::with_status("invalid listing_id", StatusCode::BAD_REQUEST)
                .into_response(),
        );
    }

    if !is_valid_detail_created_world(detail.created_world) {
        return Ok(
            warp::reply::with_status("invalid created_world", StatusCode::BAD_REQUEST)
                .into_response(),
        );
    }

    if !is_valid_detail_observed_at_unix_ms(
        detail.observed_at_unix_ms,
        Utc::now().timestamp_millis(),
    ) {
        return Ok(
            warp::reply::with_status("invalid observed_at_unix_ms", StatusCode::BAD_REQUEST)
                .into_response(),
        );
    }

    if let Some(message) = detail_payload_is_too_large(&detail, state.max_detail_member_count) {
        return Ok(
            warp::reply::with_status(message, StatusCode::PAYLOAD_TOO_LARGE).into_response(),
        );
    }

    if !detail_has_valid_content_ids(&detail) {
        return Ok(
            warp::reply::with_status("invalid content_id", StatusCode::BAD_REQUEST)
                .into_response(),
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

#[cfg(test)]
pub async fn contribute_detail_batch_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    items: Vec<UploadablePartyDetailBatchItem>,
) -> std::result::Result<warp::reply::Response, Infallible> {
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

    contribute_detail_batch_handler_inner(state, guard_ctx, items, 0).await
}

pub(crate) async fn contribute_detail_batch_handler_with_guard(
    state: Arc<State>,
    guard_ctx: GuardContext,
    items: Vec<UploadablePartyDetailBatchItem>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    contribute_detail_batch_handler_inner(state, guard_ctx, items, 1).await
}

async fn contribute_detail_batch_handler_inner(
    state: Arc<State>,
    guard_ctx: GuardContext,
    items: Vec<UploadablePartyDetailBatchItem>,
    precharged_units: usize,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let total = items.len();
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
            if result.status == "forbidden" {
                if let Err(rate_err) = ingest_guard::record_capability_auth_failure(
                    &state,
                    IngestEndpoint::ContributeDetail,
                    &guard_ctx,
                )
                .await
                {
                    return Ok(ingest_guard::guard_error_reply(rate_err));
                }
            }
            ordered_results[index] = Some(result);
        } else {
            accepted_items.push((index, item.detail));
        }
    }

    let units_to_reserve = accepted_items.len().max(1).saturating_sub(precharged_units);
    if let Err(error) = ingest_guard::reserve_rate_limit_units(
        &state,
        IngestEndpoint::ContributeDetail,
        &guard_ctx,
        units_to_reserve,
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
