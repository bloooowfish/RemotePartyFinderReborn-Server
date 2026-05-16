use super::{
    build_detail_batch_response, build_detail_leader_player_if_valid, build_detail_update_doc,
    build_listing_upsert, contribute_detail_batch_handler, detail_payload_is_too_large,
    preflight_detail_batch_item, DetailBatchItemResponse, UploadablePartyDetail,
    UploadablePartyDetailBatchItem,
};
use chrono::Utc;
use mongodb::bson::doc;
use std::sync::Arc;
use warp::http::{HeaderMap, StatusCode};

fn sample_detail(listing_id: u32) -> UploadablePartyDetail {
    UploadablePartyDetail {
        listing_id,
        leader_content_id: 2,
        leader_name: "Leader".to_string(),
        home_world: 73,
        member_content_ids: vec![2],
        member_jobs: Some(vec![37]),
        slot_flags: Some(vec!["0x0000000000000001".to_string()]),
    }
}

fn batch_item(
    detail: UploadablePartyDetail,
    capability_token: Option<String>,
) -> UploadablePartyDetailBatchItem {
    UploadablePartyDetailBatchItem {
        detail,
        capability_token,
    }
}

async fn test_state_with_signature_required() -> Arc<crate::web::State> {
    let mongo = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .expect("construct test mongo client");
    let (change_tx, _) = tokio::sync::broadcast::channel(1);

    Arc::new(crate::web::State {
        mongo,
        stats: Default::default(),
        listings_change_channel: change_tx,
        listings_revision: Default::default(),
        listings_revision_pending: Default::default(),
        listings_revision_notify: tokio::sync::Notify::new(),
        listings_revision_coalesce_window: std::time::Duration::from_millis(1),
        listings_revision_webhook: None,
        listings_snapshot_cache: Default::default(),
        listings_snapshot_source: crate::config::ListingsSnapshotSource::Inline,
        listings_snapshot_collection_name: "listings_snapshots".to_string(),
        listings_snapshot_document_id: "current".to_string(),
        listing_source_state_collection_name: "listing_source_state".to_string(),
        listing_source_state_document_id: "current".to_string(),
        listing_snapshot_revision_state_collection_name: "listing_snapshot_revision_state"
            .to_string(),
        materialized_snapshot_reconcile_interval_seconds: 30,
        snapshot_refresh_shared_secret: String::new(),
        snapshot_refresh_client_id: "listings-snapshot-refresh".to_string(),
        snapshot_refresh_clock_skew_seconds: 300,
        snapshot_refresh_nonce_ttl_seconds: 300,
        fflogs_jobs_limit: 1,
        fflogs_hidden_cache_ttl_hours: 24,
        fflogs_active_parse_refresh_minutes: 15,
        parse_cache_ttl_hours: 24,
        fflogs_non_fetch_failure_abandon_threshold: 2,
        listing_upsert_concurrency: 1,
        player_upsert_concurrency: 1,
        max_body_bytes_contribute: 1024,
        max_body_bytes_multiple: 1024,
        max_body_bytes_players: 1024,
        max_body_bytes_detail: 1024,
        max_body_bytes_detail_batch: 1024,
        max_body_bytes_fflogs_results: 1024,
        max_multiple_batch_size: 10,
        max_players_batch_size: 10,
        max_fflogs_results_batch_size: 10,
        max_detail_batch_size: 10,
        max_detail_member_count: 8,
        ingest_security: crate::web::IngestSecurityConfig {
            require_signature: true,
            shared_secret: "test-shared-secret".to_string(),
            clock_skew_seconds: 300,
            nonce_ttl_seconds: 300,
            require_capabilities_for_protected_endpoints: true,
            capability_secret: "test-capability-secret".to_string(),
            capability_session_ttl_seconds: 300,
            capability_detail_ttl_seconds: 300,
            rate_limit_prefer_client_id: false,
            rate_limits: crate::web::IngestRateLimits {
                contribute_per_minute: 60,
                multiple_per_minute: 60,
                players_per_minute: 60,
                detail_per_minute: 60,
                fflogs_jobs_per_minute: 60,
                fflogs_results_per_minute: 60,
            },
        },
        ingest_rate_windows: Default::default(),
        ingest_nonces: Default::default(),
        snapshot_refresh_nonces: Default::default(),
        fflogs_job_leases: Default::default(),
        fflogs_jobs_dispatched_total: Default::default(),
        fflogs_results_received_total: Default::default(),
        fflogs_results_rejected_total: Default::default(),
        fflogs_leases_abandoned_total: Default::default(),
        fflogs_leases_abandon_rejected_total: Default::default(),
        fflogs_hidden_refresh_total: Default::default(),
        fflogs_leader_fallback_total: Default::default(),
        monitor_snapshot_interval_seconds: 0,
    })
}

async fn test_state_with_open_ingest() -> Arc<crate::web::State> {
    test_state_with_open_ingest_options("mongodb://127.0.0.1:27017", 60).await
}

async fn test_state_with_open_ingest_mongo_uri(mongo_uri: &str) -> Arc<crate::web::State> {
    test_state_with_open_ingest_options(mongo_uri, 60).await
}

async fn test_state_with_open_ingest_options(
    mongo_uri: &str,
    detail_per_minute: u32,
) -> Arc<crate::web::State> {
    let mongo = mongodb::Client::with_uri_str(mongo_uri)
        .await
        .expect("construct test mongo client");
    let (change_tx, _) = tokio::sync::broadcast::channel(1);

    Arc::new(crate::web::State {
        mongo,
        stats: Default::default(),
        listings_change_channel: change_tx,
        listings_revision: Default::default(),
        listings_revision_pending: Default::default(),
        listings_revision_notify: tokio::sync::Notify::new(),
        listings_revision_coalesce_window: std::time::Duration::from_millis(1),
        listings_revision_webhook: None,
        listings_snapshot_cache: Default::default(),
        listings_snapshot_source: crate::config::ListingsSnapshotSource::Inline,
        listings_snapshot_collection_name: "listings_snapshots".to_string(),
        listings_snapshot_document_id: "current".to_string(),
        listing_source_state_collection_name: "listing_source_state".to_string(),
        listing_source_state_document_id: "current".to_string(),
        listing_snapshot_revision_state_collection_name: "listing_snapshot_revision_state"
            .to_string(),
        materialized_snapshot_reconcile_interval_seconds: 30,
        snapshot_refresh_shared_secret: String::new(),
        snapshot_refresh_client_id: "listings-snapshot-refresh".to_string(),
        snapshot_refresh_clock_skew_seconds: 300,
        snapshot_refresh_nonce_ttl_seconds: 300,
        fflogs_jobs_limit: 1,
        fflogs_hidden_cache_ttl_hours: 24,
        fflogs_active_parse_refresh_minutes: 15,
        parse_cache_ttl_hours: 24,
        fflogs_non_fetch_failure_abandon_threshold: 2,
        listing_upsert_concurrency: 1,
        player_upsert_concurrency: 1,
        max_body_bytes_contribute: 1024,
        max_body_bytes_multiple: 1024,
        max_body_bytes_players: 1024,
        max_body_bytes_detail: 1024,
        max_body_bytes_detail_batch: 1024,
        max_body_bytes_fflogs_results: 1024,
        max_multiple_batch_size: 10,
        max_players_batch_size: 10,
        max_fflogs_results_batch_size: 10,
        max_detail_batch_size: 10,
        max_detail_member_count: 8,
        ingest_security: crate::web::IngestSecurityConfig {
            require_signature: false,
            shared_secret: "test-shared-secret".to_string(),
            clock_skew_seconds: 300,
            nonce_ttl_seconds: 300,
            require_capabilities_for_protected_endpoints: false,
            capability_secret: "test-capability-secret".to_string(),
            capability_session_ttl_seconds: 300,
            capability_detail_ttl_seconds: 300,
            rate_limit_prefer_client_id: false,
            rate_limits: crate::web::IngestRateLimits {
                contribute_per_minute: 60,
                multiple_per_minute: 60,
                players_per_minute: 60,
                detail_per_minute,
                fflogs_jobs_per_minute: 60,
                fflogs_results_per_minute: 60,
            },
        },
        ingest_rate_windows: Default::default(),
        ingest_nonces: Default::default(),
        snapshot_refresh_nonces: Default::default(),
        fflogs_job_leases: Default::default(),
        fflogs_jobs_dispatched_total: Default::default(),
        fflogs_results_received_total: Default::default(),
        fflogs_results_rejected_total: Default::default(),
        fflogs_leases_abandoned_total: Default::default(),
        fflogs_leases_abandon_rejected_total: Default::default(),
        fflogs_hidden_refresh_total: Default::default(),
        fflogs_leader_fallback_total: Default::default(),
        monitor_snapshot_interval_seconds: 0,
    })
}

#[test]
fn build_detail_update_doc_includes_raw_slot_flags() {
    let detail = UploadablePartyDetail {
        listing_id: 1,
        leader_content_id: 2,
        leader_name: "Leader".to_string(),
        home_world: 73,
        member_content_ids: vec![11, 0, 22],
        member_jobs: Some(vec![37, 0, 24]),
        slot_flags: Some(vec![
            "0x0000000000000001".to_string(),
            "0x0000000000000000".to_string(),
            "0x0000000000000004".to_string(),
        ]),
    };

    let update = build_detail_update_doc(&detail);

    let slot_flags = update.get_array("listing.detail_slot_flags").unwrap();
    assert_eq!(slot_flags.len(), 3);
    assert_eq!(slot_flags[0].as_str(), Some("0x0000000000000001"));
    assert_eq!(slot_flags[1].as_str(), Some("0x0000000000000000"));
    assert_eq!(slot_flags[2].as_str(), Some("0x0000000000000004"));
}

#[test]
fn detail_leader_player_is_built_for_valid_leader_metadata() {
    let mut detail = sample_detail(1);
    detail.member_jobs = Some(vec![19]);
    detail.slot_flags = Some(vec!["0x1".to_string()]);

    let leader = build_detail_leader_player_if_valid(&detail).expect("valid leader");

    assert_eq!(leader.content_id, 2);
    assert_eq!(leader.name, "Leader");
    assert_eq!(leader.home_world, 73);
}

#[test]
fn detail_leader_player_is_skipped_when_metadata_is_invalid() {
    let detail = UploadablePartyDetail {
        listing_id: 1,
        leader_content_id: 0,
        leader_name: String::new(),
        home_world: 4000,
        member_content_ids: vec![0],
        member_jobs: Some(vec![0]),
        slot_flags: Some(vec!["0x0".to_string()]),
    };

    assert!(build_detail_leader_player_if_valid(&detail).is_none());
}

#[test]
fn detail_leader_player_is_skipped_when_name_is_whitespace() {
    let mut detail = sample_detail(1);
    detail.leader_name = "   ".to_string();
    detail.member_jobs = None;
    detail.slot_flags = None;

    assert!(build_detail_leader_player_if_valid(&detail).is_none());
}

#[test]
fn detail_batch_response_counts_terminal_and_transient_results() {
    let response = build_detail_batch_response(vec![
        DetailBatchItemResponse {
            listing_id: 1,
            status: "ok",
            matched_count: 1,
            modified_count: 1,
            message: None,
        },
        DetailBatchItemResponse {
            listing_id: 2,
            status: "missing",
            matched_count: 0,
            modified_count: 0,
            message: None,
        },
        DetailBatchItemResponse {
            listing_id: 3,
            status: "invalid",
            matched_count: 0,
            modified_count: 0,
            message: Some("too many members in request"),
        },
        DetailBatchItemResponse {
            listing_id: 4,
            status: "forbidden",
            matched_count: 0,
            modified_count: 0,
            message: Some("invalid capability token"),
        },
        DetailBatchItemResponse {
            listing_id: 5,
            status: "failed",
            matched_count: 0,
            modified_count: 0,
            message: Some("failed to update listing detail"),
        },
    ]);

    assert_eq!(response.status, "partial");
    assert_eq!(response.requested, 5);
    assert_eq!(response.accepted, 3);
    assert_eq!(response.applied, 1);
    assert_eq!(response.missing, 1);
    assert_eq!(response.failed, 1);
    assert_eq!(response.results.len(), 5);
}

#[tokio::test]
async fn detail_batch_handler_rejects_too_many_items() {
    let state = test_state_with_open_ingest().await;
    let items = (0..=state.max_detail_batch_size)
        .map(|index| batch_item(sample_detail(index as u32 + 1), None))
        .collect::<Vec<_>>();

    let response = contribute_detail_batch_handler(state, HeaderMap::new(), None, items)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn detail_batch_handler_returns_one_result_for_invalid_and_forbidden_items() {
    let state = test_state_with_signature_required().await;
    let headers = crate::web::test_support::signed_ingest_headers(
        "test-shared-secret",
        "batch-client",
        "detail-batch-terminal-results",
        "POST",
        "/contribute/detail/batch",
    );

    let mut invalid_detail = sample_detail(10);
    invalid_detail.member_content_ids = vec![1; state.max_detail_member_count + 1];

    let wrong_resource_token = {
        let now = Utc::now().timestamp();
        let claims = crate::web::ingest_guard::CapabilityClaims::new(
            "batch-client".to_string(),
            crate::web::ingest_guard::CapabilityScope::Detail,
            Some("listing:999".to_string()),
            now,
            now + 300,
        );
        crate::web::ingest_guard::issue_capability_token(
            &state.ingest_security.capability_secret,
            &claims,
        )
        .unwrap()
    };

    let response = contribute_detail_batch_handler(
        state,
        headers,
        None,
        vec![
            batch_item(invalid_detail, None),
            batch_item(sample_detail(11), Some(wrong_resource_token)),
        ],
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = warp::hyper::body::to_bytes(response.into_body())
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(value["status"], "ok");
    assert_eq!(value["requested"], 2);
    assert_eq!(value["accepted"], 0);
    assert_eq!(value["results"].as_array().unwrap().len(), 2);
    assert_eq!(value["results"][0]["status"], "invalid");
    assert_eq!(value["results"][1]["status"], "forbidden");
}

#[tokio::test]
async fn detail_batch_handler_reports_partial_when_an_accepted_write_fails() {
    let state =
        test_state_with_open_ingest_mongo_uri("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=10")
            .await;
    let mut detail = sample_detail(21);
    detail.leader_content_id = 0;
    detail.leader_name = String::new();
    detail.home_world = 4000;

    let response = contribute_detail_batch_handler(
        state,
        HeaderMap::new(),
        None,
        vec![batch_item(detail, None)],
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = warp::hyper::body::to_bytes(response.into_body())
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(value["status"], "partial");
    assert_eq!(value["requested"], 1);
    assert_eq!(value["accepted"], 1);
    assert_eq!(value["failed"], 1);
    assert_eq!(value["results"].as_array().unwrap().len(), 1);
    assert_eq!(value["results"][0]["listing_id"], 21);
    assert_eq!(value["results"][0]["status"], "failed");
}

#[tokio::test]
async fn detail_batch_handler_debits_detail_rate_limit_per_item() {
    let state = test_state_with_open_ingest_options(
        "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=10",
        2,
    )
    .await;
    let items = (31..=33)
        .map(|listing_id| {
            let mut detail = sample_detail(listing_id);
            detail.leader_content_id = 0;
            detail.leader_name = String::new();
            detail.home_world = 4000;
            batch_item(detail, None)
        })
        .collect::<Vec<_>>();

    let response = contribute_detail_batch_handler(state, HeaderMap::new(), None, items)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn detail_batch_rate_limit_rejection_does_not_consume_detail_quota() {
    let state = test_state_with_open_ingest_options(
        "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=10",
        1,
    )
    .await;
    let items = (41..=42)
        .map(|listing_id| {
            let mut detail = sample_detail(listing_id);
            detail.leader_content_id = 0;
            detail.leader_name = String::new();
            detail.home_world = 4000;
            batch_item(detail, None)
        })
        .collect::<Vec<_>>();

    let batch_response =
        contribute_detail_batch_handler(Arc::clone(&state), HeaderMap::new(), None, items)
            .await
            .unwrap();

    assert_eq!(batch_response.status(), StatusCode::TOO_MANY_REQUESTS);

    let mut oversized_detail = sample_detail(43);
    oversized_detail.member_content_ids = vec![1; state.max_detail_member_count + 1];
    let single_response =
        super::contribute_detail_handler(state, HeaderMap::new(), None, oversized_detail)
            .await
            .unwrap();

    assert_eq!(single_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn detail_batch_invalid_items_do_not_consume_extra_detail_quota() {
    let state = test_state_with_open_ingest_options(
        "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=10",
        2,
    )
    .await;
    let items = (51..=52)
        .map(|listing_id| {
            let mut detail = sample_detail(listing_id);
            detail.member_content_ids = vec![1; state.max_detail_member_count + 1];
            batch_item(detail, None)
        })
        .collect::<Vec<_>>();

    let batch_response =
        contribute_detail_batch_handler(Arc::clone(&state), HeaderMap::new(), None, items)
            .await
            .unwrap();

    assert_eq!(batch_response.status(), StatusCode::OK);

    let mut oversized_detail = sample_detail(53);
    oversized_detail.member_content_ids = vec![1; state.max_detail_member_count + 1];
    let single_response =
        super::contribute_detail_handler(state, HeaderMap::new(), None, oversized_detail)
            .await
            .unwrap();

    assert_eq!(single_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn oversized_detail_batch_does_not_consume_detail_quota() {
    let state = test_state_with_open_ingest_options(
        "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=10",
        1,
    )
    .await;
    let items = (0..=state.max_detail_batch_size)
        .map(|index| batch_item(sample_detail(index as u32 + 61), None))
        .collect::<Vec<_>>();

    let batch_response =
        contribute_detail_batch_handler(Arc::clone(&state), HeaderMap::new(), None, items)
            .await
            .unwrap();

    assert_eq!(batch_response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let mut oversized_detail = sample_detail(70);
    oversized_detail.member_content_ids = vec![1; state.max_detail_member_count + 1];
    let single_response =
        super::contribute_detail_handler(state, HeaderMap::new(), None, oversized_detail)
            .await
            .unwrap();

    assert_eq!(single_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn detail_batch_preflight_accepts_valid_capability_and_rejects_wrong_listing_resource() {
    let state = test_state_with_signature_required().await;
    let now = Utc::now().timestamp();
    let valid_claims = crate::web::ingest_guard::CapabilityClaims::new(
        "batch-client".to_string(),
        crate::web::ingest_guard::CapabilityScope::Detail,
        Some("listing:12".to_string()),
        now,
        now + 300,
    );
    let valid_token = crate::web::ingest_guard::issue_capability_token(
        &state.ingest_security.capability_secret,
        &valid_claims,
    )
    .unwrap();
    let valid_item = batch_item(sample_detail(12), Some(valid_token));

    assert!(preflight_detail_batch_item(&state, "batch-client", &valid_item).is_none());

    let wrong_claims = crate::web::ingest_guard::CapabilityClaims::new(
        "batch-client".to_string(),
        crate::web::ingest_guard::CapabilityScope::Detail,
        Some("listing:99".to_string()),
        now,
        now + 300,
    );
    let wrong_token = crate::web::ingest_guard::issue_capability_token(
        &state.ingest_security.capability_secret,
        &wrong_claims,
    )
    .unwrap();
    let wrong_item = batch_item(sample_detail(12), Some(wrong_token));

    let result =
        preflight_detail_batch_item(&state, "batch-client", &wrong_item).expect("forbidden");

    assert_eq!(result.status, "forbidden");
    assert_eq!(result.listing_id, 12);
}

#[test]
fn build_listing_upsert_does_not_overwrite_detail_managed_slot_flags() {
    let mut listing = crate::web::test_support::sample_listing();
    listing.detail_slot_flags = vec!["0x00000000D99DDAB4".to_string(); 8];

    let (_, _, update) = build_listing_upsert(&listing).expect("upsert should build");
    let set_doc = update[0].get_document("$set").expect("set stage");
    let listing_doc = set_doc.get_document("listing").expect("listing document");

    assert!(!listing_doc.contains_key("detail_slot_flags"));
    assert!(!listing_doc.contains_key("member_content_ids"));
    assert!(!listing_doc.contains_key("member_jobs"));
    assert!(!listing_doc.contains_key("leader_content_id"));
}

#[test]
fn build_listing_upsert_sets_expires_at_from_seconds_remaining() {
    let mut listing = crate::web::test_support::sample_listing();
    listing.seconds_remaining = 1800;

    let (_, _, update) = build_listing_upsert(&listing).expect("upsert should build");
    let set_doc = update[0].get_document("$set").expect("set stage");

    assert_eq!(
        set_doc.get_document("expires_at"),
        Ok(&doc! {
            "$dateAdd": {
                "startDate": "$$NOW",
                "unit": "second",
                "amount": 1800_i64,
            }
        })
    );
}

#[test]
fn build_listing_upsert_preserves_false_high_end_telemetry_for_stale_clear() {
    let mut listing = crate::web::test_support::sample_listing();
    listing.pf_category_high_end = false;
    listing.cfc_excel_high_end = false;
    listing.client_high_end_duty = false;

    let (_, _, update) = build_listing_upsert(&listing).expect("upsert should build");
    let set_doc = update[0].get_document("$set").expect("set stage");
    let listing_stage = set_doc.get_document("listing").expect("listing stage");
    let merge_objects = listing_stage
        .get_array("$mergeObjects")
        .expect("listing upsert should merge the new listing document");
    let listing_doc = merge_objects[1]
        .as_document()
        .expect("second merge object should be the uploaded listing");

    assert_eq!(listing_doc.get_bool("pf_category_high_end"), Ok(false));
    assert_eq!(listing_doc.get_bool("cfc_excel_high_end"), Ok(false));
    assert_eq!(listing_doc.get_bool("client_high_end_duty"), Ok(false));
}

#[test]
fn detail_payload_is_too_large_rejects_excess_slot_flags() {
    let detail = UploadablePartyDetail {
        listing_id: 1,
        leader_content_id: 2,
        leader_name: "Leader".to_string(),
        home_world: 73,
        member_content_ids: vec![11, 0, 22],
        member_jobs: Some(vec![37, 0, 24]),
        slot_flags: Some(vec![
            "0x1".to_string(),
            "0x2".to_string(),
            "0x3".to_string(),
            "0x4".to_string(),
        ]),
    };

    assert_eq!(
        detail_payload_is_too_large(&detail, 3),
        Some("too many slot flags in request")
    );
}

#[test]
fn identity_endpoint_rejects_empty_name_or_invalid_world() {
    let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let empty_name = crate::player::UploadableCharacterIdentity {
        content_id: 4001,
        name: "   ".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at,
    };
    let invalid_world = crate::player::UploadableCharacterIdentity {
        content_id: 4002,
        name: "Alpha Beta".to_string(),
        home_world: 4_000,
        world_name: "Nowhere".to_string(),
        source: "chara_card".to_string(),
        observed_at,
    };

    assert!(matches!(
        crate::mongo::prepare_character_identity_upsert(&empty_name, observed_at, 300),
        Err(crate::mongo::CharacterIdentityRejectReason::MissingName)
    ));
    assert!(matches!(
        crate::mongo::prepare_character_identity_upsert(&invalid_world, observed_at, 300),
        Err(crate::mongo::CharacterIdentityRejectReason::InvalidHomeWorld)
    ));
}

#[test]
fn identity_upsert_preserves_newer_existing_player_data_when_enrichment_is_stale() {
    let existing_observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T13:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let stale_observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let existing = crate::player::Player {
        content_id: 6006,
        name: "Fresh Name".to_string(),
        home_world: 79,
        current_world: 79,
        last_seen: existing_observed_at,
        seen_count: 3,
        account_id: "12345".to_string(),
    };
    let stale_identity = crate::player::UploadableCharacterIdentity {
        content_id: 6006,
        name: "Stale Name".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "party_detail".to_string(),
        observed_at: stale_observed_at,
    };

    let merged = crate::player::merge_identity_into_player(
        Some(&existing),
        Some(existing_observed_at),
        &stale_identity,
        existing_observed_at + chrono::TimeDelta::minutes(1),
    );

    assert!(!merged.applied_incoming_identity);
    assert_eq!(merged.player.name, "Fresh Name");
    assert_eq!(merged.player.home_world, 79);
    assert_eq!(merged.player.current_world, 79);
}

#[tokio::test]
async fn identity_endpoint_requires_standard_ingest_authorization() {
    let state = test_state_with_signature_required().await;
    let payload = vec![crate::player::UploadableCharacterIdentity {
        content_id: 7007,
        name: "Authorized Name".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at: chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
            .unwrap()
            .with_timezone(&Utc),
    }];

    let response =
        super::contribute_character_identity_handler(state, HeaderMap::new(), None, payload)
            .await
            .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn identity_endpoint_rejects_future_observed_at_beyond_clock_skew() {
    let state = test_state_with_open_ingest().await;
    let payload = vec![crate::player::UploadableCharacterIdentity {
        content_id: 7008,
        name: "Future Name".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at: Utc::now() + chrono::TimeDelta::seconds(301),
    }];

    let response =
        super::contribute_character_identity_handler(state, HeaderMap::new(), None, payload)
            .await
            .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
