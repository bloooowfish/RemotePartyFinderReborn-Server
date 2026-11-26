use chrono::Utc;
use hmac::{Hmac, Mac};
use mongodb::Client;
use serde_json::json;
use sestring::payload::{AutoTranslatePayload, TextPayload};
use sestring::Payload;
use sestring::SeString;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use warp::http::{HeaderMap, HeaderValue, StatusCode};

use crate::config::ListingsSnapshotSource;
use crate::listing::{
    ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
    ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
};
use crate::listings_render::{ParseDisplay, ProgressDisplay, RenderableMember};
use crate::listings_snapshot::ApiReadableListing;
use crate::parse_resolver::ParseSource;
use crate::player::Player;
use crate::web::{
    CachedListingsSnapshot, IngestRateLimits, IngestSecurityConfig, ListingsRevisionWebhook,
    ListingsSnapshotCacheState, State,
};

use super::refresh::{
    apply_materialized_snapshot_if_newer, authorize_snapshot_refresh_request,
    reconcile_materialized_snapshot_once_with_loader, refresh_materialized_snapshot_with_loader,
    SnapshotRefreshRequest,
};
use super::snapshot::{
    build_snapshot_payload, get_materialized_cached_snapshot_with_loader,
    get_or_build_cached_snapshot, get_or_build_cached_snapshot_with_stale_flag,
    if_none_match_matches, materialized_snapshot_result_response, snapshot_json_response,
    snapshot_response_for_request,
};
use super::stats::stats_json_handler;

fn sample_listing() -> PartyFinderListing {
    PartyFinderListing {
        id: 166_086,
        content_id_lower: 40_205_661,
        name: SeString::parse(b"Yuki Coffee").unwrap(),
        description: SeString::parse(b"Test Description").unwrap(),
        created_world: 44,
        home_world: 44,
        current_world: 44,
        category: DutyCategory::HighEndDuty,
        duty: 1010,
        duty_type: DutyType::Normal,
        pf_category_high_end: false,
        cfc_excel_high_end: false,
        client_high_end_duty: false,
        beginners_welcome: false,
        seconds_remaining: 1667,
        min_item_level: 0,
        num_parties: 3,
        slots_available: 24,
        last_server_restart: 1_774_336_035,
        objective: ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::DUTY_COMPLETE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: std::iter::repeat_with(|| PartyFinderSlot {
            accepting: JobFlags::all() - JobFlags::GLADIATOR,
        })
        .take(8)
        .collect(),
        jobs_present: vec![0; 8],
        member_content_ids: vec![
            1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 7, 8, 9, 0, 10,
        ],
        member_jobs: vec![
            37, 0, 24, 0, 22, 0, 31, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 28, 34, 22, 0, 38,
        ],
        detail_slot_flags: vec!["0x0000000000000000".to_string(); 24],
        leader_content_id: 5,
    }
}

fn sample_cached_snapshot(revision: u64) -> CachedListingsSnapshot {
    CachedListingsSnapshot {
        revision,
        body: format!(r#"{{"revision":{revision}}}"#).into_bytes().into(),
        body_gzip: None,
        etag: None,
        content_encoding: None,
    }
}

fn sample_gzipped_cached_snapshot(revision: u64) -> CachedListingsSnapshot {
    CachedListingsSnapshot {
        revision,
        body: vec![0x1f, 0x8b, 0x08, 0x00].into(),
        body_gzip: None,
        etag: Some(format!("sha256-test-{revision}")),
        content_encoding: Some("gzip".to_string()),
    }
}

async fn test_state(source: ListingsSnapshotSource, current_revision: u64) -> Arc<State> {
    test_state_with_ingest_skew(source, current_revision, 300).await
}

async fn test_state_with_ingest_skew(
    source: ListingsSnapshotSource,
    current_revision: u64,
    ingest_clock_skew_seconds: i64,
) -> Arc<State> {
    let mongo = Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .expect("construct test mongo client");
    let (change_tx, _) = tokio::sync::broadcast::channel(8);

    Arc::new(State {
        mongo,
        stats: Default::default(),
        listings_change_channel: change_tx,
        listings_revision: std::sync::atomic::AtomicU64::new(current_revision),
        revision_epoch: crate::web::generate_revision_epoch(),
        listings_revision_pending: Default::default(),
        listings_revision_notify: Notify::new(),
        listings_revision_coalesce_window: std::time::Duration::from_millis(1),
        listings_revision_webhook: None,
        listings_snapshot_cache: Default::default(),
        listings_snapshot_source: source,
        listings_snapshot_collection_name: "listings_snapshots".to_string(),
        listings_snapshot_document_id: "current".to_string(),
        listing_source_state_collection_name: "listing_source_state".to_string(),
        listing_source_state_document_id: "current".to_string(),
        listing_snapshot_revision_state_collection_name: "listing_snapshot_revision_state"
            .to_string(),
        materialized_snapshot_reconcile_interval_seconds: 30,
        snapshot_refresh_shared_secret: "refresh-secret".to_string(),
        snapshot_refresh_client_id: "listings-snapshot-refresh".to_string(),
        snapshot_refresh_clock_skew_seconds: 300,
        snapshot_refresh_nonce_ttl_seconds: 300,
        fflogs_jobs_limit: 1,
        fflogs_hidden_cache_ttl_hours: 24,
        fflogs_active_parse_refresh_minutes: 15,
        parse_cache_ttl_hours: 24,
        fflogs_non_fetch_failure_abandon_threshold: 2,
        tomestone_jobs_limit: 1,
        tomestone_active_refresh_minutes: 60,
        tomestone_non_fetch_failure_abandon_threshold: 2,
        listing_upsert_concurrency: 1,
        player_upsert_concurrency: 1,
        max_body_bytes_contribute: 1024,
        max_body_bytes_multiple: 1024,
        max_body_bytes_players: 1024,
        max_body_bytes_detail: 1024,
        max_body_bytes_detail_batch: 1024,
        max_body_bytes_fflogs_results: 1024,
        max_body_bytes_tomestone_results: 1024,
        max_multiple_batch_size: 10,
        max_players_batch_size: 10,
        max_fflogs_results_batch_size: 10,
        max_tomestone_results_batch_size: 10,
        max_detail_batch_size: 10,
        max_detail_member_count: 8,
        ingest_security: IngestSecurityConfig {
            require_signature: false,
            require_signature_for_fflogs: None,
            require_signature_for_tomestone: None,
            shared_secret: "public-ingest-secret".to_string(),
            clock_skew_seconds: ingest_clock_skew_seconds,
            nonce_ttl_seconds: 300,
            require_capabilities_for_protected_endpoints: false,
            capability_secret: "test-capability-secret".to_string(),
            capability_session_ttl_seconds: 300,
            capability_detail_ttl_seconds: 300,
            rate_limit_prefer_client_id: false,
            trust_loopback_proxy_headers: true,
            trusted_relay_shared_secret: String::new(),
            rate_limits: IngestRateLimits {
                contribute_per_minute: 60,
                multiple_per_minute: 60,
                players_per_minute: 60,
                detail_per_minute: 60,
                fflogs_jobs_per_minute: 60,
                fflogs_results_per_minute: 60,
                tomestone_jobs_per_minute: 60,
                tomestone_results_per_minute: 60,
            },
            open_aggregate_rate_limits: IngestRateLimits::default_open_aggregate(),
        },
        ingest_rate_windows: Default::default(),
        ingest_open_rate_windows: Default::default(),
        ingest_nonces: Default::default(),
        snapshot_refresh_nonces: Default::default(),
        ws_clients: Default::default(),
        fflogs_job_leases: RwLock::new(HashMap::new()),
        fflogs_jobs_dispatched_total: Default::default(),
        fflogs_results_received_total: Default::default(),
        fflogs_results_rejected_total: Default::default(),
        fflogs_leases_abandoned_total: Default::default(),
        fflogs_leases_abandon_rejected_total: Default::default(),
        fflogs_hidden_refresh_total: Default::default(),
        fflogs_leader_fallback_total: Default::default(),
        tomestone_job_leases: RwLock::new(HashMap::new()),
        tomestone_jobs_dispatched_total: Default::default(),
        tomestone_results_received_total: Default::default(),
        tomestone_results_rejected_total: Default::default(),
        tomestone_leases_abandoned_total: Default::default(),
        tomestone_leases_abandon_rejected_total: Default::default(),
        monitor_snapshot_interval_seconds: 0,
    })
}

fn signed_refresh_headers(
    secret: &str,
    client_id: &str,
    nonce: &str,
    document_id: &str,
    expected_revision: u64,
) -> HeaderMap {
    signed_refresh_headers_at(
        secret,
        client_id,
        nonce,
        document_id,
        expected_revision,
        Utc::now().timestamp(),
    )
}

fn signed_refresh_headers_at(
    secret: &str,
    client_id: &str,
    nonce: &str,
    document_id: &str,
    expected_revision: u64,
    timestamp: i64,
) -> HeaderMap {
    let timestamp = timestamp.to_string();
    let payload = format!(
        "POST\n/internal/listings/snapshot/refresh\n{}\n{}\n{}\n{}\n{}",
        timestamp, nonce, client_id, document_id, expected_revision
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("test hmac should initialize");
    mac.update(payload.as_bytes());
    let signature = base64::encode(mac.finalize().into_bytes());

    let mut headers = HeaderMap::new();
    headers.insert("x-rpf-client-id", HeaderValue::from_str(client_id).unwrap());
    headers.insert(
        "x-rpf-snapshot-document-id",
        HeaderValue::from_str(document_id).unwrap(),
    );
    headers.insert(
        "x-rpf-expected-revision",
        HeaderValue::from_str(&expected_revision.to_string()).unwrap(),
    );
    headers.insert(
        "x-rpf-timestamp",
        HeaderValue::from_str(&timestamp).unwrap(),
    );
    headers.insert("x-rpf-nonce", HeaderValue::from_str(nonce).unwrap());
    headers.insert("x-rpf-signature-version", HeaderValue::from_static("v1"));
    headers.insert(
        "x-rpf-signature",
        HeaderValue::from_str(&signature).unwrap(),
    );
    headers
}

fn sample_renderable_member(party_index: u8, parse: ParseDisplay) -> RenderableMember {
    RenderableMember {
        job_id: 19,
        player: Player {
            content_id: 42,
            name: "Neutral Member".to_string(),
            home_world: 73,
            current_world: 73,
            last_seen: Utc::now(),
            seen_count: 1,
            account_id: "-1".to_string(),
        },
        parse,
        progress: ProgressDisplay::default(),
        slot_index: 0,
        party_index,
        identity_fallback: false,
    }
}

#[test]
fn api_listing_uses_display_slot_model_for_alliance_layouts() {
    let mut source = sample_listing();
    source.slots = Vec::new();
    source.detail_slot_flags[9] = "0x0000000020422040".to_string();
    source.detail_slot_flags[22] = "0x00000000D99DDAB4".to_string();

    let listing = ApiReadableListing::from_parts(
        source,
        Vec::new(),
        Default::default(),
        Default::default(),
    );

    assert_eq!(listing.slot_count, 24);
    assert_eq!(listing.display_slots.len(), 24);
    assert_eq!(listing.slots_filled_count, 10);
    assert_eq!(listing.display_slots[9].role_class, "healer");
    assert_eq!(listing.display_slots[9].title, "CNJ WHM SCH AST SGE");
    assert_eq!(listing.display_slots[22].role_class, "dps");
    assert!(listing.display_slots[22].title.contains("DNC"));
}

#[test]
fn api_listing_high_end_uses_server_authoritative_duty_metadata() {
    let mut known_high_end = sample_listing();
    known_high_end.duty = 1010;
    known_high_end.category = DutyCategory::Raid;
    known_high_end.client_high_end_duty = false;
    let known_high_end_json = serde_json::to_value(ApiReadableListing::from_parts(
        known_high_end,
        Vec::new(),
        Default::default(),
        Default::default(),
    ))
    .expect("api listing should serialize");

    let mut known_non_high_end = sample_listing();
    known_non_high_end.duty = 55;
    known_non_high_end.duty_type = DutyType::Other;
    known_non_high_end.category = DutyCategory::HighEndDuty;
    known_non_high_end.client_high_end_duty = true;
    known_non_high_end.pf_category_high_end = true;
    known_non_high_end.cfc_excel_high_end = true;
    let known_non_high_end_json = serde_json::to_value(ApiReadableListing::from_parts(
        known_non_high_end,
        Vec::new(),
        Default::default(),
        Default::default(),
    ))
    .expect("api listing should serialize");

    let mut unknown_duty_high_end = sample_listing();
    unknown_duty_high_end.duty = u16::MAX;
    unknown_duty_high_end.category = DutyCategory::HighEndDuty;
    let unknown_duty_high_end_json = serde_json::to_value(ApiReadableListing::from_parts(
        unknown_duty_high_end,
        Vec::new(),
        Default::default(),
        Default::default(),
    ))
    .expect("api listing should serialize");

    assert_eq!(known_high_end_json["high_end"], true);
    assert_eq!(known_non_high_end_json["high_end"], false);
    assert_eq!(unknown_duty_high_end_json["high_end"], true);
}

#[test]
fn api_listing_serializes_language_neutral_text_tokens() {
    let mut listing = sample_listing();
    listing.description = SeString(vec![
        Payload::Text(TextPayload("Need ".to_string())),
        Payload::AutoTranslate(AutoTranslatePayload { group: 1, key: 101 }),
    ]);
    listing.pf_category_high_end = true;
    listing.cfc_excel_high_end = true;
    listing.client_high_end_duty = true;

    let serialized = serde_json::to_value(ApiReadableListing::from_parts(
        listing,
        Vec::new(),
        Default::default(),
        Default::default(),
    ))
    .expect("api listing should serialize");

    assert_eq!(
        serialized["creator_name"],
        json!([{ "type": "text", "text": "Yuki Coffee" }])
    );
    assert_eq!(
        serialized["description"],
        json!([
            { "type": "text", "text": "Need " },
            { "type": "auto_translate", "group": 1, "key": 101 }
        ])
    );
    assert_eq!(serialized["duty_id"], 1010);
    assert_eq!(serialized["duty_type"], DutyType::Normal as u8);
    assert_eq!(serialized["category"], DutyCategory::HighEndDuty as u32);
    assert!(
        serialized.get("duty_name").is_none(),
        "snapshot should not include localized duty names"
    );
    assert!(
        serialized.get("pf_category_high_end").is_none(),
        "snapshot should not expose telemetry-only high-end signals"
    );
    assert!(
        serialized.get("cfc_excel_high_end").is_none(),
        "snapshot should not expose telemetry-only high-end signals"
    );
    assert!(
        serialized.get("client_high_end_duty").is_none(),
        "snapshot should not expose telemetry-only high-end signals"
    );
}

#[test]
fn api_listing_serializes_neutral_member_display_state() {
    let hidden_fallback_parse = ParseDisplay::new(
        Some(84),
        "parse-purple".to_string(),
        None,
        "parse-none".to_string(),
        false,
        false,
        true,
        false,
        ParseSource::Plugin,
    );
    let hidden_leader_parse = ParseDisplay::new(
        None,
        "parse-none".to_string(),
        None,
        "parse-none".to_string(),
        false,
        false,
        true,
        false,
        ParseSource::Plugin,
    );

    let serialized = serde_json::to_value(ApiReadableListing::from_parts(
        sample_listing(),
        vec![sample_renderable_member(1, hidden_fallback_parse)],
        hidden_leader_parse,
        Default::default(),
    ))
    .expect("api listing should serialize");

    let member = &serialized["members"][0];
    assert!(
        member.get("party_label").is_none(),
        "snapshot should expose party_index, not localized alliance labels"
    );
    assert_eq!(member["parse"]["originally_hidden"], json!(true));
    assert_eq!(serialized["leader_parse"]["originally_hidden"], json!(true));
}

#[tokio::test]
async fn snapshot_cache_reuses_ready_entry_for_matching_revision() {
    let cache = RwLock::new(ListingsSnapshotCacheState::Ready(sample_cached_snapshot(7)));
    let builds = AtomicUsize::new(0);

    let snapshot = get_or_build_cached_snapshot(&cache, 7, || async {
        builds.fetch_add(1, Ordering::SeqCst);
        Ok::<_, ()>(sample_cached_snapshot(8))
    })
    .await
    .expect("cached snapshot should resolve");

    assert_eq!(snapshot.revision, 7);
    assert_eq!(builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn listings_revision_endpoint_exposes_current_revision_without_snapshot_body() {
    let state = test_state(ListingsSnapshotSource::Inline, 42).await;
    let expected_epoch = state.revision_epoch().to_string();
    let route = crate::api::api(state);

    let response = warp::test::request()
        .method("GET")
        .path("/api/listings/revision")
        .reply(&route)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");

    let body: serde_json::Value =
        serde_json::from_slice(response.body()).expect("revision response should be json");
    assert_eq!(body["revision"], json!(42));
    assert_eq!(body["revision_epoch"], json!(expected_epoch));
    assert!(
        body.get("listings").is_none(),
        "revision endpoint should stay lightweight"
    );
}

#[tokio::test]
async fn snapshot_cache_coalesces_concurrent_builders() {
    let cache = RwLock::new(ListingsSnapshotCacheState::Empty);
    let builds = AtomicUsize::new(0);

    let build = || async {
        builds.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        Ok::<_, ()>(sample_cached_snapshot(11))
    };

    let (left, right) = tokio::join!(
        get_or_build_cached_snapshot(&cache, 11, build),
        get_or_build_cached_snapshot(&cache, 11, build)
    );

    assert_eq!(left.expect("left snapshot should resolve").revision, 11);
    assert_eq!(right.expect("right snapshot should resolve").revision, 11);
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn snapshot_payload_uses_stable_revision_without_retrying() {
    let revisions = Arc::new([5_u64, 5]);
    let cursor = Arc::new(AtomicUsize::new(0));
    let builds = AtomicUsize::new(0);

    let (revision, payload) = build_snapshot_payload(
        || async {
            builds.fetch_add(1, Ordering::SeqCst);
            "payload"
        },
        {
            let revisions = Arc::clone(&revisions);
            let cursor = Arc::clone(&cursor);
            move || {
                let index = cursor.fetch_add(1, Ordering::SeqCst);
                revisions[index]
            }
        },
    )
    .await;

    assert_eq!(revision, 5);
    assert_eq!(payload, "payload");
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn snapshot_payload_uses_lower_bound_when_revision_races() {
    let revisions = Arc::new([7_u64, 8]);
    let cursor = Arc::new(AtomicUsize::new(0));

    let (revision, payload) = build_snapshot_payload(|| async { "payload" }, {
        let revisions = Arc::clone(&revisions);
        let cursor = Arc::clone(&cursor);
        move || {
            let index = cursor.fetch_add(1, Ordering::SeqCst);
            revisions[index]
        }
    })
    .await;

    assert_eq!(revision, 7);
    assert_eq!(payload, "payload");
}

#[test]
fn snapshot_response_emits_etag_and_gzip_headers_for_materialized_cache() {
    let response = snapshot_json_response(sample_gzipped_cached_snapshot(9));

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json; charset=utf-8"
    );
    assert_eq!(
        response.headers().get("cache-control").unwrap(),
        "no-cache, max-age=0, must-revalidate"
    );
    assert_eq!(response.headers().get("etag").unwrap(), "\"sha256-test-9-gzip\"");
    assert_eq!(response.headers().get("content-encoding").unwrap(), "gzip");
}

#[tokio::test]
async fn materialized_absent_snapshot_returns_service_unavailable() {
    let response = materialized_snapshot_result_response(Ok(None));

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = warp::hyper::body::to_bytes(response.into_body())
        .await
        .expect("body should collect");
    assert_eq!(body.as_ref(), b"snapshot not ready",);
}

#[tokio::test]
async fn materialized_reconciliation_loads_newer_snapshot_and_ignores_same_or_older_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let mut receiver = state.listings_change_channel.subscribe();

    let newer = reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        Ok(Some(sample_cached_snapshot(6)))
    })
    .await
    .expect("reconcile should load newer snapshot");

    assert_eq!(newer.applied_revision, Some(6));
    assert_eq!(state.current_listings_revision(), 6);
    assert_eq!(receiver.recv().await.expect("revision should broadcast"), 6);

    let same = reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        Ok(Some(sample_cached_snapshot(6)))
    })
    .await
    .expect("same revision should be ignored");
    let older = reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        Ok(Some(sample_cached_snapshot(4)))
    })
    .await
    .expect("older revision should be ignored");

    assert_eq!(same.applied_revision, None);
    assert_eq!(older.applied_revision, None);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "same/older snapshots must not rebroadcast"
    );
}

#[tokio::test]
async fn reconcile_materialized_snapshot_warms_empty_cache_at_revision_one_on_startup() {
    let state = test_state(ListingsSnapshotSource::Materialized, 1).await;
    assert!(matches!(
        &*state.listings_snapshot_cache.read().await,
        ListingsSnapshotCacheState::Empty
    ));
    let mut receiver = state.listings_change_channel.subscribe();

    let outcome = reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        Ok(Some(sample_cached_snapshot(1)))
    })
    .await
    .expect("reconcile at revision 1 should succeed");

    assert_eq!(outcome.applied_revision, None);
    assert_eq!(state.current_listings_revision(), 1);
    assert!(matches!(
        &*state.listings_snapshot_cache.read().await,
        ListingsSnapshotCacheState::Ready(cached) if cached.revision == 1
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "warming initial revision 1 cache must not broadcast a spurious revision change"
    );
}

#[tokio::test]
async fn refresh_signature_rejects_missing_bad_and_replayed_nonce() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;

    let missing = HeaderMap::new();
    assert!(authorize_snapshot_refresh_request(&state, &missing)
        .await
        .is_err());

    let mut bad = signed_refresh_headers("wrong-secret", "worker", "bad-nonce", "current", 6);
    assert!(authorize_snapshot_refresh_request(&state, &bad)
        .await
        .is_err());

    bad = signed_refresh_headers(
        "refresh-secret",
        "listings-snapshot-refresh",
        "replayed-nonce",
        "current",
        6,
    );
    authorize_snapshot_refresh_request(&state, &bad)
        .await
        .expect("first use of nonce should authorize");
    let replay = authorize_snapshot_refresh_request(&state, &bad).await;
    assert!(replay.is_err(), "replayed nonce should be rejected");
}

#[tokio::test]
async fn refresh_signature_rejects_missing_expected_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let timestamp = Utc::now().timestamp().to_string();
    let nonce = "missing-expected-revision";
    let client_id = "listings-snapshot-refresh";
    let payload = format!(
        "POST\n/internal/listings/snapshot/refresh\n{}\n{}\n{}",
        timestamp, nonce, client_id
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(b"refresh-secret").expect("test hmac should initialize");
    mac.update(payload.as_bytes());
    let signature = base64::encode(mac.finalize().into_bytes());

    let mut headers = HeaderMap::new();
    headers.insert("x-rpf-client-id", HeaderValue::from_static(client_id));
    headers.insert(
        "x-rpf-snapshot-document-id",
        HeaderValue::from_static("current"),
    );
    headers.insert(
        "x-rpf-timestamp",
        HeaderValue::from_str(&timestamp).unwrap(),
    );
    headers.insert("x-rpf-nonce", HeaderValue::from_static(nonce));
    headers.insert("x-rpf-signature-version", HeaderValue::from_static("v1"));
    headers.insert(
        "x-rpf-signature",
        HeaderValue::from_str(&signature).unwrap(),
    );

    let error = authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect_err("refresh auth must require expected revision");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(
        error.message.contains("expected revision"),
        "unexpected auth error: {}",
        error.message
    );
}

#[tokio::test]
async fn refresh_signature_uses_snapshot_refresh_skew_independent_of_public_ingest_skew() {
    let state = test_state_with_ingest_skew(ListingsSnapshotSource::Materialized, 5, 1).await;
    let headers = signed_refresh_headers_at(
        "refresh-secret",
        "listings-snapshot-refresh",
        "snapshot-skew-independent",
        "current",
        6,
        Utc::now().timestamp() - 2,
    );

    authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect("snapshot refresh skew should not use public ingest skew");
}

#[tokio::test]
async fn refresh_signature_uses_dedicated_nonce_store() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let nonce = "dedicated-refresh-nonce";
    state.ingest_nonces.write().await.insert(
        format!("snapshot-refresh:listings-snapshot-refresh:{nonce}"),
        Utc::now(),
    );
    let headers = signed_refresh_headers(
        "refresh-secret",
        "listings-snapshot-refresh",
        nonce,
        "current",
        6,
    );

    authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect("public ingest nonce store must not affect snapshot refresh nonces");

    assert_eq!(state.ingest_nonces.read().await.len(), 1);
    assert!(state
        .snapshot_refresh_nonces
        .read()
        .await
        .contains_key(&format!(
            "snapshot-refresh:listings-snapshot-refresh:{nonce}"
        )));
}

#[tokio::test]
async fn refresh_signature_rejects_timestamp_outside_snapshot_refresh_skew() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let headers = signed_refresh_headers_at(
        "refresh-secret",
        "listings-snapshot-refresh",
        "snapshot-skew-too-old",
        "current",
        6,
        Utc::now().timestamp() - 301,
    );

    let error = authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect_err("snapshot refresh skew should reject old timestamps");

    assert_eq!(error.status, StatusCode::FORBIDDEN);
    assert_eq!(error.message, "timestamp skew exceeded");
}

#[tokio::test]
async fn valid_refresh_broadcasts_only_after_snapshot_revision_advances() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;
    let mut receiver = state.listings_change_channel.subscribe();

    let same = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "current".to_string(),
            expected_revision: 7,
        },
        || async { Ok(Some(sample_cached_snapshot(7))) },
    )
    .await
    .expect("same revision refresh should succeed");

    assert_eq!(same.applied_revision, None);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "same revision refresh must not broadcast"
    );

    let advanced = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "current".to_string(),
            expected_revision: 8,
        },
        || async { Ok(Some(sample_cached_snapshot(8))) },
    )
    .await
    .expect("advanced revision refresh should succeed");

    assert_eq!(advanced.applied_revision, Some(8));
    assert_eq!(receiver.recv().await.expect("revision should broadcast"), 8);
}

#[tokio::test]
async fn refresh_fails_when_loaded_snapshot_is_older_than_expected_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;

    let error = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "current".to_string(),
            expected_revision: 8,
        },
        || async { Ok(Some(sample_cached_snapshot(7))) },
    )
    .await
    .expect_err("refresh must fail when Mongo snapshot is older than worker expectation");

    assert!(
        error.to_string().contains("expected revision"),
        "unexpected refresh error: {error:#}"
    );
}

#[tokio::test]
async fn refresh_fails_when_expected_document_id_differs_from_server_document_id() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;

    let error = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "other-current".to_string(),
            expected_revision: 8,
        },
        || async { Ok(Some(sample_cached_snapshot(8))) },
    )
    .await
    .expect_err("refresh must fail when worker and server target different snapshot docs");

    assert!(
        error.to_string().contains("document id"),
        "unexpected refresh error: {error:#}"
    );
}

#[tokio::test]
async fn materialized_cache_applies_only_newer_snapshot() {
    let state = test_state(ListingsSnapshotSource::Materialized, 3).await;

    let applied = apply_materialized_snapshot_if_newer(&state, sample_cached_snapshot(4)).await;
    let ignored = apply_materialized_snapshot_if_newer(&state, sample_cached_snapshot(4)).await;

    assert_eq!(applied.applied_revision, Some(4));
    assert_eq!(ignored.applied_revision, None);
}

#[tokio::test]
async fn materialized_snapshot_fetch_ignores_cache_older_than_visible_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 8).await;
    *state.listings_snapshot_cache.write().await =
        ListingsSnapshotCacheState::Ready(sample_cached_snapshot(7));
    let loads = Arc::new(AtomicUsize::new(0));

    let snapshot = get_materialized_cached_snapshot_with_loader(Arc::clone(&state), {
        let loads = Arc::clone(&loads);
        || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(sample_cached_snapshot(8)))
        }
    })
    .await
    .expect("materialized snapshot lookup should succeed")
    .expect("loader should return a snapshot");

    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot.revision, 8);
}

#[tokio::test]
async fn snapshot_cache_recovers_when_inflight_builder_is_cancelled() {
    let cache = RwLock::new(ListingsSnapshotCacheState::Empty);
    let build_started = Arc::new(Notify::new());
    let release_build = Arc::new(Notify::new());

    {
        let first_lookup = get_or_build_cached_snapshot(&cache, 1, {
            let build_started = Arc::clone(&build_started);
            let release_build = Arc::clone(&release_build);
            move || {
                let build_started = Arc::clone(&build_started);
                let release_build = Arc::clone(&release_build);
                async move {
                    build_started.notify_one();
                    release_build.notified().await;
                    Ok::<_, anyhow::Error>(sample_cached_snapshot(1))
                }
            }
        });
        tokio::pin!(first_lookup);

        tokio::select! {
            _ = build_started.notified() => {}
            result = &mut first_lookup => panic!("first lookup finished unexpectedly: {result:?}"),
        }
    }

    let second = tokio::time::timeout(
        Duration::from_millis(100),
        get_or_build_cached_snapshot(&cache, 1, || async {
            Ok::<_, anyhow::Error>(sample_cached_snapshot(1))
        }),
    )
    .await;

    assert!(
        second.is_ok(),
        "cancelled builders must not leave the snapshot cache permanently building"
    );
    assert_eq!(
        second
            .expect("second lookup should not time out")
            .expect("second lookup should rebuild")
            .revision,
        1
    );
}

#[tokio::test]
async fn snapshot_cache_recovers_when_cancelled_drop_cannot_acquire_cache_write_lock() {
    let cache = RwLock::new(ListingsSnapshotCacheState::Empty);
    let build_started = Arc::new(Notify::new());
    let release_build = Arc::new(Notify::new());

    {
        let first_lookup = get_or_build_cached_snapshot(&cache, 1, {
            let build_started = Arc::clone(&build_started);
            let release_build = Arc::clone(&release_build);
            move || {
                let build_started = Arc::clone(&build_started);
                let release_build = Arc::clone(&release_build);
                async move {
                    build_started.notify_one();
                    release_build.notified().await;
                    Ok::<_, anyhow::Error>(sample_cached_snapshot(1))
                }
            }
        });
        tokio::pin!(first_lookup);

        tokio::select! {
            _ = build_started.notified() => {}
            result = &mut first_lookup => panic!("first lookup finished unexpectedly: {result:?}"),
        }

        // Hold a read guard across cancellation so SnapshotBuildGuard::drop cannot try_write().
        let _read_guard = cache.read().await;
        drop(first_lookup);
    }

    let second = tokio::time::timeout(
        Duration::from_millis(200),
        get_or_build_cached_snapshot(&cache, 1, || async {
            Ok::<_, anyhow::Error>(sample_cached_snapshot(1))
        }),
    )
    .await
    .expect("subsequent lookup must not hang when cancelled Building state lingers")
    .expect("subsequent lookup should rebuild");

    assert_eq!(second.revision, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_cache_concurrent_waiters_never_suffer_lost_wakeup() {
    for round in 0..50_u64 {
        let revision = round + 1;
        let cache = Arc::new(RwLock::new(ListingsSnapshotCacheState::Empty));
        let builds = Arc::new(AtomicUsize::new(0));
        let start_barrier = Arc::new(tokio::sync::Barrier::new(32));

        let mut handles = Vec::with_capacity(32);
        for _ in 0..32 {
            let cache = Arc::clone(&cache);
            let builds = Arc::clone(&builds);
            let start_barrier = Arc::clone(&start_barrier);
            handles.push(tokio::spawn(async move {
                start_barrier.wait().await;
                get_or_build_cached_snapshot(&cache, revision, || {
                    let builds = Arc::clone(&builds);
                    async move {
                        builds.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        Ok::<_, anyhow::Error>(sample_cached_snapshot(revision))
                    }
                })
                .await
            }));
        }

        let results = tokio::time::timeout(Duration::from_secs(3), async {
            let mut out = Vec::with_capacity(handles.len());
            for handle in handles {
                out.push(handle.await.expect("task should join").expect("build ok"));
            }
            out
        })
        .await
        .expect("concurrent snapshot waiters must never hang with lost wakeup");

        assert!(results.iter().all(|snap| snap.revision == revision));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn materialized_snapshot_fetch_reconciles_stale_process_revision_on_cadence() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;
    *state.listings_snapshot_cache.write().await =
        ListingsSnapshotCacheState::Ready(sample_cached_snapshot(7));
    let loads = Arc::new(AtomicUsize::new(0));

    let snapshot = get_materialized_cached_snapshot_with_loader(Arc::clone(&state), {
        let loads = Arc::clone(&loads);
        || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(sample_cached_snapshot(8)))
        }
    })
    .await
    .expect("materialized snapshot lookup should succeed")
    .expect("loader should return a snapshot");

    assert_eq!(snapshot.revision, 8);
    assert_eq!(state.current_listings_revision(), 8);
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    let cached = get_materialized_cached_snapshot_with_loader(Arc::clone(&state), {
        let loads = Arc::clone(&loads);
        || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(sample_cached_snapshot(9)))
        }
    })
    .await
    .expect("materialized snapshot lookup should succeed")
    .expect("cache should remain ready");

    assert_eq!(cached.revision, 8);
    assert_eq!(
        loads.load(Ordering::SeqCst),
        1,
        "freshness cadence should avoid loading Mongo on every request"
    );
}

#[tokio::test]
async fn refresh_signature_rejects_empty_shared_secret() {
    let mut state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    Arc::get_mut(&mut state).unwrap().snapshot_refresh_shared_secret = String::new();
    let headers = signed_refresh_headers(
        "",
        "listings-snapshot-refresh",
        "empty-secret-nonce",
        "current",
        6,
    );

    let err = authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect_err("empty refresh shared secret must fail closed");
    assert_eq!(err.status, warp::http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn snapshot_cache_serves_stale_on_error_and_503_when_empty() {
    let empty_cache = RwLock::new(ListingsSnapshotCacheState::Empty);
    let err = get_or_build_cached_snapshot(&empty_cache, 1, || async {
        Err::<CachedListingsSnapshot, _>(anyhow::anyhow!("mongo unavailable"))
    })
    .await
    .expect_err("empty cache must propagate build error");
    assert!(err.to_string().contains("mongo unavailable"));
    assert!(matches!(&*empty_cache.read().await, ListingsSnapshotCacheState::Empty));

    let error_response = materialized_snapshot_result_response(Err(anyhow::anyhow!("boom")));
    assert_eq!(error_response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let stale = sample_cached_snapshot(7);
    let cache = RwLock::new(ListingsSnapshotCacheState::Ready(stale));
    let served = get_or_build_cached_snapshot(&cache, 8, || async {
        Err::<CachedListingsSnapshot, _>(anyhow::anyhow!("mongo timeout"))
    })
    .await
    .expect("build failure with ready stale snapshot should serve stale snapshot");
    assert_eq!(served.revision, 7);
    assert!(matches!(
        &*cache.read().await,
        ListingsSnapshotCacheState::Ready(ready) if ready.revision == 7
    ));
}

#[tokio::test]
async fn snapshot_response_supports_if_none_match_304_and_precompressed_gzip() {
    let plain_json = br#"{"revision":9,"listings":[]}"#;
    let gzipped = crate::listings_snapshot::gzip_body(plain_json).unwrap();
    let snapshot = CachedListingsSnapshot {
        revision: 9,
        body: plain_json.to_vec().into(),
        body_gzip: Some(gzipped.clone().into()),
        etag: Some("\"sha256-etag-9\"".to_string()),
        content_encoding: None,
    };

    let not_modified_gzip = snapshot_response_for_request(
        snapshot.clone(),
        Some("gzip"),
        Some("\"sha256-etag-9-gzip\""),
    );
    assert_eq!(not_modified_gzip.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        not_modified_gzip.headers().get("etag").unwrap(),
        "\"sha256-etag-9-gzip\""
    );
    assert_eq!(
        not_modified_gzip.headers().get("cache-control").unwrap(),
        "no-cache, max-age=0, must-revalidate"
    );
    assert_eq!(
        not_modified_gzip.headers().get("vary").unwrap(),
        "Accept-Encoding"
    );

    let weak_not_modified_gzip = snapshot_response_for_request(
        snapshot.clone(),
        Some("gzip"),
        Some("W/\"sha256-etag-9-gzip\""),
    );
    assert_eq!(weak_not_modified_gzip.status(), StatusCode::NOT_MODIFIED);

    // Sending plain ETag when requesting gzip must not return 304.
    let mismatch_resp = snapshot_response_for_request(
        snapshot.clone(),
        Some("gzip, br"),
        Some("\"sha256-etag-9\""),
    );
    assert_eq!(mismatch_resp.status(), StatusCode::OK);
    assert_eq!(mismatch_resp.headers().get("content-encoding").unwrap(), "gzip");
    assert_eq!(
        mismatch_resp.headers().get("etag").unwrap(),
        "\"sha256-etag-9-gzip\""
    );
    assert_eq!(mismatch_resp.headers().get("vary").unwrap(), "Accept-Encoding");
    let gzip_bytes = warp::hyper::body::to_bytes(mismatch_resp.into_body())
        .await
        .unwrap();
    assert_eq!(gzip_bytes.as_ref(), gzipped.as_slice());

    let not_modified_plain =
        snapshot_response_for_request(snapshot.clone(), None, Some("\"sha256-etag-9\""));
    assert_eq!(not_modified_plain.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        not_modified_plain.headers().get("etag").unwrap(),
        "\"sha256-etag-9\""
    );

    let plain_resp = snapshot_response_for_request(snapshot, None, Some("\"other-etag\""));
    assert_eq!(plain_resp.status(), StatusCode::OK);
    assert_eq!(
        plain_resp.headers().get("etag").unwrap(),
        "\"sha256-etag-9\""
    );
    assert_eq!(
        plain_resp.headers().get("cache-control").unwrap(),
        "no-cache, max-age=0, must-revalidate"
    );
    assert!(plain_resp.headers().get("content-encoding").is_none());
    let plain_bytes = warp::hyper::body::to_bytes(plain_resp.into_body())
        .await
        .unwrap();
    assert_eq!(plain_bytes.as_ref(), plain_json);
}

#[test]
fn if_none_match_parser_enforces_rfc9110_quotes_weak_prefix_and_commas() {
    assert!(if_none_match_matches(Some("*"), Some("\"sha256-abc\"")));
    assert!(if_none_match_matches(
        Some("\"a,b\", W/\"sha256-abc\", \"c\""),
        Some("\"sha256-abc\"")
    ));
    assert!(!if_none_match_matches(
        Some("sha256-abc"),
        Some("\"sha256-abc\"")
    ));
    assert!(!if_none_match_matches(
        Some("w/\"sha256-abc\""),
        Some("\"sha256-abc\"")
    ));
    assert!(!if_none_match_matches(
        Some("W/ \"sha256-abc\""),
        Some("\"sha256-abc\"")
    ));
}

#[tokio::test]
async fn get_or_build_cached_snapshot_with_stale_flag_signals_stale_and_preserves_on_error() {
    let cache = RwLock::new(ListingsSnapshotCacheState::Empty);
    let first = get_or_build_cached_snapshot_with_stale_flag(&cache, 1, |has_stale| async move {
        assert!(!has_stale);
        Ok::<_, anyhow::Error>(sample_cached_snapshot(4))
    })
    .await
    .unwrap();
    assert_eq!(first.revision, 4);

    let preserved =
        get_or_build_cached_snapshot_with_stale_flag(&cache, 5, |has_stale| async move {
            assert!(has_stale);
            Err::<CachedListingsSnapshot, _>(anyhow::anyhow!("timeout preserving stale"))
        })
        .await
        .unwrap();
    assert_eq!(preserved.revision, 4);
}

#[tokio::test]
async fn apply_materialized_snapshot_dispatches_webhook_only_when_revision_advances() {
    use warp::Filter;

    let delivered = Arc::new(std::sync::Mutex::new(Vec::<(u64, Option<String>, Option<String>)>::new()));
    let route = {
        let delivered = Arc::clone(&delivered);
        warp::post()
            .and(warp::path("internal"))
            .and(warp::path("listings"))
            .and(warp::path("revision"))
            .and(warp::path::end())
            .and(warp::header::optional::<String>("x-rpf-revision-epoch"))
            .and(warp::body::json())
            .map(move |epoch_header: Option<String>, body: serde_json::Value| {
                let rev = body["revision"].as_u64().unwrap_or_default();
                let epoch_body = body["revision_epoch"].as_str().map(str::to_string);
                delivered.lock().unwrap().push((rev, epoch_header, epoch_body));
                warp::reply()
            })
    };
    let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
    tokio::task::spawn(server);

    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let mut state_owned = Arc::try_unwrap(state).unwrap_or_else(|_| panic!("single ref"));
    state_owned.listings_revision_webhook = Some(ListingsRevisionWebhook::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        reqwest::Url::parse(&format!("http://{addr}/internal/listings/revision")).unwrap(),
        "test-webhook-secret".to_string(),
    ));
    let state = Arc::new(state_owned);
    let expected_epoch = state.revision_epoch().to_string();

    // Equal or older revision must NOT dispatch webhook.
    let stale_outcome = apply_materialized_snapshot_if_newer(&state, sample_cached_snapshot(5)).await;
    assert_eq!(stale_outcome.applied_revision, None);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(delivered.lock().unwrap().is_empty());

    // Newer revision MUST advance state and dispatch webhook.
    let newer_outcome = apply_materialized_snapshot_if_newer(&state, sample_cached_snapshot(8)).await;
    assert_eq!(newer_outcome.applied_revision, Some(8));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if delivered.lock().unwrap().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("materialized snapshot advancement must dispatch revision webhook");
    let entries = delivered.lock().unwrap();
    assert_eq!(
        entries.as_slice(),
        &[(8, Some(expected_epoch.clone()), Some(expected_epoch))]
    );
}

#[tokio::test]
async fn stats_json_handler_serves_precached_gzip_etag_304_and_cache_control() {
    let state = test_state(ListingsSnapshotSource::Inline, 1).await;

    // 503 before background task populates stats.
    let empty_resp = stats_json_handler(Arc::clone(&state), HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!(empty_resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    let sample_stats = crate::stats::Statistics {
        count: vec![crate::stats::Count { count: 12 }],
        aliases: HashMap::new(),
        duties: vec![crate::stats::DutyInfo {
            info: (crate::listing::DutyType::Normal as u8, 1010, 1),
            count: 5,
        }],
        hosts: Vec::new(),
        hours: vec![crate::stats::HourInfo { hour: 12, count: 4 }],
        days: vec![crate::stats::DayInfo {
            day: 2,
            count: 7,
        }],
    };
    let seven_days_stats = crate::stats::Statistics {
        count: vec![crate::stats::Count { count: 3 }],
        aliases: HashMap::new(),
        duties: Vec::new(),
        hosts: Vec::new(),
        hours: Vec::new(),
        days: Vec::new(),
    };
    *state.stats.write().await = Some(Arc::new(
        crate::stats::CachedStatistics::from_stats(sample_stats, seven_days_stats).unwrap(),
    ));

    // Plain JSON 200 all-time stats
    let resp = stats_json_handler(Arc::clone(&state), HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("cache-control").unwrap(),
        "public, max-age=60, stale-while-revalidate=300"
    );
    assert_eq!(
        resp.headers().get("vary").unwrap(),
        "Accept-Encoding, Accept-Language, Cookie"
    );
    let plain_etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();
    assert!(plain_etag.starts_with("\"sha256-"));
    assert!(!plain_etag.ends_with("-gzip\""));
    assert!(resp.headers().get("content-encoding").is_none());

    // Gzip JSON 200 + distinct -gzip ETag
    let mut gzip_headers = HeaderMap::new();
    gzip_headers.insert("accept-encoding", HeaderValue::from_static("gzip, br"));
    gzip_headers.insert("accept-language", HeaderValue::from_static("ja"));
    let gzip_resp = stats_json_handler(Arc::clone(&state), gzip_headers.clone(), false)
        .await
        .unwrap();
    assert_eq!(gzip_resp.status(), StatusCode::OK);
    assert_eq!(gzip_resp.headers().get("content-encoding").unwrap(), "gzip");
    let gzip_etag = gzip_resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(gzip_etag.ends_with("-gzip\""));
    assert_ne!(gzip_etag, plain_etag);
    let compressed_bytes = warp::hyper::body::to_bytes(gzip_resp.into_body())
        .await
        .unwrap();
    let decompressed = crate::listings_snapshot::gunzip_body(&compressed_bytes).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&decompressed).unwrap();
    assert_eq!(decoded["num_listings"], 12);

    // Conditional If-None-Match -> 304 Not Modified (including 7days endpoint)
    let mut inm_headers = HeaderMap::new();
    inm_headers.insert("accept-encoding", HeaderValue::from_static("gzip"));
    inm_headers.insert("if-none-match", HeaderValue::from_str(&gzip_etag).unwrap());
    let not_mod = stats_json_handler(Arc::clone(&state), inm_headers, false)
        .await
        .unwrap();
    assert_eq!(not_mod.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        not_mod.headers().get("cache-control").unwrap(),
        "public, max-age=60, stale-while-revalidate=300"
    );
    assert_eq!(not_mod.headers().get("etag").unwrap(), gzip_etag.as_str());

    let seven_days_resp = stats_json_handler(state, HeaderMap::new(), true)
        .await
        .unwrap();
    assert_eq!(seven_days_resp.status(), StatusCode::OK);
    let seven_days_body = warp::hyper::body::to_bytes(seven_days_resp.into_body())
        .await
        .unwrap();
    let seven_days_json: serde_json::Value = serde_json::from_slice(&seven_days_body).unwrap();
    assert_eq!(seven_days_json["num_listings"], 3);
}

#[tokio::test]
async fn ws_route_returns_503_when_max_ws_clients_capacity_is_exhausted() {
    let state = test_state(ListingsSnapshotSource::Inline, 5).await;
    let route = crate::api::api(Arc::clone(&state));

    let mut permits = Vec::with_capacity(crate::ws::MAX_WS_CLIENTS);
    for _ in 0..crate::ws::MAX_WS_CLIENTS {
        permits.push(
            state
                .ws_clients
                .try_acquire()
                .expect("should acquire permit up to MAX_WS_CLIENTS"),
        );
    }
    assert!(state.ws_clients.try_acquire().is_none());

    let response = warp::test::request()
        .method("GET")
        .path("/api/ws")
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .reply(&route)
        .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    drop(permits);
    assert!(state.ws_clients.try_acquire().is_some());
}
