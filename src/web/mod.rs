use anyhow::Result;
use chrono::{DateTime, Utc};
use mongodb::Client as MongoClient;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::sync::broadcast::Sender;
use tokio::sync::{Notify, RwLock};
use warp::hyper::body::Bytes;

use crate::config::{Config, ListingsSnapshotSource};
use crate::stats::CachedStatistics;

pub mod background;
mod contribute;
mod fflogs_ingest;
pub mod ingest_guard;
pub mod routes;
mod state;
#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use state::indexes::report_parse_summary_identity_index_model;

pub const FFLOGS_LEASE_TTL_MINUTES: i64 = 3;
pub const FFLOGS_LEASE_SWEEP_INTERVAL_SECONDS: u64 = 30;

pub async fn start(config: Arc<Config>) -> Result<()> {
    let state = State::new(Arc::clone(&config)).await?;

    // Background tasks
    background::spawn_stats_task(Arc::clone(&state));
    background::spawn_report_parse_summary_index_task(Arc::clone(&state));
    background::spawn_fflogs_lease_sweeper_task(Arc::clone(&state));
    background::spawn_listings_revision_publisher_task(Arc::clone(&state));
    background::spawn_monitor_snapshot_task(Arc::clone(&state));
    background::spawn_materialized_snapshot_reconcile_task(Arc::clone(&state));

    tracing::info!("listening at {}", config.web.host);
    warp::serve(routes::router(state))
        .run(config.web.host)
        .await;
    Ok(())
}

pub struct State {
    pub mongo: MongoClient,
    pub stats: RwLock<Option<CachedStatistics>>,
    pub listings_change_channel: Sender<u64>,
    pub listings_revision: AtomicU64,
    pub listings_revision_pending: AtomicBool,
    pub listings_revision_notify: Notify,
    pub listings_revision_coalesce_window: Duration,
    pub listings_revision_webhook: Option<ListingsRevisionWebhook>,
    pub listings_snapshot_cache: RwLock<ListingsSnapshotCacheState>,
    pub listings_snapshot_source: ListingsSnapshotSource,
    pub listings_snapshot_collection_name: String,
    pub listings_snapshot_document_id: String,
    pub listing_source_state_collection_name: String,
    pub listing_source_state_document_id: String,
    pub listing_snapshot_revision_state_collection_name: String,
    pub materialized_snapshot_reconcile_interval_seconds: u64,
    pub snapshot_refresh_shared_secret: String,
    pub snapshot_refresh_client_id: String,
    pub snapshot_refresh_clock_skew_seconds: i64,
    pub snapshot_refresh_nonce_ttl_seconds: i64,
    pub fflogs_jobs_limit: usize,
    pub fflogs_hidden_cache_ttl_hours: i64,
    pub fflogs_active_parse_refresh_minutes: i64,
    pub parse_cache_ttl_hours: i64,
    pub fflogs_non_fetch_failure_abandon_threshold: u32,
    pub listing_upsert_concurrency: usize,
    pub player_upsert_concurrency: usize,
    pub max_body_bytes_contribute: u64,
    pub max_body_bytes_multiple: u64,
    pub max_body_bytes_players: u64,
    pub max_body_bytes_detail: u64,
    pub max_body_bytes_detail_batch: u64,
    pub max_body_bytes_fflogs_results: u64,
    pub max_multiple_batch_size: usize,
    pub max_players_batch_size: usize,
    pub max_fflogs_results_batch_size: usize,
    pub max_detail_batch_size: usize,
    pub max_detail_member_count: usize,
    pub ingest_security: IngestSecurityConfig,
    pub ingest_rate_windows: RwLock<HashMap<IngestRateKey, IngestRateWindow>>,
    pub ingest_nonces: RwLock<HashMap<String, DateTime<Utc>>>,
    pub snapshot_refresh_nonces: RwLock<HashMap<String, DateTime<Utc>>>,
    /// in-flight FFLogs job lease map to avoid duplicate dispatch across workers
    pub fflogs_job_leases: RwLock<HashMap<FflogsLeaseKey, FflogsLeaseEntry>>,
    /// total number of FFLogs jobs dispatched to workers
    pub fflogs_jobs_dispatched_total: AtomicU64,
    /// total number of FFLogs result payload items received from workers
    pub fflogs_results_received_total: AtomicU64,
    /// total number of FFLogs result payload items rejected by lease/token validation
    pub fflogs_results_rejected_total: AtomicU64,
    /// total number of FFLogs lease items released via explicit abandon endpoint
    pub fflogs_leases_abandoned_total: AtomicU64,
    /// total number of FFLogs lease abandon items rejected by validation
    pub fflogs_leases_abandon_rejected_total: AtomicU64,
    /// total hidden-cache refresh candidates detected during jobs allocation
    pub fflogs_hidden_refresh_total: AtomicU64,
    /// total leader fallback applications when rendering listings
    pub fflogs_leader_fallback_total: AtomicU64,
    /// periodic monitor snapshot interval (seconds). set 0 to disable.
    pub monitor_snapshot_interval_seconds: u64,
}

#[derive(Clone)]
pub struct ListingsRevisionWebhook {
    pub client: reqwest::Client,
    pub url: reqwest::Url,
    pub shared_secret: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FflogsLeaseKey {
    pub content_id: u64,
    pub zone_id: u32,
    pub difficulty_id: i32,
}

#[derive(Debug, Clone)]
pub struct FflogsLeaseEntry {
    pub leased_at: DateTime<Utc>,
    pub client_id: String,
    pub lease_token: String,
}

#[derive(Debug, Clone)]
pub struct IngestSecurityConfig {
    pub require_signature: bool,
    pub shared_secret: String,
    pub clock_skew_seconds: i64,
    pub nonce_ttl_seconds: i64,
    pub require_capabilities_for_protected_endpoints: bool,
    pub capability_secret: String,
    pub capability_session_ttl_seconds: i64,
    pub capability_detail_ttl_seconds: i64,
    pub rate_limit_prefer_client_id: bool,
    pub rate_limits: IngestRateLimits,
}

#[derive(Debug, Clone)]
pub struct IngestRateLimits {
    pub contribute_per_minute: u32,
    pub multiple_per_minute: u32,
    pub players_per_minute: u32,
    pub detail_per_minute: u32,
    pub fflogs_jobs_per_minute: u32,
    pub fflogs_results_per_minute: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IngestRateKey {
    pub endpoint: &'static str,
    pub client_id: String,
}

#[derive(Debug, Clone)]
pub struct IngestRateWindow {
    pub window_started: DateTime<Utc>,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct CachedListingsSnapshot {
    pub revision: u64,
    pub body: Bytes,
    pub etag: Option<String>,
    pub content_encoding: Option<String>,
}

#[derive(Debug, Default)]
pub enum ListingsSnapshotCacheState {
    #[default]
    Empty,
    Building {
        notify: Arc<Notify>,
        /// Dropped when the request building this snapshot is cancelled.
        owner: Weak<()>,
    },
    Ready(CachedListingsSnapshot),
}

#[cfg(test)]
mod tests {
    use super::State;
    use crate::config::Config;
    use crate::web::state::indexes::{
        duplicate_ids_to_remove, is_namespace_not_found_error, listing_ttl_index_model,
        parse_cache_ttl_index_model, players_content_id_dedupe_pipeline,
        players_content_id_unique_index_model, report_parse_summary_identity_index_model,
        PlayerDuplicateGroup,
    };
    use mongodb::{
        bson::{doc, oid::ObjectId},
        Client as MongoClient,
    };
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    fn minimal_config_toml(extra_web: &str) -> String {
        format!(
            r#"
[web]
host = "127.0.0.1:0"
{extra_web}

[mongo]
url = "mongodb://127.0.0.1:27017"
"#
        )
    }

    #[tokio::test]
    async fn state_clamps_zero_fflogs_active_parse_refresh_minutes_to_one() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
fflogs_active_parse_refresh_minutes = 0
"#,
        ))
        .expect("config should parse");

        let mongo = MongoClient::with_uri_str(&config.mongo.url)
            .await
            .expect("mongo client should be constructed");
        let state =
            State::from_config(Arc::new(config), mongo).expect("state should be constructed");

        assert_eq!(state.fflogs_active_parse_refresh_minutes, 1);
    }

    #[tokio::test]
    async fn state_construction_applies_revision_defaults_from_config() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_revision_coalesce_millis = 0
listings_snapshot_collection = "snapshots_test"
listing_source_state_collection = "source_state_test"
listing_snapshot_revision_state_collection = "revision_state_test"
"#,
        ))
        .expect("config should parse");

        let mongo = MongoClient::with_uri_str(&config.mongo.url)
            .await
            .expect("mongo client should be constructed");
        let state =
            State::from_config(Arc::new(config), mongo).expect("state should be constructed");

        assert_eq!(state.current_listings_revision(), 1);
        assert_eq!(
            state.listings_revision_coalesce_window,
            Duration::from_millis(1)
        );
        assert_eq!(state.listings_snapshot_collection_name, "snapshots_test");
        assert_eq!(
            state.listing_source_state_collection_name,
            "source_state_test"
        );
        assert_eq!(
            state.listing_snapshot_revision_state_collection_name,
            "revision_state_test"
        );
    }

    #[tokio::test]
    async fn collection_getters_use_expected_database_and_collection_names() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_collection = "snapshots_test"
listing_source_state_collection = "source_state_test"
listing_snapshot_revision_state_collection = "revision_state_test"
"#,
        ))
        .expect("config should parse");
        let mongo = MongoClient::with_uri_str(&config.mongo.url)
            .await
            .expect("mongo client should be constructed");
        let state =
            State::from_config(Arc::new(config), mongo).expect("state should be constructed");

        assert_eq!(state.collection().namespace().db, "rpf");
        assert_eq!(state.collection().namespace().coll, "listings");
        assert_eq!(state.players_collection().namespace().coll, "players");
        assert_eq!(state.parse_collection().namespace().coll, "parses");
        assert_eq!(
            state.fflogs_job_failure_state_collection().namespace().coll,
            "fflogs_job_failure_state"
        );
        assert_eq!(
            state.report_parse_summary_collection().namespace().coll,
            "report_parse_summaries"
        );
        assert_eq!(
            state.listings_snapshot_collection().namespace().coll,
            "snapshots_test"
        );
        assert_eq!(
            state.listing_source_state_collection().namespace().coll,
            "source_state_test"
        );
        assert_eq!(
            state
                .listing_snapshot_revision_state_collection()
                .namespace()
                .coll,
            "revision_state_test"
        );
    }

    #[tokio::test]
    async fn notify_listings_changed_sets_pending_and_wakes_waiters_only_for_changes() {
        let config: Config = toml::from_str(&minimal_config_toml("")).expect("config should parse");
        let mongo = MongoClient::with_uri_str(&config.mongo.url)
            .await
            .expect("mongo client should be constructed");
        let state =
            State::from_config(Arc::new(config), mongo).expect("state should be constructed");

        state.notify_listings_changed(0);
        assert!(!state.listings_revision_pending.load(Ordering::Relaxed));
        timeout(
            Duration::from_millis(10),
            state.listings_revision_notify.notified(),
        )
        .await
        .expect_err("zero changes should not wake revision waiters");

        state.notify_listings_changed(1);
        assert!(state.listings_revision_pending.load(Ordering::Relaxed));
        timeout(
            Duration::from_millis(50),
            state.listings_revision_notify.notified(),
        )
        .await
        .expect("changed listings should wake revision waiters");
    }

    #[test]
    fn identity_players_unique_index_model_marks_content_id_as_unique() {
        let index = players_content_id_unique_index_model();

        assert_eq!(index.keys.get_i32("content_id").unwrap(), 1);
        assert!(index
            .options
            .as_ref()
            .and_then(|options| options.unique)
            .unwrap_or(false));
    }

    #[test]
    fn report_parse_summary_identity_index_matches_listing_lookup_shape() {
        let index = report_parse_summary_identity_index_model();

        assert_eq!(index.keys, doc! { "normalized_name": 1, "home_world": 1 });
        assert!(
            index
                .options
                .as_ref()
                .and_then(|options| options.unique)
                .unwrap_or(false),
            "report parse summary identity index must stay unique to match ReportWorker"
        );
    }

    #[test]
    fn parse_cache_ttl_index_uses_updated_at_and_configured_hours() {
        let index = parse_cache_ttl_index_model(6);

        assert_eq!(index.keys, doc! { "updated_at": 1 });
        assert_eq!(
            index
                .options
                .as_ref()
                .and_then(|options| options.expire_after),
            Some(Duration::from_secs(6 * 60 * 60))
        );
    }

    #[test]
    fn listing_ttl_index_uses_expires_at_as_exact_expiry_time() {
        let index = listing_ttl_index_model();

        assert_eq!(index.keys, doc! { "expires_at": 1 });
        assert_eq!(
            index
                .options
                .as_ref()
                .and_then(|options| options.expire_after),
            Some(Duration::from_secs(0))
        );
    }

    #[test]
    fn namespace_not_found_command_errors_are_detected() {
        let command_error: mongodb::error::CommandError = serde_json::from_str(
            r#"{"code":26,"codeName":"NamespaceNotFound","errmsg":"ns does not exist: rpf.players"}"#,
        )
        .unwrap();
        let error = mongodb::error::Error::from(mongodb::error::ErrorKind::Command(command_error));

        assert!(is_namespace_not_found_error(&error));
    }

    #[test]
    fn identity_players_dedupe_pipeline_prefers_freshest_player_documents() {
        let pipeline = players_content_id_dedupe_pipeline();

        assert_eq!(pipeline.len(), 4);
        assert_eq!(
            pipeline[1].get_document("$sort").unwrap(),
            &doc! {
                "content_id": 1,
                "identity_observed_at": -1,
                "last_seen": -1,
                "seen_count": -1,
                "_id": 1,
            }
        );
        assert_eq!(
            pipeline[3].get_document("$match").unwrap(),
            &doc! { "count": { "$gt": 1 } }
        );
    }

    #[test]
    fn identity_players_duplicate_cleanup_removes_all_non_winner_rows() {
        let winner = ObjectId::new();
        let loser_a = ObjectId::new();
        let loser_b = ObjectId::new();
        let group = PlayerDuplicateGroup {
            content_id: 7007,
            winner_id: winner,
            duplicate_ids: vec![winner, loser_a, loser_b],
            count: 3,
        };

        let to_remove = duplicate_ids_to_remove(&group);

        assert_eq!(group.count, 3);
        assert_eq!(to_remove, vec![loser_a, loser_b]);
    }
}
