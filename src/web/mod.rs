use anyhow::Result;
use chrono::{DateTime, Utc};
use mongodb::Client as MongoClient;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
    time::Duration,
};
use tokio::sync::broadcast::Sender;
use tokio::sync::{watch, Notify, RwLock};
use warp::hyper::body::Bytes;

use crate::config::{Config, ListingsSnapshotSource};
use crate::stats::CachedStatistics;
use crate::ws::WsClientGate;
use ingest_guard::{NonceCache, RateWindowCache};

pub mod background;
mod contribute;
mod fflogs_ingest;
pub mod ingest_guard;
mod parse_ingest_common;
pub mod routes;
mod state;
#[cfg(test)]
pub(crate) mod test_support;
mod tomestone_ingest;

pub const FFLOGS_LEASE_TTL_MINUTES: i64 = 3;
pub const FFLOGS_LEASE_SWEEP_INTERVAL_SECONDS: u64 = 30;
pub const TOMESTONE_LEASE_TTL_MINUTES: i64 = 5;
pub const TOMESTONE_LEASE_SWEEP_INTERVAL_SECONDS: u64 = 30;

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(err) = result {
                    tracing::warn!(error = ?err, "ctrl_c signal listener failed");
                }
            }
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = ?err, "ctrl_c signal listener failed");
        }
    }
    tracing::info!("shutdown signal received, gracefully shutting down HTTP server");
}

pub async fn start(config: Arc<Config>) -> Result<()> {
    let state = State::new(Arc::clone(&config)).await?;

    // Supervised background tasks
    let mut stats_handle = background::spawn_stats_task(Arc::clone(&state));
    let mut lease_sweeper_handle = background::spawn_fflogs_lease_sweeper_task(Arc::clone(&state));
    let mut tomestone_lease_sweeper_handle =
        background::spawn_tomestone_lease_sweeper_task(Arc::clone(&state));
    let mut revision_publisher_handle =
        background::spawn_listings_revision_publisher_task(Arc::clone(&state));
    let _monitor_handle = background::spawn_monitor_snapshot_task(Arc::clone(&state));
    let reconcile_enabled =
        state.listings_snapshot_source == ListingsSnapshotSource::Materialized;
    let mut reconcile_handle =
        background::spawn_materialized_snapshot_reconcile_task(Arc::clone(&state));

    tracing::info!("listening at {}", config.web.host);
    let (bound_addr, server) =
        warp::serve(routes::router(state)).bind_with_graceful_shutdown(config.web.host, shutdown_signal());
    tracing::info!("bound HTTP server on {bound_addr}");
    tokio::select! {
        _ = server => Ok(()),
        result = &mut stats_handle => {
            Err(background::format_supervised_task_exit("stats_task", result))
        }
        result = &mut lease_sweeper_handle => {
            Err(background::format_supervised_task_exit("fflogs_lease_sweeper_task", result))
        }
        result = &mut tomestone_lease_sweeper_handle => {
            Err(background::format_supervised_task_exit("tomestone_lease_sweeper_task", result))
        }
        result = &mut revision_publisher_handle => {
            Err(background::format_supervised_task_exit("listings_revision_publisher_task", result))
        }
        result = &mut reconcile_handle, if reconcile_enabled => {
            Err(background::format_supervised_task_exit("materialized_snapshot_reconcile_task", result))
        }
    }
}

pub struct State {
    pub mongo: MongoClient,
    pub stats: RwLock<Option<Arc<CachedStatistics>>>,
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
    pub tomestone_jobs_limit: usize,
    pub tomestone_active_refresh_minutes: i64,
    pub tomestone_non_fetch_failure_abandon_threshold: u32,
    pub listing_upsert_concurrency: usize,
    pub player_upsert_concurrency: usize,
    pub max_body_bytes_contribute: u64,
    pub max_body_bytes_multiple: u64,
    pub max_body_bytes_players: u64,
    pub max_body_bytes_detail: u64,
    pub max_body_bytes_detail_batch: u64,
    pub max_body_bytes_fflogs_results: u64,
    pub max_body_bytes_tomestone_results: u64,
    pub max_multiple_batch_size: usize,
    pub max_players_batch_size: usize,
    pub max_fflogs_results_batch_size: usize,
    pub max_tomestone_results_batch_size: usize,
    pub max_detail_batch_size: usize,
    pub max_detail_member_count: usize,
    pub ingest_security: IngestSecurityConfig,
    pub ingest_rate_windows: RwLock<RateWindowCache>,
    pub ingest_nonces: RwLock<NonceCache>,
    pub snapshot_refresh_nonces: RwLock<NonceCache>,
    pub ws_clients: WsClientGate,
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
    /// in-flight Tomestone job lease map to avoid duplicate dispatch across workers
    pub tomestone_job_leases: RwLock<HashMap<TomestoneLeaseKey, TomestoneLeaseEntry>>,
    /// total number of Tomestone jobs dispatched to workers
    pub tomestone_jobs_dispatched_total: AtomicU64,
    /// total number of Tomestone result payload items received from workers
    pub tomestone_results_received_total: AtomicU64,
    /// total number of Tomestone result payload items rejected by lease/token validation
    pub tomestone_results_rejected_total: AtomicU64,
    /// total number of Tomestone lease items released via explicit abandon endpoint
    pub tomestone_leases_abandoned_total: AtomicU64,
    /// total number of Tomestone lease abandon items rejected by validation
    pub tomestone_leases_abandon_rejected_total: AtomicU64,
    /// periodic monitor snapshot interval (seconds). set 0 to disable.
    pub monitor_snapshot_interval_seconds: u64,
}

#[derive(Default)]
pub struct WebhookDispatchState {
    pub latest_revision: AtomicU64,
    pub notify: Notify,
    pub worker_started: AtomicBool,
}

#[derive(Clone)]
pub struct ListingsRevisionWebhook {
    pub client: reqwest::Client,
    pub url: reqwest::Url,
    pub shared_secret: String,
    pub dispatch: Arc<WebhookDispatchState>,
}

impl ListingsRevisionWebhook {
    pub fn new(client: reqwest::Client, url: reqwest::Url, shared_secret: String) -> Self {
        Self {
            client,
            url,
            shared_secret,
            dispatch: Arc::new(WebhookDispatchState::default()),
        }
    }
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
    pub processor_kind: String,
    pub encounter_id: Option<u32>,
    pub secondary_encounter_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TomestoneLeaseKey {
    pub content_id: u64,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub encounter_id: u32,
}

#[derive(Debug, Clone)]
pub struct TomestoneLeaseEntry {
    pub leased_at: DateTime<Utc>,
    pub client_id: String,
    pub lease_token: String,
    pub processor_kind: String,
    pub secondary_encounter_id: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IngestSecurityConfig {
    pub require_signature: bool,
    pub require_signature_for_fflogs: Option<bool>,
    pub require_signature_for_tomestone: Option<bool>,
    pub shared_secret: String,
    pub clock_skew_seconds: i64,
    pub nonce_ttl_seconds: i64,
    pub require_capabilities_for_protected_endpoints: bool,
    pub capability_secret: String,
    pub capability_session_ttl_seconds: i64,
    pub capability_detail_ttl_seconds: i64,
    pub rate_limit_prefer_client_id: bool,
    pub trust_loopback_proxy_headers: bool,
    pub trusted_relay_shared_secret: String,
    pub rate_limits: IngestRateLimits,
    pub open_aggregate_rate_limits: IngestRateLimits,
}

impl IngestSecurityConfig {
    pub fn effective_require_signature(&self, endpoint: ingest_guard::IngestEndpoint) -> bool {
        if endpoint.is_fflogs() {
            self.require_signature_for_fflogs
                .unwrap_or(self.require_signature)
        } else if endpoint.is_tomestone() {
            self.require_signature_for_tomestone
                .unwrap_or(self.require_signature)
        } else {
            self.require_signature
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestRateLimits {
    pub contribute_per_minute: u32,
    pub multiple_per_minute: u32,
    pub players_per_minute: u32,
    pub detail_per_minute: u32,
    pub fflogs_jobs_per_minute: u32,
    pub fflogs_results_per_minute: u32,
    pub tomestone_jobs_per_minute: u32,
    pub tomestone_results_per_minute: u32,
}

impl IngestRateLimits {
    pub fn default_open_aggregate() -> Self {
        Self {
            contribute_per_minute: 1800,
            multiple_per_minute: 900,
            players_per_minute: 1800,
            detail_per_minute: 1800,
            fflogs_jobs_per_minute: 600,
            fflogs_results_per_minute: 1800,
            tomestone_jobs_per_minute: 600,
            tomestone_results_per_minute: 1800,
        }
    }
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
    pub body_gzip: Option<Bytes>,
    pub etag: Option<String>,
    pub content_encoding: Option<String>,
}

#[derive(Debug, Default)]
pub enum ListingsSnapshotCacheState {
    #[default]
    Empty,
    Building {
        done_rx: watch::Receiver<bool>,
        stale: Option<CachedListingsSnapshot>,
    },
    Ready(CachedListingsSnapshot),
}

#[cfg(test)]
mod tests {
    use super::State;
    use crate::config::Config;
    use crate::web::state::indexes::{
        PlayerDuplicateGroup, duplicate_ids_to_remove, fflogs_job_failure_state_index_model,
        is_index_not_found_error, is_namespace_not_found_error,
        listing_stats_history_identity_index_model, listing_ttl_index_model,
        parse_cache_ttl_index_model, players_content_id_dedupe_pipeline,
        players_content_id_unique_index_model, players_last_seen_ttl_index_model,
        tomestone_job_failure_state_index_model,
    };
    use mongodb::{
        Client as MongoClient,
        bson::{doc, oid::ObjectId},
    };
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
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
            state.tomestone_job_failure_collection().namespace().coll,
            "tomestone_job_failure_state"
        );
        assert_eq!(
            state.listing_stats_history_collection().namespace().coll,
            "listing_stats_history"
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
        assert!(
            index
                .options
                .as_ref()
                .and_then(|options| options.unique)
                .unwrap_or(false)
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
    fn index_not_found_command_errors_are_detected() {
        let command_error: mongodb::error::CommandError = serde_json::from_str(
            r#"{"code":27,"codeName":"IndexNotFound","errmsg":"index not found with name [old_idx]"}"#,
        )
        .unwrap();
        let error = mongodb::error::Error::from(mongodb::error::ErrorKind::Command(command_error));

        assert!(is_index_not_found_error(&error));
    }

    #[test]
    fn job_failure_state_indexes_lead_with_target_identity_and_last_failed_at() {
        let fflogs_index = fflogs_job_failure_state_index_model();
        assert_eq!(
            fflogs_index.keys,
            doc! {
                "content_id": 1,
                "zone_id": 1,
                "difficulty_id": 1,
                "processor_kind": 1,
                "final_for_processor": 1,
                "last_failed_at": -1,
            }
        );

        let tomestone_index = tomestone_job_failure_state_index_model();
        assert_eq!(
            tomestone_index.keys,
            doc! {
                "content_id": 1,
                "zone_id": 1,
                "difficulty_id": 1,
                "encounter_id": 1,
                "processor_kind": 1,
                "final_for_processor": 1,
                "last_failed_at": -1,
            }
        );
    }

    #[test]
    fn players_last_seen_ttl_index_expires_after_180_days() {
        let index = players_last_seen_ttl_index_model();
        assert_eq!(index.keys, doc! { "last_seen": 1 });
        assert_eq!(
            index
                .options
                .as_ref()
                .and_then(|options| options.expire_after),
            Some(Duration::from_secs(180 * 86_400))
        );
    }

    #[test]
    fn listing_stats_history_identity_index_includes_last_server_restart() {
        let index = listing_stats_history_identity_index_model();
        assert_eq!(
            index.keys,
            doc! {
                "listing_id": 1,
                "last_server_restart": 1,
                "created_world": 1,
            }
        );
        assert_eq!(
            index.options.as_ref().and_then(|options| options.unique),
            Some(true)
        );
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

    #[tokio::test]
    async fn deterministic_capability_secret_is_derived_without_random_uuid() {
        let config = std::sync::Arc::new(
            toml::from_str::<crate::config::Config>(&minimal_config_toml("")).unwrap(),
        );
        let mongo = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017")
            .await
            .unwrap();
        let first = State::from_config(std::sync::Arc::clone(&config), mongo.clone()).unwrap();
        let second = State::from_config(config, mongo).unwrap();

        assert!(!first.ingest_security.capability_secret.is_empty());
        assert_eq!(
            first.ingest_security.capability_secret,
            second.ingest_security.capability_secret
        );
    }
}
