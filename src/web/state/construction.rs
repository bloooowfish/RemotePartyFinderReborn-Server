use anyhow::{Context, Result};
use mongodb::Client as MongoClient;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::config::Config;
use crate::web::{IngestRateLimits, IngestSecurityConfig, ListingsRevisionWebhook, State};

impl State {
    pub async fn new(config: Arc<Config>) -> Result<Arc<Self>> {
        config.validate()?;

        let mongo = MongoClient::with_uri_str(&config.mongo.url)
            .await
            .context("could not create mongodb client")?;

        let state = Self::from_config(config, mongo)?;

        state.ensure_indexes().await?;

        Ok(state)
    }

    pub(crate) fn from_config(config: Arc<Config>, mongo: MongoClient) -> Result<Arc<Self>> {
        let (change_tx, _) = tokio::sync::broadcast::channel(64);
        let listings_revision_webhook =
            if config.web.listings_revision_webhook_url.trim().is_empty() {
                None
            } else {
                Some(ListingsRevisionWebhook {
                    client: reqwest::Client::new(),
                    url: reqwest::Url::parse(config.web.listings_revision_webhook_url.trim())
                        .context("could not parse listings revision webhook URL")?,
                    shared_secret: config
                        .web
                        .listings_revision_webhook_shared_secret
                        .trim()
                        .to_string(),
                })
            };
        if config.web.ingest_rate_limit_prefer_client_id && !config.web.ingest_require_signature {
            tracing::warn!(
                "web.ingest_rate_limit_prefer_client_id is true but web.ingest_require_signature is false; client-id rate-limit preference will be ignored"
            );
        }

        let state = Arc::new(Self {
            mongo,
            stats: Default::default(),
            listings_change_channel: change_tx,
            listings_revision: AtomicU64::new(1),
            listings_revision_pending: AtomicBool::new(false),
            listings_revision_notify: Notify::new(),
            listings_revision_coalesce_window: Duration::from_millis(
                config.web.listings_revision_coalesce_millis.max(1),
            ),
            listings_revision_webhook,
            listings_snapshot_cache: Default::default(),
            listings_snapshot_source: config.web.listings_snapshot_source,
            listings_snapshot_collection_name: config.web.listings_snapshot_collection.clone(),
            listings_snapshot_document_id: config.web.listings_snapshot_document_id.clone(),
            listing_source_state_collection_name: config
                .web
                .listing_source_state_collection
                .clone(),
            listing_source_state_document_id: config.web.listing_source_state_document_id.clone(),
            listing_snapshot_revision_state_collection_name: config
                .web
                .listing_snapshot_revision_state_collection
                .clone(),
            materialized_snapshot_reconcile_interval_seconds: config
                .web
                .materialized_snapshot_reconcile_interval_seconds,
            snapshot_refresh_shared_secret: config
                .web
                .snapshot_refresh_shared_secret
                .trim()
                .to_string(),
            snapshot_refresh_client_id: config.web.snapshot_refresh_client_id.clone(),
            snapshot_refresh_clock_skew_seconds: config
                .web
                .snapshot_refresh_clock_skew_seconds
                .max(1),
            snapshot_refresh_nonce_ttl_seconds: config
                .web
                .snapshot_refresh_nonce_ttl_seconds
                .max(1),
            fflogs_jobs_limit: config.web.fflogs_jobs_limit.max(1),
            fflogs_hidden_cache_ttl_hours: config.web.fflogs_hidden_cache_ttl_hours.max(1),
            fflogs_active_parse_refresh_minutes: config
                .web
                .fflogs_active_parse_refresh_minutes
                .max(1),
            parse_cache_ttl_hours: config.web.parse_cache_ttl_hours.max(1),
            fflogs_non_fetch_failure_abandon_threshold: config
                .web
                .fflogs_non_fetch_failure_abandon_threshold
                .max(1),
            listing_upsert_concurrency: config.web.listing_upsert_concurrency.max(1),
            player_upsert_concurrency: config.web.player_upsert_concurrency.max(1),
            max_body_bytes_contribute: config.web.max_body_bytes_contribute.max(1024),
            max_body_bytes_multiple: config.web.max_body_bytes_multiple.max(1024),
            max_body_bytes_players: config.web.max_body_bytes_players.max(1024),
            max_body_bytes_detail: config.web.max_body_bytes_detail.max(1024),
            max_body_bytes_detail_batch: config.web.max_body_bytes_detail_batch.max(1024),
            max_body_bytes_fflogs_results: config.web.max_body_bytes_fflogs_results.max(1024),
            max_multiple_batch_size: config.web.max_multiple_batch_size.max(1),
            max_players_batch_size: config.web.max_players_batch_size.max(1),
            max_fflogs_results_batch_size: config.web.max_fflogs_results_batch_size.max(1),
            max_detail_batch_size: config.web.max_detail_batch_size.max(1),
            max_detail_member_count: config.web.max_detail_member_count.max(1),
            ingest_security: IngestSecurityConfig {
                require_signature: config.web.ingest_require_signature,
                shared_secret: config.web.ingest_shared_secret.clone(),
                clock_skew_seconds: config.web.ingest_clock_skew_seconds.max(1),
                nonce_ttl_seconds: config.web.ingest_nonce_ttl_seconds.max(1),
                require_capabilities_for_protected_endpoints: config
                    .web
                    .ingest_require_capabilities_for_protected_endpoints,
                capability_secret: if config.web.ingest_capability_secret.trim().is_empty() {
                    let generated = format!("runtime-capability-{}", Uuid::new_v4());
                    if config
                        .web
                        .ingest_require_capabilities_for_protected_endpoints
                    {
                        tracing::warn!(
                            "web.ingest_capability_secret is empty; generated an ephemeral runtime secret for protected endpoint capabilities"
                        );
                    }
                    generated
                } else {
                    config.web.ingest_capability_secret.trim().to_string()
                },
                capability_session_ttl_seconds: config
                    .web
                    .ingest_capability_session_ttl_seconds
                    .max(60),
                capability_detail_ttl_seconds: config
                    .web
                    .ingest_capability_detail_ttl_seconds
                    .max(60),
                rate_limit_prefer_client_id: config.web.ingest_rate_limit_prefer_client_id,
                rate_limits: IngestRateLimits {
                    contribute_per_minute: config
                        .web
                        .ingest_rate_limit_contribute_per_minute
                        .max(1),
                    multiple_per_minute: config.web.ingest_rate_limit_multiple_per_minute.max(1),
                    players_per_minute: config.web.ingest_rate_limit_players_per_minute.max(1),
                    detail_per_minute: config.web.ingest_rate_limit_detail_per_minute.max(1),
                    fflogs_jobs_per_minute: config
                        .web
                        .ingest_rate_limit_fflogs_jobs_per_minute
                        .max(1),
                    fflogs_results_per_minute: config
                        .web
                        .ingest_rate_limit_fflogs_results_per_minute
                        .max(1),
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
            monitor_snapshot_interval_seconds: config.web.monitor_snapshot_interval_seconds,
        });

        Ok(state)
    }
}
