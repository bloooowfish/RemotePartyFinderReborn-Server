use anyhow::{bail, Result};
use serde::Deserialize;
use std::net::SocketAddr;

fn validate_trimmed_non_empty(value: &str, field: &str, context: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field} must be set when {context}");
    }
    if trimmed.len() != value.len() {
        bail!("{field} must not contain leading or trailing whitespace when {context}");
    }
    Ok(())
}

fn is_local_test_webhook_url(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host.eq_ignore_ascii_case("web")
            || host == "127.0.0.1"
            || host == "::1"
            || host == "[::1]"
    })
}

fn default_fflogs_jobs_limit() -> usize {
    20
}

fn default_fflogs_hidden_cache_ttl_hours() -> i64 {
    24
}

fn default_fflogs_active_parse_refresh_minutes() -> i64 {
    15
}

fn default_parse_cache_ttl_hours() -> i64 {
    24
}

fn default_fflogs_non_fetch_failure_abandon_threshold() -> u32 {
    2
}

fn default_listing_upsert_concurrency() -> usize {
    16
}

fn default_player_upsert_concurrency() -> usize {
    32
}

fn default_max_body_bytes_contribute() -> u64 {
    256 * 1024
}

fn default_max_body_bytes_multiple() -> u64 {
    1024 * 1024
}

fn default_max_body_bytes_players() -> u64 {
    512 * 1024
}

fn default_max_body_bytes_detail() -> u64 {
    128 * 1024
}

fn default_max_body_bytes_detail_batch() -> u64 {
    512 * 1024
}

fn default_max_body_bytes_fflogs_results() -> u64 {
    512 * 1024
}

fn default_max_multiple_batch_size() -> usize {
    200
}

fn default_max_players_batch_size() -> usize {
    100
}

fn default_max_fflogs_results_batch_size() -> usize {
    100
}

fn default_max_detail_batch_size() -> usize {
    16
}

fn default_max_detail_member_count() -> usize {
    48
}

fn default_ingest_rate_limit_contribute_per_minute() -> u32 {
    120
}

fn default_ingest_rate_limit_multiple_per_minute() -> u32 {
    60
}

fn default_ingest_rate_limit_players_per_minute() -> u32 {
    120
}

fn default_ingest_rate_limit_detail_per_minute() -> u32 {
    120
}

fn default_ingest_rate_limit_fflogs_jobs_per_minute() -> u32 {
    30
}

fn default_ingest_rate_limit_fflogs_results_per_minute() -> u32 {
    60
}

fn default_ingest_rate_limit_prefer_client_id() -> bool {
    false
}

fn default_ingest_require_signature() -> bool {
    false
}

fn default_ingest_shared_secret() -> String {
    "rpf-reborn-public-ingest-v1".to_string()
}

fn default_ingest_clock_skew_seconds() -> i64 {
    300
}

fn default_ingest_nonce_ttl_seconds() -> i64 {
    300
}

fn default_ingest_require_capabilities_for_protected_endpoints() -> bool {
    false
}

fn default_ingest_capability_secret() -> String {
    String::new()
}

fn default_ingest_capability_session_ttl_seconds() -> i64 {
    60 * 60 * 12
}

fn default_ingest_capability_detail_ttl_seconds() -> i64 {
    60 * 15
}

fn default_monitor_snapshot_interval_seconds() -> u64 {
    0
}

fn default_listings_revision_coalesce_millis() -> u64 {
    250
}

fn default_listings_revision_webhook_url() -> String {
    String::new()
}

fn default_listings_revision_webhook_shared_secret() -> String {
    String::new()
}

fn default_listings_snapshot_source() -> ListingsSnapshotSource {
    ListingsSnapshotSource::Inline
}

fn default_listings_snapshot_collection() -> String {
    "listings_snapshots".to_string()
}

fn default_listings_snapshot_document_id() -> String {
    "current".to_string()
}

fn default_listing_source_state_collection() -> String {
    "listing_source_state".to_string()
}

fn default_listing_source_state_document_id() -> String {
    "current".to_string()
}

fn default_listing_snapshot_revision_state_collection() -> String {
    "listing_snapshot_revision_state".to_string()
}

fn default_materialized_snapshot_reconcile_interval_seconds() -> u64 {
    30
}

fn default_snapshot_refresh_shared_secret() -> String {
    String::new()
}

fn default_snapshot_refresh_client_id() -> String {
    "listings-snapshot-refresh".to_string()
}

fn default_snapshot_refresh_clock_skew_seconds() -> i64 {
    300
}

fn default_snapshot_refresh_nonce_ttl_seconds() -> i64 {
    300
}

fn validate_listings_revision_webhook_url_transport(url: &reqwest::Url) -> Result<()> {
    match url.scheme() {
        "https" => Ok(()),
        "http" if is_local_test_webhook_url(url) => Ok(()),
        _ => bail!(
            "web.listings_revision_webhook_url must use https except for local testing URLs on localhost, 127.0.0.1, [::1], or Docker Compose service host web"
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingsSnapshotSource {
    Inline,
    Materialized,
}

#[derive(Deserialize)]
pub struct Config {
    pub web: Web,
    pub mongo: Mongo,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        self.validate_for_server()
    }

    pub fn validate_for_server(&self) -> Result<()> {
        if self.web.listings_snapshot_source == ListingsSnapshotSource::Materialized
            && self.web.snapshot_refresh_shared_secret.trim().is_empty()
        {
            bail!(
                "web.snapshot_refresh_shared_secret must be set when listings_snapshot_source is materialized"
            );
        }

        if self.web.listings_snapshot_source == ListingsSnapshotSource::Materialized {
            validate_trimmed_non_empty(
                &self.web.listings_snapshot_document_id,
                "web.listings_snapshot_document_id",
                "listings_snapshot_source is materialized",
            )?;
            validate_trimmed_non_empty(
                &self.web.snapshot_refresh_client_id,
                "web.snapshot_refresh_client_id",
                "listings_snapshot_source is materialized",
            )?;
        }

        let webhook_url = self.web.listings_revision_webhook_url.trim();
        if !webhook_url.is_empty() {
            validate_trimmed_non_empty(
                &self.web.listings_revision_webhook_url,
                "web.listings_revision_webhook_url",
                "listings revision webhook is configured",
            )?;
            validate_trimmed_non_empty(
                &self.web.listings_revision_webhook_shared_secret,
                "web.listings_revision_webhook_shared_secret",
                "listings revision webhook is configured",
            )?;
            let Ok(webhook_url) = reqwest::Url::parse(webhook_url) else {
                bail!("web.listings_revision_webhook_url must be an absolute URL");
            };
            validate_listings_revision_webhook_url_transport(&webhook_url)?;
        }

        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Web {
    pub host: SocketAddr,
    #[serde(default = "default_fflogs_jobs_limit")]
    pub fflogs_jobs_limit: usize,
    #[serde(default = "default_fflogs_hidden_cache_ttl_hours")]
    pub fflogs_hidden_cache_ttl_hours: i64,
    #[serde(default = "default_fflogs_active_parse_refresh_minutes")]
    pub fflogs_active_parse_refresh_minutes: i64,
    #[serde(default = "default_parse_cache_ttl_hours")]
    pub parse_cache_ttl_hours: i64,
    #[serde(default = "default_fflogs_non_fetch_failure_abandon_threshold")]
    pub fflogs_non_fetch_failure_abandon_threshold: u32,
    #[serde(default = "default_listing_upsert_concurrency")]
    pub listing_upsert_concurrency: usize,
    #[serde(default = "default_player_upsert_concurrency")]
    pub player_upsert_concurrency: usize,
    #[serde(default = "default_max_body_bytes_contribute")]
    pub max_body_bytes_contribute: u64,
    #[serde(default = "default_max_body_bytes_multiple")]
    pub max_body_bytes_multiple: u64,
    #[serde(default = "default_max_body_bytes_players")]
    pub max_body_bytes_players: u64,
    #[serde(default = "default_max_body_bytes_detail")]
    pub max_body_bytes_detail: u64,
    #[serde(default = "default_max_body_bytes_detail_batch")]
    pub max_body_bytes_detail_batch: u64,
    #[serde(default = "default_max_body_bytes_fflogs_results")]
    pub max_body_bytes_fflogs_results: u64,
    #[serde(default = "default_max_multiple_batch_size")]
    pub max_multiple_batch_size: usize,
    #[serde(default = "default_max_players_batch_size")]
    pub max_players_batch_size: usize,
    #[serde(default = "default_max_fflogs_results_batch_size")]
    pub max_fflogs_results_batch_size: usize,
    #[serde(default = "default_max_detail_batch_size")]
    pub max_detail_batch_size: usize,
    #[serde(default = "default_max_detail_member_count")]
    pub max_detail_member_count: usize,
    #[serde(default = "default_ingest_rate_limit_contribute_per_minute")]
    pub ingest_rate_limit_contribute_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_multiple_per_minute")]
    pub ingest_rate_limit_multiple_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_players_per_minute")]
    pub ingest_rate_limit_players_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_detail_per_minute")]
    pub ingest_rate_limit_detail_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_fflogs_jobs_per_minute")]
    pub ingest_rate_limit_fflogs_jobs_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_fflogs_results_per_minute")]
    pub ingest_rate_limit_fflogs_results_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_prefer_client_id")]
    pub ingest_rate_limit_prefer_client_id: bool,
    #[serde(default = "default_ingest_require_signature")]
    pub ingest_require_signature: bool,
    #[serde(default = "default_ingest_shared_secret")]
    pub ingest_shared_secret: String,
    #[serde(default = "default_ingest_clock_skew_seconds")]
    pub ingest_clock_skew_seconds: i64,
    #[serde(default = "default_ingest_nonce_ttl_seconds")]
    pub ingest_nonce_ttl_seconds: i64,
    #[serde(default = "default_ingest_require_capabilities_for_protected_endpoints")]
    pub ingest_require_capabilities_for_protected_endpoints: bool,
    #[serde(default = "default_ingest_capability_secret")]
    pub ingest_capability_secret: String,
    #[serde(default = "default_ingest_capability_session_ttl_seconds")]
    pub ingest_capability_session_ttl_seconds: i64,
    #[serde(default = "default_ingest_capability_detail_ttl_seconds")]
    pub ingest_capability_detail_ttl_seconds: i64,
    #[serde(default = "default_monitor_snapshot_interval_seconds")]
    pub monitor_snapshot_interval_seconds: u64,
    #[serde(default = "default_listings_revision_coalesce_millis")]
    pub listings_revision_coalesce_millis: u64,
    #[serde(default = "default_listings_revision_webhook_url")]
    pub listings_revision_webhook_url: String,
    #[serde(default = "default_listings_revision_webhook_shared_secret")]
    pub listings_revision_webhook_shared_secret: String,
    #[serde(default = "default_listings_snapshot_source")]
    pub listings_snapshot_source: ListingsSnapshotSource,
    #[serde(default = "default_listings_snapshot_collection")]
    pub listings_snapshot_collection: String,
    #[serde(default = "default_listings_snapshot_document_id")]
    pub listings_snapshot_document_id: String,
    #[serde(default = "default_listing_source_state_collection")]
    pub listing_source_state_collection: String,
    #[serde(default = "default_listing_source_state_document_id")]
    pub listing_source_state_document_id: String,
    #[serde(default = "default_listing_snapshot_revision_state_collection")]
    pub listing_snapshot_revision_state_collection: String,
    #[serde(default = "default_materialized_snapshot_reconcile_interval_seconds")]
    pub materialized_snapshot_reconcile_interval_seconds: u64,
    #[serde(default = "default_snapshot_refresh_shared_secret")]
    pub snapshot_refresh_shared_secret: String,
    #[serde(default = "default_snapshot_refresh_client_id")]
    pub snapshot_refresh_client_id: String,
    #[serde(default = "default_snapshot_refresh_clock_skew_seconds")]
    pub snapshot_refresh_clock_skew_seconds: i64,
    #[serde(default = "default_snapshot_refresh_nonce_ttl_seconds")]
    pub snapshot_refresh_nonce_ttl_seconds: i64,
}

#[derive(Deserialize)]
pub struct Mongo {
    pub url: String,
}

#[cfg(test)]
mod tests;
