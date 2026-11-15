use super::{Config, ListingsSnapshotSource};

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

#[test]
fn config_defaults_to_inline_listings_snapshot_source_and_safe_materialized_names() {
    let config: Config = toml::from_str(&minimal_config_toml("")).expect("config should parse");

    assert_eq!(config.web.parse_cache_ttl_hours, 24);
    assert_eq!(config.web.fflogs_active_parse_refresh_minutes, 60);
    assert_eq!(
        config.web.listings_snapshot_source,
        ListingsSnapshotSource::Inline
    );
    assert_eq!(
        config.web.listings_snapshot_collection,
        "listings_snapshots"
    );
    assert_eq!(config.web.listings_snapshot_document_id, "current");
    assert_eq!(
        config.web.listing_source_state_collection,
        "listing_source_state"
    );
    assert_eq!(config.web.listing_source_state_document_id, "current");
    assert_eq!(
        config.web.listing_snapshot_revision_state_collection,
        "listing_snapshot_revision_state"
    );
    assert_eq!(
        config.web.materialized_snapshot_reconcile_interval_seconds,
        30
    );
    assert_eq!(config.web.listings_revision_coalesce_millis, 250);
    assert_eq!(config.web.snapshot_refresh_shared_secret, "");
    assert_eq!(
        config.web.snapshot_refresh_client_id,
        "listings-snapshot-refresh"
    );
    assert_eq!(config.web.snapshot_refresh_clock_skew_seconds, 300);
    assert_eq!(config.web.snapshot_refresh_nonce_ttl_seconds, 300);
    assert_eq!(config.web.listings_revision_webhook_url, "");
    assert_eq!(config.web.listings_revision_webhook_shared_secret, "");
    config
        .validate_for_server()
        .expect("inline mode may use a blank refresh secret");
}

#[test]
fn config_accepts_parse_cache_ttl_hours_override() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
parse_cache_ttl_hours = 6
"#,
    ))
    .expect("config should parse");

    assert_eq!(config.web.parse_cache_ttl_hours, 6);
}

#[test]
fn fflogs_active_parse_refresh_accepts_minutes_override() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
fflogs_active_parse_refresh_minutes = 5
"#,
    ))
    .expect("config should parse");

    assert_eq!(config.web.fflogs_active_parse_refresh_minutes, 5);
}

#[test]
fn materialized_snapshot_source_rejects_blank_refresh_secret() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "   "
"#,
    ))
    .expect("config should parse");

    let error = config
        .validate_for_server()
        .expect_err("materialized mode must fail closed without refresh secret");

    assert!(
        error.to_string().contains("snapshot_refresh_shared_secret"),
        "unexpected validation error: {error:#}"
    );
}

#[test]
fn materialized_snapshot_source_rejects_whitespace_padded_refresh_identity() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
listings_snapshot_source = "materialized"
listings_snapshot_document_id = " current "
snapshot_refresh_shared_secret = "worker-refresh-secret"
snapshot_refresh_client_id = " listings-snapshot-refresh "
"#,
    ))
    .expect("config should parse");

    let error = config
        .validate_for_server()
        .expect_err("materialized server mode must reject ambiguous refresh identity config");

    let message = error.to_string();
    assert!(
        message.contains("listings_snapshot_document_id")
            || message.contains("snapshot_refresh_client_id"),
        "unexpected validation error: {message}"
    );
}

#[test]
fn listings_revision_webhook_requires_secret_when_url_is_configured() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
listings_revision_webhook_url = "https://web.example/internal/listings/revision"
listings_revision_webhook_shared_secret = "   "
"#,
    ))
    .expect("config should parse");

    let error = config
        .validate_for_server()
        .expect_err("webhook URL must require a shared secret");

    assert!(
        error
            .to_string()
            .contains("listings_revision_webhook_shared_secret"),
        "unexpected validation error: {error:#}"
    );
}

#[test]
fn listings_revision_webhook_accepts_https_url() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
listings_revision_webhook_url = "https://web.example/internal/listings/revision"
listings_revision_webhook_shared_secret = "webhook-secret"
"#,
    ))
    .expect("config should parse");

    config
        .validate_for_server()
        .expect("https webhook URL should be accepted");
}

#[test]
fn listings_revision_webhook_accepts_docker_web_service_http_url() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
listings_revision_webhook_url = "http://web:3000/internal/listings/revision"
listings_revision_webhook_shared_secret = "webhook-secret"
"#,
    ))
    .expect("config should parse");

    config
        .validate_for_server()
        .expect("docker-compose web service webhook URL should be accepted for local tests");
}

#[test]
fn listings_revision_webhook_rejects_non_localhost_http_url() {
    let config: Config = toml::from_str(&minimal_config_toml(
        r#"
listings_revision_webhook_url = "http://web.example/internal/listings/revision"
listings_revision_webhook_shared_secret = "webhook-secret"
"#,
    ))
    .expect("config should parse");

    let error = config
        .validate_for_server()
        .expect_err("webhook URL must use https outside local tests");

    assert!(
        error.to_string().contains("listings_revision_webhook_url"),
        "unexpected validation error: {error:#}"
    );
}

#[test]
fn server_validation_rejects_signature_requirement_with_default_or_blank_secret() {
    for extra in [
        "ingest_require_signature = true",
        "ingest_require_signature = true\ningest_shared_secret = \"   \"",
        "ingest_require_signature_for_fflogs = true",
        "ingest_require_signature_for_tomestone = true",
    ] {
        let config: Config =
            toml::from_str(&minimal_config_toml(extra)).expect("config should parse");
        let error = config
            .validate_for_server()
            .expect_err("signature enforcement without private secret must fail");
        assert!(
            error.to_string().contains("ingest_shared_secret"),
            "unexpected validation error: {error:#}"
        );
    }
}

#[test]
fn server_validation_rejects_capability_enforcement_without_capability_secret() {
    let config: Config = toml::from_str(&minimal_config_toml(
        "ingest_require_capabilities_for_protected_endpoints = true\ningest_capability_secret = \"\"",
    ))
    .expect("config should parse");

    let error = config
        .validate_for_server()
        .expect_err("capability enforcement without secret must fail");
    assert!(
        error.to_string().contains("ingest_capability_secret"),
        "unexpected validation error: {error:#}"
    );
}

#[test]
fn server_validation_allows_open_mode_defaults_with_proxy_trust_and_open_caps() {
    let config: Config = toml::from_str(&minimal_config_toml("")).expect("config should parse");

    assert!(config.web.ingest_trust_loopback_proxy_headers);
    assert_eq!(config.web.ingest_require_signature_for_fflogs, None);
    assert_eq!(config.web.ingest_require_signature_for_tomestone, None);
    assert_eq!(config.web.tomestone_jobs_limit, 20);
    assert_eq!(config.web.tomestone_active_refresh_minutes, 60);
    assert_eq!(config.web.tomestone_non_fetch_failure_abandon_threshold, 2);
    assert_eq!(config.web.max_body_bytes_tomestone_results, 512 * 1024);
    assert_eq!(config.web.max_tomestone_results_batch_size, 100);
    assert_eq!(config.web.ingest_rate_limit_tomestone_jobs_per_minute, 30);
    assert_eq!(
        config.web.ingest_rate_limit_tomestone_results_per_minute,
        60
    );
    assert_eq!(
        config.web.ingest_rate_limit_open_tomestone_jobs_per_minute,
        600
    );
    assert_eq!(
        config
            .web
            .ingest_rate_limit_open_tomestone_results_per_minute,
        1800
    );
    assert!(config.web.ingest_rate_limit_open_contribute_per_minute > 0);
    config
        .validate_for_server()
        .expect("default open config should validate");
}
