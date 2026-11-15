use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use base64::{
    decode_config as base64_decode_config, encode as base64_encode,
    encode_config as base64_encode_config, URL_SAFE_NO_PAD,
};
use chrono::{DateTime, TimeDelta, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use super::{IngestRateKey, IngestRateLimits, IngestRateWindow, State};

const WINDOW_SECONDS: i64 = 60;
const OPEN_GLOBAL_IDENTITY: &str = "open:global";
pub(crate) const MAX_SECURITY_CACHE_ENTRIES: usize = 50_000;

#[derive(Debug, Clone, Copy)]
pub enum IngestEndpoint {
    Contribute,
    ContributeMultiple,
    ContributePlayers,
    ContributeDetail,
    ContributeFflogsJobs,
    ContributeFflogsResults,
    ContributeFflogsLeasesAbandon,
    ContributeTomestoneJobs,
    ContributeTomestoneResults,
    ContributeTomestoneLeasesAbandon,
}

impl IngestEndpoint {
    pub fn is_fflogs(self) -> bool {
        matches!(
            self,
            Self::ContributeFflogsJobs
                | Self::ContributeFflogsResults
                | Self::ContributeFflogsLeasesAbandon
        )
    }

    pub fn is_tomestone(self) -> bool {
        matches!(
            self,
            Self::ContributeTomestoneJobs
                | Self::ContributeTomestoneResults
                | Self::ContributeTomestoneLeasesAbandon
        )
    }

    fn key(self) -> &'static str {
        match self {
            Self::Contribute => "contribute",
            Self::ContributeMultiple => "contribute_multiple",
            Self::ContributePlayers => "contribute_players",
            Self::ContributeDetail => "contribute_detail",
            Self::ContributeFflogsJobs => "contribute_fflogs_jobs",
            Self::ContributeFflogsResults => "contribute_fflogs_results",
            Self::ContributeFflogsLeasesAbandon => "contribute_fflogs_leases_abandon",
            Self::ContributeTomestoneJobs => "contribute_tomestone_jobs",
            Self::ContributeTomestoneResults => "contribute_tomestone_results",
            Self::ContributeTomestoneLeasesAbandon => "contribute_tomestone_leases_abandon",
        }
    }

    fn limit_per_minute(self, limits: &IngestRateLimits) -> u32 {
        match self {
            Self::Contribute => limits.contribute_per_minute,
            Self::ContributeMultiple => limits.multiple_per_minute,
            Self::ContributePlayers => limits.players_per_minute,
            Self::ContributeDetail => limits.detail_per_minute,
            Self::ContributeFflogsJobs => limits.fflogs_jobs_per_minute,
            Self::ContributeFflogsResults => limits.fflogs_results_per_minute,
            Self::ContributeFflogsLeasesAbandon => limits.fflogs_results_per_minute,
            Self::ContributeTomestoneJobs => limits.tomestone_jobs_per_minute,
            Self::ContributeTomestoneResults => limits.tomestone_results_per_minute,
            Self::ContributeTomestoneLeasesAbandon => limits.tomestone_results_per_minute,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GuardContext {
    pub client_id: String,
    pub request_identity: String,
    pub auth_failure_identity: String,
    pub signature_verified: bool,
    pub open_aggregate_limited: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityScope {
    Detail,
    FflogsJobs,
    FflogsResults,
    FflogsLeasesAbandon,
    TomestoneJobs,
    TomestoneResults,
    TomestoneLeasesAbandon,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityClaims {
    pub client_id: String,
    pub scope: CapabilityScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    pub issued_at: i64,
    pub expires_at: i64,
}

impl CapabilityClaims {
    pub fn new(
        client_id: String,
        scope: CapabilityScope,
        resource_id: Option<String>,
        issued_at: i64,
        expires_at: i64,
    ) -> Self {
        Self {
            client_id,
            scope,
            resource_id,
            issued_at,
            expires_at,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GuardError {
    BadRequest(&'static str),
    Unauthorized(&'static str),
    Forbidden(&'static str),
    TooManyRequests { retry_after_seconds: u64 },
}

pub async fn authorize_request(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    method: &str,
    path: &str,
) -> Result<GuardContext, GuardError> {
    authorize_request_with_rate_limit_units(state, endpoint, headers, remote_addr, method, path, 1)
        .await
}

pub async fn authorize_request_with_rate_limit_units(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    method: &str,
    path: &str,
    rate_limit_units: usize,
) -> Result<GuardContext, GuardError> {
    let context =
        authenticate_request(state, endpoint, headers, remote_addr, method, path).await?;
    enforce_guard_rate_limit_units(state, endpoint, &context, rate_limit_units).await?;
    Ok(context)
}

pub async fn authenticate_request(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    method: &str,
    path: &str,
) -> Result<GuardContext, GuardError> {
    let client_id = extract_client_id(headers);
    let require_signature = state.ingest_security.effective_require_signature(endpoint);
    let trust_loopback = state.ingest_security.trust_loopback_proxy_headers;
    let trusted_relay_secret = state.ingest_security.trusted_relay_shared_secret.as_str();
    let auth_failure_identity = extract_auth_failure_identity_with_relay(
        headers,
        remote_addr,
        trust_loopback,
        trusted_relay_secret,
    );
    let should_attempt_signature = require_signature
        || (state.ingest_security.rate_limit_prefer_client_id && has_signature_headers(headers));

    // Verify HMAC, clock skew, and nonce BEFORE touching auth-failure:ip:* buckets so
    // unauthenticated noise behind a loopback proxy cannot lock out valid signed clients.
    let signature_verified = if should_attempt_signature {
        let auth_result = match verify_signature(state, headers, method, path, &client_id) {
            Ok(()) => enforce_nonce(state, headers, &client_id).await,
            Err(error) => Err(error),
        };
        if let Err(error) = auth_result {
            enforce_rate_limit_with_limits(
                state,
                endpoint,
                &auth_failure_identity,
                1,
                &state.ingest_security.rate_limits,
            )
            .await?;
            return Err(error);
        }
        true
    } else {
        false
    };

    let prefer_client_id = effective_rate_limit_prefer_client_id(
        state.ingest_security.rate_limit_prefer_client_id,
        signature_verified,
    );
    let request_identity = extract_request_identity_with_relay(
        headers,
        remote_addr,
        prefer_client_id,
        trust_loopback,
        trusted_relay_secret,
    );

    Ok(GuardContext {
        client_id,
        request_identity,
        auth_failure_identity,
        signature_verified,
        open_aggregate_limited: !signature_verified,
    })
}

pub async fn reserve_rate_limit_units(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    guard_ctx: &GuardContext,
    units: usize,
) -> Result<(), GuardError> {
    enforce_guard_rate_limit_units(state, endpoint, guard_ctx, units).await
}

pub async fn record_capability_auth_failure(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    guard_ctx: &GuardContext,
) -> Result<(), GuardError> {
    enforce_rate_limit_with_limits(
        state,
        endpoint,
        &guard_ctx.auth_failure_identity,
        1,
        &state.ingest_security.rate_limits,
    )
    .await
}

pub fn guard_error_reply(error: GuardError) -> warp::reply::Response {
    match error {
        GuardError::BadRequest(message) => {
            warp::reply::with_status(message, StatusCode::BAD_REQUEST).into_response()
        }
        GuardError::Unauthorized(message) => {
            warp::reply::with_status(message, StatusCode::UNAUTHORIZED).into_response()
        }
        GuardError::Forbidden(message) => {
            warp::reply::with_status(message, StatusCode::FORBIDDEN).into_response()
        }
        GuardError::TooManyRequests {
            retry_after_seconds,
        } => {
            let base = warp::reply::with_status("too many requests", StatusCode::TOO_MANY_REQUESTS);
            warp::reply::with_header(base, "Retry-After", retry_after_seconds.to_string())
                .into_response()
        }
    }
}

fn has_signature_headers(headers: &HeaderMap) -> bool {
    headers.contains_key("x-rpf-signature")
        || headers.contains_key("x-rpf-timestamp")
        || headers.contains_key("x-rpf-nonce")
        || headers.contains_key("x-rpf-signature-version")
}

fn parse_single_ip_header(raw: &str) -> Option<IpAddr> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<IpAddr>().ok()
}

fn parse_forwarded_for_header(raw: &str) -> Option<IpAddr> {
    raw.split(',')
        .map(str::trim)
        .find(|part| !part.is_empty())
        .and_then(|first| first.parse::<IpAddr>().ok())
}

fn has_valid_relay_secret(headers: &HeaderMap, expected_secret: &str) -> bool {
    if expected_secret.is_empty() {
        return false;
    }
    let Some(provided) = header_value(headers, "x-rpf-relay-secret") else {
        return false;
    };
    let expected_digest = Sha256::digest(expected_secret.as_bytes());
    let provided_digest = Sha256::digest(provided.as_bytes());
    constant_time_eq(&provided_digest, &expected_digest)
}

pub fn extract_client_ip_with_relay(
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    trust_loopback_proxy_headers: bool,
    trusted_relay_secret: &str,
) -> Option<IpAddr> {
    if has_valid_relay_secret(headers, trusted_relay_secret) {
        if let Some(ip) =
            header_value(headers, "x-rpf-forwarded-for").and_then(parse_forwarded_for_header)
        {
            return Some(ip);
        }
    }
    extract_client_ip(headers, remote_addr, trust_loopback_proxy_headers)
}

pub fn extract_client_ip(
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    trust_loopback_proxy_headers: bool,
) -> Option<IpAddr> {
    let peer_ip = remote_addr.map(|addr| addr.ip());
    if trust_loopback_proxy_headers && peer_ip.is_some_and(|ip| ip.is_loopback()) {
        if let Some(ip) = header_value(headers, "cf-connecting-ip").and_then(parse_single_ip_header)
        {
            return Some(ip);
        }
        if let Some(ip) =
            header_value(headers, "x-rpf-forwarded-for").and_then(parse_forwarded_for_header)
        {
            return Some(ip);
        }
        if let Some(ip) =
            header_value(headers, "x-forwarded-for").and_then(parse_forwarded_for_header)
        {
            return Some(ip);
        }
    }
    peer_ip
}

fn extract_client_id(headers: &HeaderMap) -> String {
    header_value(headers, "x-rpf-client-id")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "anonymous".to_string())
}

pub fn extract_request_identity(
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    prefer_client_id: bool,
) -> String {
    extract_request_identity_with_trust(headers, remote_addr, prefer_client_id, true)
}

pub fn extract_request_identity_with_trust(
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    prefer_client_id: bool,
    trust_loopback_proxy_headers: bool,
) -> String {
    extract_request_identity_with_relay(
        headers,
        remote_addr,
        prefer_client_id,
        trust_loopback_proxy_headers,
        "",
    )
}

fn extract_request_identity_with_relay(
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    prefer_client_id: bool,
    trust_loopback_proxy_headers: bool,
    trusted_relay_secret: &str,
) -> String {
    if prefer_client_id {
        let client_id = extract_client_id(headers);
        if client_id != "anonymous" {
            return format!("client:{client_id}");
        }
    }

    if let Some(client_ip) = extract_client_ip_with_relay(
        headers,
        remote_addr,
        trust_loopback_proxy_headers,
        trusted_relay_secret,
    ) {
        return format!("ip:{client_ip}");
    }

    format!("client:{}", extract_client_id(headers))
}

fn extract_auth_failure_identity_with_relay(
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    trust_loopback_proxy_headers: bool,
    trusted_relay_secret: &str,
) -> String {
    match extract_client_ip_with_relay(
        headers,
        remote_addr,
        trust_loopback_proxy_headers,
        trusted_relay_secret,
    ) {
        Some(client_ip) => format!("auth-failure:ip:{client_ip}"),
        None => "auth-failure:ip:unknown".to_string(),
    }
}

fn effective_rate_limit_prefer_client_id(
    configured_prefer_client_id: bool,
    require_signature: bool,
) -> bool {
    configured_prefer_client_id && require_signature
}

fn header_value<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|value| value.to_str().ok())
}

fn verify_signature(
    state: &Arc<State>,
    headers: &HeaderMap,
    method: &str,
    path: &str,
    client_id: &str,
) -> Result<(), GuardError> {
    if let Some(version) = header_value(headers, "x-rpf-signature-version") {
        if version != "v1" {
            return Err(GuardError::Unauthorized("unsupported signature version"));
        }
    }

    let timestamp_raw = header_value(headers, "x-rpf-timestamp")
        .ok_or(GuardError::Unauthorized("missing timestamp"))?;
    let nonce =
        header_value(headers, "x-rpf-nonce").ok_or(GuardError::Unauthorized("missing nonce"))?;
    let signature = header_value(headers, "x-rpf-signature")
        .ok_or(GuardError::Unauthorized("missing signature"))?;

    let timestamp = timestamp_raw
        .parse::<i64>()
        .map_err(|_| GuardError::BadRequest("invalid timestamp"))?;

    let now = Utc::now().timestamp();
    let Some(skew) = now.checked_sub(timestamp).and_then(|diff| diff.checked_abs()) else {
        return Err(GuardError::Forbidden("timestamp skew exceeded"));
    };
    if skew > state.ingest_security.clock_skew_seconds {
        return Err(GuardError::Forbidden("timestamp skew exceeded"));
    }

    let payload = format!(
        "{}\n{}\n{}\n{}\n{}",
        method, path, timestamp_raw, nonce, client_id
    );
    let expected = compute_signature(&state.ingest_security.shared_secret, &payload)
        .map_err(|_| GuardError::Unauthorized("signature setup invalid"))?;

    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(GuardError::Unauthorized("invalid signature"));
    }

    Ok(())
}

pub(crate) fn insert_bounded_nonce(
    nonces: &mut HashMap<String, DateTime<Utc>>,
    nonce_key: String,
    now: DateTime<Utc>,
    ttl: TimeDelta,
) -> Result<(), GuardError> {
    nonces.retain(|_, seen_at| *seen_at + ttl > now);

    if nonces.contains_key(&nonce_key) {
        return Err(GuardError::Forbidden("replayed nonce"));
    }

    while nonces.len() >= MAX_SECURITY_CACHE_ENTRIES {
        let Some(oldest_key) = nonces
            .iter()
            .min_by_key(|(_, seen_at)| *seen_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        nonces.remove(&oldest_key);
    }

    nonces.insert(nonce_key, now);
    Ok(())
}

async fn enforce_nonce(
    state: &Arc<State>,
    headers: &HeaderMap,
    client_id: &str,
) -> Result<(), GuardError> {
    let nonce =
        header_value(headers, "x-rpf-nonce").ok_or(GuardError::Unauthorized("missing nonce"))?;
    let nonce_key = format!("{}:{}", client_id, nonce);

    let Some(ttl) = TimeDelta::try_seconds(state.ingest_security.nonce_ttl_seconds) else {
        return Err(GuardError::BadRequest("invalid nonce ttl"));
    };

    let now = Utc::now();
    let mut nonces = state.ingest_nonces.write().await;
    insert_bounded_nonce(&mut nonces, nonce_key, now, ttl)
}

async fn enforce_rate_limit_with_limits(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    request_identity: &str,
    units: usize,
    limits: &IngestRateLimits,
) -> Result<(), GuardError> {
    if units == 0 {
        return Ok(());
    }

    let now = Utc::now();
    let units = u32::try_from(units).unwrap_or(u32::MAX);
    let key = IngestRateKey {
        endpoint: endpoint.key(),
        client_id: request_identity.to_string(),
    };
    let limit = endpoint.limit_per_minute(limits).max(1);
    let mut windows = state.ingest_rate_windows.write().await;
    prune_stale_rate_windows(&mut windows, now);
    check_rate_window_available(&mut windows, &key, limit, units, now)?;
    record_rate_window_units(&mut windows, key, units, now);
    Ok(())
}

async fn enforce_guard_rate_limit_units(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    guard_ctx: &GuardContext,
    units: usize,
) -> Result<(), GuardError> {
    if units == 0 {
        return Ok(());
    }

    let now = Utc::now();
    let units = u32::try_from(units).unwrap_or(u32::MAX);
    let identity_key = IngestRateKey {
        endpoint: endpoint.key(),
        client_id: guard_ctx.request_identity.clone(),
    };
    let identity_limit = endpoint
        .limit_per_minute(&state.ingest_security.rate_limits)
        .max(1);
    let open_key = IngestRateKey {
        endpoint: endpoint.key(),
        client_id: OPEN_GLOBAL_IDENTITY.to_string(),
    };
    let open_limit = endpoint
        .limit_per_minute(&state.ingest_security.open_aggregate_rate_limits)
        .max(1);

    let mut windows = state.ingest_rate_windows.write().await;
    prune_stale_rate_windows(&mut windows, now);

    check_rate_window_available(&mut windows, &identity_key, identity_limit, units, now)?;
    if guard_ctx.open_aggregate_limited {
        check_rate_window_available(&mut windows, &open_key, open_limit, units, now)?;
    }

    record_rate_window_units(&mut windows, identity_key, units, now);
    if guard_ctx.open_aggregate_limited {
        record_rate_window_units(&mut windows, open_key, units, now);
    }

    Ok(())
}

fn prune_stale_rate_windows(
    windows: &mut HashMap<IngestRateKey, IngestRateWindow>,
    now: DateTime<Utc>,
) {
    let stale_after = TimeDelta::seconds(WINDOW_SECONDS * 2);
    windows.retain(|_, window| now - window.window_started <= stale_after);
}

fn check_rate_window_available(
    windows: &mut HashMap<IngestRateKey, IngestRateWindow>,
    key: &IngestRateKey,
    limit: u32,
    units: u32,
    now: DateTime<Utc>,
) -> Result<(), GuardError> {
    let Some(window) = windows.get_mut(key) else {
        if units > limit {
            return Err(GuardError::TooManyRequests {
                retry_after_seconds: WINDOW_SECONDS as u64,
            });
        }
        return Ok(());
    };

    let elapsed = now - window.window_started;
    if elapsed.num_seconds() >= WINDOW_SECONDS {
        window.window_started = now;
        window.count = 0;
    }

    if window.count.saturating_add(units) > limit {
        let retry_after = remaining_window_seconds(window.window_started, now);
        return Err(GuardError::TooManyRequests {
            retry_after_seconds: retry_after,
        });
    }

    Ok(())
}

fn record_rate_window_units(
    windows: &mut HashMap<IngestRateKey, IngestRateWindow>,
    key: IngestRateKey,
    units: u32,
    now: DateTime<Utc>,
) {
    if let Some(window) = windows.get_mut(&key) {
        if (now - window.window_started).num_seconds() >= WINDOW_SECONDS {
            window.window_started = now;
            window.count = 0;
        }
        window.count = window.count.saturating_add(units);
        return;
    }

    while windows.len() >= MAX_SECURITY_CACHE_ENTRIES {
        let Some(oldest_key) = windows
            .iter()
            .min_by_key(|(_, window)| window.window_started)
            .map(|(existing_key, _)| existing_key.clone())
        else {
            break;
        };
        windows.remove(&oldest_key);
    }

    windows.insert(
        key,
        IngestRateWindow {
            window_started: now,
            count: units,
        },
    );
}

fn remaining_window_seconds(window_started: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    let elapsed = (now - window_started).num_seconds().max(0);
    let remaining = (WINDOW_SECONDS - elapsed).max(1);
    remaining as u64
}

fn compute_signature(secret: &str, payload: &str) -> Result<String, hmac::digest::InvalidLength> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(payload.as_bytes());
    let signature = mac.finalize().into_bytes();
    Ok(base64_encode(signature))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }

    diff == 0
}

pub fn issue_capability_token(
    secret: &str,
    claims: &CapabilityClaims,
) -> Result<String, GuardError> {
    let payload_json = serde_json::to_vec(claims)
        .map_err(|_| GuardError::Unauthorized("invalid capability claims"))?;
    let payload = base64_encode_config(payload_json, URL_SAFE_NO_PAD);
    let signature = compute_capability_signature(secret, &payload)
        .map_err(|_| GuardError::Unauthorized("capability setup invalid"))?;
    Ok(format!("rpf1.{}.{}", payload, signature))
}

pub fn validate_capability_token(
    secret: &str,
    token: &str,
    expected_scope: CapabilityScope,
    expected_client_id: &str,
    expected_resource_id: Option<&str>,
    now: i64,
) -> Result<CapabilityClaims, GuardError> {
    let mut parts = token.split('.');
    let Some(version) = parts.next() else {
        return Err(GuardError::Unauthorized("invalid capability token"));
    };
    let Some(payload) = parts.next() else {
        return Err(GuardError::Unauthorized("invalid capability token"));
    };
    let Some(signature) = parts.next() else {
        return Err(GuardError::Unauthorized("invalid capability token"));
    };
    if parts.next().is_some() || version != "rpf1" {
        return Err(GuardError::Unauthorized("invalid capability token"));
    }

    let expected_signature = compute_capability_signature(secret, payload)
        .map_err(|_| GuardError::Unauthorized("capability setup invalid"))?;
    if !constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
        return Err(GuardError::Unauthorized("invalid capability token"));
    }

    let payload_json = base64_decode_config(payload, URL_SAFE_NO_PAD)
        .map_err(|_| GuardError::Unauthorized("invalid capability token"))?;
    let claims: CapabilityClaims = serde_json::from_slice(&payload_json)
        .map_err(|_| GuardError::Unauthorized("invalid capability token"))?;

    if claims.scope != expected_scope {
        return Err(GuardError::Forbidden("capability scope mismatch"));
    }
    if claims.client_id != expected_client_id {
        return Err(GuardError::Forbidden("capability client mismatch"));
    }
    if claims.expires_at < now {
        return Err(GuardError::Forbidden("capability expired"));
    }

    match (expected_resource_id, claims.resource_id.as_deref()) {
        (Some(expected), Some(actual)) if expected == actual => {}
        (Some(_), Some(_)) => return Err(GuardError::Forbidden("capability resource mismatch")),
        (Some(_), None) => return Err(GuardError::Forbidden("capability resource mismatch")),
        (None, Some(_)) => return Err(GuardError::Forbidden("capability resource mismatch")),
        (None, None) => {}
    }

    Ok(claims)
}

pub fn require_capability(
    state: &Arc<State>,
    headers: &HeaderMap,
    client_id: &str,
    scope: CapabilityScope,
    resource_id: Option<&str>,
) -> Result<CapabilityClaims, GuardError> {
    if !state
        .ingest_security
        .require_capabilities_for_protected_endpoints
    {
        return Ok(CapabilityClaims::new(
            client_id.to_string(),
            scope,
            resource_id.map(ToOwned::to_owned),
            Utc::now().timestamp(),
            Utc::now().timestamp() + state.ingest_security.capability_session_ttl_seconds,
        ));
    }

    let token = header_value(headers, "x-rpf-capability")
        .ok_or(GuardError::Unauthorized("missing capability token"))?;
    validate_capability_token(
        &state.ingest_security.capability_secret,
        token,
        scope,
        client_id,
        resource_id,
        Utc::now().timestamp(),
    )
}

fn compute_capability_signature(
    secret: &str,
    payload: &str,
) -> Result<String, hmac::digest::InvalidLength> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(payload.as_bytes());
    let signature = mac.finalize().into_bytes();
    Ok(base64_encode_config(signature, URL_SAFE_NO_PAD))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use chrono::Utc;
    use warp::http::{HeaderMap, HeaderValue};

    use super::{
        authorize_request, compute_signature, effective_rate_limit_prefer_client_id,
        extract_request_identity, issue_capability_token, validate_capability_token,
        CapabilityClaims, CapabilityScope, GuardError, IngestEndpoint,
    };

    fn signed_ingest_headers(
        secret: &str,
        client_id: &str,
        nonce: &str,
        method: &str,
        path: &str,
    ) -> HeaderMap {
        let timestamp = Utc::now().timestamp().to_string();
        let payload = format!("{method}\n{path}\n{timestamp}\n{nonce}\n{client_id}");
        let signature = compute_signature(secret, &payload).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-rpf-signature-version", HeaderValue::from_static("v1"));
        headers.insert("x-rpf-client-id", HeaderValue::from_str(client_id).unwrap());
        headers.insert(
            "x-rpf-timestamp",
            HeaderValue::from_str(&timestamp).unwrap(),
        );
        headers.insert("x-rpf-nonce", HeaderValue::from_str(nonce).unwrap());
        headers.insert(
            "x-rpf-signature",
            HeaderValue::from_str(&signature).unwrap(),
        );
        headers
    }

    #[test]
    fn capability_token_round_trips_for_expected_scope_and_resource() {
        let claims = CapabilityClaims::new(
            "client-123".to_string(),
            CapabilityScope::Detail,
            Some("listing:42".to_string()),
            1_700_000_000,
            1_700_000_300,
        );
        let token = issue_capability_token("server-secret", &claims).unwrap();

        let validated = validate_capability_token(
            "server-secret",
            &token,
            CapabilityScope::Detail,
            "client-123",
            Some("listing:42"),
            1_700_000_100,
        )
        .unwrap();

        assert_eq!(validated.client_id, "client-123");
        assert_eq!(validated.resource_id.as_deref(), Some("listing:42"));
    }

    #[test]
    fn capability_token_rejects_wrong_resource_binding() {
        let claims = CapabilityClaims::new(
            "client-123".to_string(),
            CapabilityScope::Detail,
            Some("listing:42".to_string()),
            1_700_000_000,
            1_700_000_300,
        );
        let token = issue_capability_token("server-secret", &claims).unwrap();

        let error = validate_capability_token(
            "server-secret",
            &token,
            CapabilityScope::Detail,
            "client-123",
            Some("listing:99"),
            1_700_000_100,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            GuardError::Forbidden("capability resource mismatch")
        ));
    }

    #[test]
    fn request_identity_defaults_to_remote_ip_over_client_id_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-rpf-client-id",
            HeaderValue::from_static("user-provided-id"),
        );

        let identity = extract_request_identity(
            &headers,
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)),
                4321,
            )),
            false,
        );

        assert_eq!(identity, "ip:10.0.0.9");
    }

    #[test]
    fn request_identity_can_prefer_client_id_for_relayed_ingest() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rpf-client-id", HeaderValue::from_static("client-a"));

        let identity = extract_request_identity(
            &headers,
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)),
                4321,
            )),
            true,
        );

        assert_eq!(identity, "client:client-a");
    }

    #[test]
    fn client_id_preference_is_disabled_when_signature_verification_is_disabled() {
        assert!(!effective_rate_limit_prefer_client_id(true, false));
        assert!(effective_rate_limit_prefer_client_id(true, true));
        assert!(!effective_rate_limit_prefer_client_id(false, true));
    }

    #[tokio::test]
    async fn relay_client_id_rate_limit_is_applied_after_signature_verification() {
        let state =
            crate::web::test_support::state_with_signature_required_relay_rate_limit(1).await;
        let method = "POST";
        let path = "/contribute/detail";
        let remote_addr = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)),
            4321,
        ));

        let client_a_headers =
            signed_ingest_headers("test-shared-secret", "client-a", "nonce-a", method, path);
        let client_a_context = authorize_request(
            &state,
            IngestEndpoint::ContributeDetail,
            &client_a_headers,
            remote_addr,
            method,
            path,
        )
        .await
        .unwrap();
        assert_eq!(client_a_context.request_identity, "client:client-a");

        let client_b_headers =
            signed_ingest_headers("test-shared-secret", "client-b", "nonce-b", method, path);
        let client_b_context = authorize_request(
            &state,
            IngestEndpoint::ContributeDetail,
            &client_b_headers,
            remote_addr,
            method,
            path,
        )
        .await
        .unwrap();
        assert_eq!(client_b_context.request_identity, "client:client-b");

        let windows = state.ingest_rate_windows.read().await;
        assert!(windows.keys().any(|key| key.client_id == "client:client-a"));
        assert!(windows.keys().any(|key| key.client_id == "client:client-b"));
        assert!(!windows.keys().any(|key| key.client_id == "ip:10.0.0.9"));
    }

    #[tokio::test]
    async fn relay_auth_failures_are_limited_by_remote_ip_before_client_id_spray() {
        let state =
            crate::web::test_support::state_with_signature_required_relay_rate_limit(1).await;
        let method = "POST";
        let path = "/contribute/detail";
        let remote_addr = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)),
            4321,
        ));

        let bad_headers =
            signed_ingest_headers("wrong-secret", "spray-client", "bad-nonce", method, path);
        let error = authorize_request(
            &state,
            IngestEndpoint::ContributeDetail,
            &bad_headers,
            remote_addr,
            method,
            path,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            GuardError::Unauthorized("invalid signature")
        ));
        assert!(state
            .ingest_rate_windows
            .read()
            .await
            .keys()
            .any(|key| key.client_id == "auth-failure:ip:10.0.0.9"));

        let second_bad_headers = signed_ingest_headers(
            "wrong-secret",
            "spray-client-2",
            "bad-nonce-2",
            method,
            path,
        );
        let error = authorize_request(
            &state,
            IngestEndpoint::ContributeDetail,
            &second_bad_headers,
            remote_addr,
            method,
            path,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, GuardError::TooManyRequests { .. }));
    }

    #[tokio::test]
    async fn open_mode_allows_unsigned_contribute_while_fflogs_toggle_enforces_signatures() {
        let mut state = crate::web::test_support::state_with_signature_required().await;
        {
            let mutable = std::sync::Arc::get_mut(&mut state).unwrap();
            mutable.ingest_security.require_signature = false;
            mutable.ingest_security.require_signature_for_fflogs = Some(true);
            mutable.ingest_security.rate_limit_prefer_client_id = true;
        }
        let peer = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)),
            4000,
        ));
        let headers = HeaderMap::new();

        let contrib_ctx = authorize_request(
            &state,
            IngestEndpoint::Contribute,
            &headers,
            peer,
            "POST",
            "/contribute",
        )
        .await
        .expect("open mode should permit unsigned /contribute");
        assert_eq!(contrib_ctx.request_identity, "ip:198.51.100.10");

        let fflogs_err = authorize_request(
            &state,
            IngestEndpoint::ContributeFflogsJobs,
            &headers,
            peer,
            "GET",
            "/contribute/fflogs/jobs",
        )
        .await
        .expect_err("fflogs toggle must enforce signatures even when contribute is open");
        assert!(matches!(fflogs_err, GuardError::Unauthorized(_)));

        let signed_fflogs = signed_ingest_headers(
            "test-shared-secret",
            "worker-1",
            "nonce-fflogs-1",
            "GET",
            "/contribute/fflogs/jobs",
        );
        let fflogs_ctx = authorize_request(
            &state,
            IngestEndpoint::ContributeFflogsJobs,
            &signed_fflogs,
            peer,
            "GET",
            "/contribute/fflogs/jobs",
        )
        .await
        .expect("signed fflogs request should pass");
        assert_eq!(fflogs_ctx.request_identity, "client:worker-1");

        mutable_signature_test_for_tomestone(&peer).await;
    }

    async fn mutable_signature_test_for_tomestone(peer: &Option<SocketAddr>) {
        let mut state = crate::web::test_support::state_with_signature_required().await;
        {
            let mutable = std::sync::Arc::get_mut(&mut state).unwrap();
            mutable.ingest_security.require_signature = false;
            mutable.ingest_security.require_signature_for_tomestone = Some(true);
        }
        let err = authorize_request(
            &state,
            IngestEndpoint::ContributeTomestoneJobs,
            &HeaderMap::new(),
            *peer,
            "GET",
            "/contribute/tomestone/jobs",
        )
        .await
        .expect_err("tomestone toggle must enforce signatures even when contribute is open");
        assert!(matches!(err, GuardError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn loopback_proxy_headers_trust_cf_connecting_ip_when_enabled() {
        let state = crate::web::test_support::state_with_signature_required().await;
        let loopback_peer = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8443));
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", "203.0.113.42".parse().unwrap());
        headers.insert("x-forwarded-for", "198.51.100.99, 10.0.0.1".parse().unwrap());

        assert_eq!(
            super::extract_client_ip(&headers, loopback_peer, true),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 42)))
        );
        assert_eq!(
            super::extract_client_ip(&headers, loopback_peer, false),
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        );

        let non_loopback_peer = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)),
            8443,
        ));
        assert_eq!(
            super::extract_client_ip(&headers, non_loopback_peer, true),
            Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)))
        );

        let mut mutable_state = state;
        {
            let mutable = std::sync::Arc::get_mut(&mut mutable_state).unwrap();
            mutable.ingest_security.require_signature = false;
            mutable.ingest_security.open_aggregate_rate_limits.contribute_per_minute = 2;
            mutable.ingest_security.rate_limits.contribute_per_minute = 10;
        }

        for expected_status in [true, true, false] {
            let mut open_headers = HeaderMap::new();
            open_headers.insert("cf-connecting-ip", "203.0.113.50".parse().unwrap());
            let result = authorize_request(
                &mutable_state,
                IngestEndpoint::Contribute,
                &open_headers,
                non_loopback_peer,
                "POST",
                "/contribute",
            )
            .await;
            assert_eq!(result.is_ok(), expected_status);
        }
    }

    #[tokio::test]
    async fn valid_signed_client_behind_loopback_is_not_locked_out_by_unauthenticated_ip_noise() {
        let mut state =
            crate::web::test_support::state_with_signature_required_relay_rate_limit(1).await;
        {
            let mutable = std::sync::Arc::get_mut(&mut state).unwrap();
            mutable.ingest_security.rate_limit_prefer_client_id = true;
            mutable.ingest_security.trust_loopback_proxy_headers = false;
        }
        let loopback_peer = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000));
        let method = "POST";
        let path = "/contribute/detail";

        let bad_1 = signed_ingest_headers("wrong-secret", "noise-1", "bad-1", method, path);
        let err_1 = authorize_request(
            &state,
            IngestEndpoint::ContributeDetail,
            &bad_1,
            loopback_peer,
            method,
            path,
        )
        .await
        .unwrap_err();
        assert!(matches!(err_1, GuardError::Unauthorized(_)));

        let bad_2 = signed_ingest_headers("wrong-secret", "noise-2", "bad-2", method, path);
        let err_2 = authorize_request(
            &state,
            IngestEndpoint::ContributeDetail,
            &bad_2,
            loopback_peer,
            method,
            path,
        )
        .await
        .unwrap_err();
        assert!(matches!(err_2, GuardError::TooManyRequests { .. }));

        let valid_headers =
            signed_ingest_headers("test-shared-secret", "valid-relay", "good-1", method, path);
        let valid_ctx = authorize_request(
            &state,
            IngestEndpoint::ContributeDetail,
            &valid_headers,
            loopback_peer,
            method,
            path,
        )
        .await
        .expect("valid signed client behind loopback must not be blocked by auth-failure:ip:127.0.0.1");
        assert_eq!(valid_ctx.request_identity, "client:valid-relay");
    }

    #[test]
    fn nonce_and_rate_window_caches_stay_bounded_at_max_entries() {
        let now = Utc::now();
        let ttl = chrono::TimeDelta::seconds(300);
        let mut nonces =
            std::collections::HashMap::with_capacity(super::MAX_SECURITY_CACHE_ENTRIES);
        for idx in 0..super::MAX_SECURITY_CACHE_ENTRIES {
            nonces.insert(format!("n-{idx}"), now);
        }
        super::insert_bounded_nonce(&mut nonces, "fresh-nonce".to_string(), now, ttl)
            .expect("inserting new nonce past cap should evict oldest");
        assert_eq!(nonces.len(), super::MAX_SECURITY_CACHE_ENTRIES);
        assert!(nonces.contains_key("fresh-nonce"));

        let mut windows =
            std::collections::HashMap::with_capacity(super::MAX_SECURITY_CACHE_ENTRIES);
        for idx in 0..super::MAX_SECURITY_CACHE_ENTRIES {
            windows.insert(
                crate::web::IngestRateKey {
                    endpoint: "contribute",
                    client_id: format!("c-{idx}"),
                },
                crate::web::IngestRateWindow {
                    window_started: now,
                    count: 1,
                },
            );
        }
        super::record_rate_window_units(
            &mut windows,
            crate::web::IngestRateKey {
                endpoint: "contribute",
                client_id: "fresh-client".to_string(),
            },
            1,
            now,
        );
        assert_eq!(windows.len(), super::MAX_SECURITY_CACHE_ENTRIES);
        assert!(windows.contains_key(&crate::web::IngestRateKey {
            endpoint: "contribute",
            client_id: "fresh-client".to_string(),
        }));
    }

    #[tokio::test]
    async fn authenticated_relay_secret_prioritizes_x_rpf_forwarded_for_and_isolates_rate_limits() {
        let mut state = crate::web::test_support::state_with_signature_required().await;
        {
            let mutable = std::sync::Arc::get_mut(&mut state).unwrap();
            mutable.ingest_security.require_signature = false;
            mutable.ingest_security.require_capabilities_for_protected_endpoints = false;
            mutable.ingest_security.trusted_relay_shared_secret = "relay-secret-123".to_string();
            mutable.ingest_security.rate_limits.contribute_per_minute = 1;
            mutable.ingest_security.open_aggregate_rate_limits.contribute_per_minute = 100;
        }

        let loopback_peer = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000));
        let docker_bridge_peer = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 18, 0, 5)), 9000));

        // 1. Valid relay secret overrides cf-connecting-ip (Koyeb egress) on loopback peer.
        let mut relay_a = HeaderMap::new();
        relay_a.insert("x-rpf-relay-secret", HeaderValue::from_static("relay-secret-123"));
        relay_a.insert("x-rpf-forwarded-for", HeaderValue::from_static("198.51.100.10"));
        relay_a.insert("cf-connecting-ip", HeaderValue::from_static("203.0.113.1"));
        let ctx_a = authorize_request(
            &state,
            IngestEndpoint::Contribute,
            &relay_a,
            loopback_peer,
            "POST",
            "/contribute",
        )
        .await
        .expect("first player through authenticated relay must succeed");
        assert_eq!(ctx_a.request_identity, "ip:198.51.100.10");

        // Second request from same player IP hits per-IP limit (1/min).
        let err_a = authorize_request(
            &state,
            IngestEndpoint::Contribute,
            &relay_a,
            loopback_peer,
            "POST",
            "/contribute",
        )
        .await
        .unwrap_err();
        assert!(matches!(err_a, GuardError::TooManyRequests { .. }));

        // Different player through same Koyeb egress + non-loopback Docker peer gets independent limit.
        let mut relay_b = HeaderMap::new();
        relay_b.insert("x-rpf-relay-secret", HeaderValue::from_static("relay-secret-123"));
        relay_b.insert("x-rpf-forwarded-for", HeaderValue::from_static("198.51.100.11"));
        relay_b.insert("cf-connecting-ip", HeaderValue::from_static("203.0.113.1"));
        let ctx_b = authorize_request(
            &state,
            IngestEndpoint::Contribute,
            &relay_b,
            docker_bridge_peer,
            "POST",
            "/contribute",
        )
        .await
        .expect("second player IP through authenticated relay must remain isolated");
        assert_eq!(ctx_b.request_identity, "ip:198.51.100.11");

        // 2. Missing or wrong relay secret ignores spoofed x-rpf-forwarded-for.
        let mut wrong_secret = HeaderMap::new();
        wrong_secret.insert("x-rpf-relay-secret", HeaderValue::from_static("wrong-secret"));
        wrong_secret.insert("x-rpf-forwarded-for", HeaderValue::from_static("198.51.100.99"));
        wrong_secret.insert("cf-connecting-ip", HeaderValue::from_static("203.0.113.2"));
        assert_eq!(
            super::extract_client_ip_with_relay(
                &wrong_secret,
                loopback_peer,
                true,
                "relay-secret-123",
            ),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 2)))
        );
        assert_eq!(
            super::extract_client_ip_with_relay(
                &wrong_secret,
                docker_bridge_peer,
                true,
                "relay-secret-123",
            ),
            Some(IpAddr::V4(Ipv4Addr::new(172, 18, 0, 5)))
        );
    }
}
