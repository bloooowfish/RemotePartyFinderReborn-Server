use std::{net::SocketAddr, sync::Arc};

use base64::{
    decode_config as base64_decode_config, encode as base64_encode,
    encode_config as base64_encode_config, URL_SAFE_NO_PAD,
};
use chrono::{DateTime, TimeDelta, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use super::{IngestRateKey, IngestRateLimits, IngestRateWindow, State};

const WINDOW_SECONDS: i64 = 60;

#[derive(Debug, Clone, Copy)]
pub enum IngestEndpoint {
    Contribute,
    ContributeMultiple,
    ContributePlayers,
    ContributeDetail,
    ContributeFflogsJobs,
    ContributeFflogsResults,
    ContributeFflogsLeasesAbandon,
}

impl IngestEndpoint {
    fn key(self) -> &'static str {
        match self {
            Self::Contribute => "contribute",
            Self::ContributeMultiple => "contribute_multiple",
            Self::ContributePlayers => "contribute_players",
            Self::ContributeDetail => "contribute_detail",
            Self::ContributeFflogsJobs => "contribute_fflogs_jobs",
            Self::ContributeFflogsResults => "contribute_fflogs_results",
            Self::ContributeFflogsLeasesAbandon => "contribute_fflogs_leases_abandon",
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
        }
    }
}

#[derive(Debug, Clone)]
pub struct GuardContext {
    pub client_id: String,
    pub request_identity: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityScope {
    Detail,
    FflogsJobs,
    FflogsResults,
    FflogsLeasesAbandon,
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

#[derive(Debug)]
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
    let client_id = extract_client_id(headers);
    let should_verify_signature = state.ingest_security.require_signature;
    let prefer_client_id = effective_rate_limit_prefer_client_id(
        state.ingest_security.rate_limit_prefer_client_id,
        should_verify_signature,
    );
    let request_identity = extract_request_identity(headers, remote_addr, prefer_client_id);

    if !prefer_client_id {
        enforce_rate_limit_units(state, endpoint, &request_identity, rate_limit_units).await?;
    }
    let auth_failure_identity =
        prefer_client_id.then(|| extract_auth_failure_identity(remote_addr));
    if let Some(identity) = &auth_failure_identity {
        check_rate_limit_available_units(state, endpoint, identity, 1).await?;
    }

    if should_verify_signature {
        if let Err(error) = verify_signature(state, headers, method, path, &client_id) {
            if let Some(identity) = &auth_failure_identity {
                enforce_rate_limit(state, endpoint, identity).await?;
            }
            return Err(error);
        }
        if let Err(error) = enforce_nonce(state, headers, &client_id).await {
            if let Some(identity) = &auth_failure_identity {
                enforce_rate_limit(state, endpoint, identity).await?;
            }
            return Err(error);
        }
    }

    if prefer_client_id {
        enforce_rate_limit_units(state, endpoint, &request_identity, rate_limit_units).await?;
    }

    Ok(GuardContext {
        client_id,
        request_identity,
    })
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
    let should_verify_signature = state.ingest_security.require_signature;
    let prefer_client_id = effective_rate_limit_prefer_client_id(
        state.ingest_security.rate_limit_prefer_client_id,
        should_verify_signature,
    );
    let request_identity = extract_request_identity(headers, remote_addr, prefer_client_id);

    if should_verify_signature {
        let auth_failure_identity = extract_auth_failure_identity(remote_addr);
        check_rate_limit_available_units(state, endpoint, &auth_failure_identity, 1).await?;

        if let Err(error) = verify_signature(state, headers, method, path, &client_id) {
            enforce_rate_limit(state, endpoint, &auth_failure_identity).await?;
            return Err(error);
        }
        if let Err(error) = enforce_nonce(state, headers, &client_id).await {
            enforce_rate_limit(state, endpoint, &auth_failure_identity).await?;
            return Err(error);
        }
    }

    Ok(GuardContext {
        client_id,
        request_identity,
    })
}

pub async fn reserve_rate_limit_units(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    request_identity: &str,
    units: usize,
) -> Result<(), GuardError> {
    enforce_rate_limit_units(state, endpoint, request_identity, units).await
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
    if prefer_client_id {
        let client_id = extract_client_id(headers);
        if client_id != "anonymous" {
            return format!("client:{client_id}");
        }
    }

    if let Some(remote_addr) = remote_addr {
        return format!("ip:{}", remote_addr.ip());
    }

    format!("client:{}", extract_client_id(headers))
}

fn extract_auth_failure_identity(remote_addr: Option<SocketAddr>) -> String {
    match remote_addr {
        Some(remote_addr) => format!("auth-failure:ip:{}", remote_addr.ip()),
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
    let skew = (now - timestamp).abs();
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
    nonces.retain(|_, seen_at| *seen_at + ttl > now);

    if nonces.contains_key(&nonce_key) {
        return Err(GuardError::Forbidden("replayed nonce"));
    }

    nonces.insert(nonce_key, now);
    Ok(())
}

async fn enforce_rate_limit(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    request_identity: &str,
) -> Result<(), GuardError> {
    enforce_rate_limit_units(state, endpoint, request_identity, 1).await
}

async fn enforce_rate_limit_units(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    request_identity: &str,
    units: usize,
) -> Result<(), GuardError> {
    apply_rate_limit_units(state, endpoint, request_identity, units, true).await
}

async fn check_rate_limit_available_units(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    request_identity: &str,
    units: usize,
) -> Result<(), GuardError> {
    apply_rate_limit_units(state, endpoint, request_identity, units, false).await
}

async fn apply_rate_limit_units(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    request_identity: &str,
    units: usize,
    increment: bool,
) -> Result<(), GuardError> {
    if units == 0 {
        return Ok(());
    }

    let now = Utc::now();
    let key = IngestRateKey {
        endpoint: endpoint.key(),
        client_id: request_identity.to_string(),
    };
    let limit = endpoint
        .limit_per_minute(&state.ingest_security.rate_limits)
        .max(1);
    let units = u32::try_from(units).unwrap_or(u32::MAX);

    let mut windows = state.ingest_rate_windows.write().await;
    let stale_after = TimeDelta::seconds(WINDOW_SECONDS * 2);
    windows.retain(|_, window| now - window.window_started <= stale_after);

    let Some(window) = windows.get_mut(&key) else {
        if units > limit {
            return Err(GuardError::TooManyRequests {
                retry_after_seconds: WINDOW_SECONDS as u64,
            });
        }
        if increment {
            windows.insert(
                key,
                IngestRateWindow {
                    window_started: now,
                    count: units,
                },
            );
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

    if increment {
        window.count += units;
    }
    Ok(())
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
}
