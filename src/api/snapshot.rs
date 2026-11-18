use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant},
};

use crate::config::ListingsSnapshotSource;
use crate::listings_render::EnrichmentFallbackMode;
use crate::listings_snapshot::{
    build_listings_payload_with_policy, format_representation_etag, gunzip_body,
    load_current_materialized_snapshot, serialize_snapshot, BuiltListingsPayload,
};
use crate::web::{CachedListingsSnapshot, ListingsSnapshotCacheState, State};
use anyhow::Context;
use tokio::sync::{watch, RwLock};
use warp::http::{
    header::{HeaderValue, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, ETAG, VARY},
    StatusCode,
};
use warp::hyper::Body;
use warp::Reply;

use super::refresh::apply_materialized_snapshot_if_newer;
const MIN_MATERIALIZED_SNAPSHOT_REQUEST_RECONCILE_INTERVAL_SECONDS: u64 = 5;

type MaterializedSnapshotFreshnessChecks = HashMap<usize, (Weak<State>, Instant)>;

fn materialized_snapshot_freshness_checks() -> &'static Mutex<MaterializedSnapshotFreshnessChecks> {
    static CHECKS: OnceLock<Mutex<MaterializedSnapshotFreshnessChecks>> = OnceLock::new();
    CHECKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn materialized_snapshot_request_reconcile_interval(state: &State) -> Duration {
    Duration::from_secs(
        state
            .materialized_snapshot_reconcile_interval_seconds
            .max(MIN_MATERIALIZED_SNAPSHOT_REQUEST_RECONCILE_INTERVAL_SECONDS),
    )
}

pub(super) fn record_materialized_snapshot_freshness_check(
    state: &Arc<State>,
    checked_at: Instant,
) {
    let key = Arc::as_ptr(state) as usize;
    let mut checks = materialized_snapshot_freshness_checks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    checks.retain(|_, (state, _)| state.strong_count() > 0);
    checks.insert(key, (Arc::downgrade(state), checked_at));
}

fn should_check_materialized_snapshot_freshness(state: &Arc<State>) -> bool {
    let key = Arc::as_ptr(state) as usize;
    let now = Instant::now();
    let interval = materialized_snapshot_request_reconcile_interval(state);
    let mut checks = materialized_snapshot_freshness_checks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    checks.retain(|_, (state, _)| state.strong_count() > 0);

    if let Some((cached_state, last_checked_at)) = checks.get(&key) {
        if cached_state.strong_count() > 0
            && now
                .checked_duration_since(*last_checked_at)
                .unwrap_or_default()
                < interval
        {
            return false;
        }
    }

    checks.insert(key, (Arc::downgrade(state), now));
    true
}

struct SnapshotBuildGuard<'a> {
    cache: &'a RwLock<ListingsSnapshotCacheState>,
    done_tx: watch::Sender<bool>,
    done_rx: watch::Receiver<bool>,
    active: bool,
}

impl<'a> SnapshotBuildGuard<'a> {
    fn new(
        cache: &'a RwLock<ListingsSnapshotCacheState>,
        done_tx: watch::Sender<bool>,
        done_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            cache,
            done_tx,
            done_rx,
            active: true,
        }
    }

    fn complete(mut self) {
        self.active = false;
        let _ = self.done_tx.send(true);
    }
}

impl Drop for SnapshotBuildGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        if let Ok(mut cache) = self.cache.try_write() {
            if let ListingsSnapshotCacheState::Building { done_rx, stale } = &mut *cache {
                if done_rx.same_channel(&self.done_rx) {
                    *cache = match stale.take() {
                        Some(snapshot) => ListingsSnapshotCacheState::Ready(snapshot),
                        None => ListingsSnapshotCacheState::Empty,
                    };
                }
            }
        }

        let _ = self.done_tx.send(true);
    }
}

fn snapshot_builder_is_active(done_rx: &watch::Receiver<bool>) -> bool {
    !*done_rx.borrow() && matches!(done_rx.has_changed(), Ok(false))
}

fn wait_for_active_snapshot_builder(
    done_rx: &watch::Receiver<bool>,
) -> Option<watch::Receiver<bool>> {
    if snapshot_builder_is_active(done_rx) {
        Some(done_rx.clone())
    } else {
        None
    }
}

fn install_snapshot_builder(
    cache: &mut ListingsSnapshotCacheState,
    new_done_rx: watch::Receiver<bool>,
) -> bool {
    let stale = match std::mem::take(cache) {
        ListingsSnapshotCacheState::Ready(snapshot) => Some(snapshot),
        ListingsSnapshotCacheState::Building { stale, .. } => stale,
        ListingsSnapshotCacheState::Empty => None,
    };
    let has_stale = stale.is_some();

    *cache = ListingsSnapshotCacheState::Building {
        done_rx: new_done_rx,
        stale,
    };
    has_stale
}

pub(super) async fn build_snapshot_payload<T, Build, BuildFut, CurrentRevision>(
    mut build: Build,
    current_revision: CurrentRevision,
) -> (u64, T)
where
    Build: FnMut() -> BuildFut,
    BuildFut: Future<Output = T>,
    CurrentRevision: Fn() -> u64,
{
    let revision_before = current_revision();
    let payload = build().await;
    let revision_after = current_revision();

    if revision_before == revision_after {
        return (revision_after, payload);
    }

    // If writes race the build, report the lower bound that the payload is
    // guaranteed to include. The next client/server revision check will catch up
    // without multiplying the expensive listing-enrichment pass.
    (revision_before, payload)
}

async fn build_api_snapshot_with_policy(
    state: Arc<State>,
    fallback_mode: EnrichmentFallbackMode,
) -> anyhow::Result<CachedListingsSnapshot> {
    let (revision, payload): (u64, anyhow::Result<BuiltListingsPayload>) = build_snapshot_payload(
        || build_listings_payload_with_policy(Arc::clone(&state), fallback_mode),
        || state.current_listings_revision(),
    )
    .await;
    let payload = payload?;
    let rev_i64: i64 = revision
        .try_into()
        .context("listings snapshot revision exceeded i64")?;
    let revision_epoch = state.revision_epoch().to_string();
    let snapshot =
        tokio::task::spawn_blocking(move || serialize_snapshot(rev_i64, &revision_epoch, payload))
            .await
            .context("listings snapshot serialization task panicked")??;

    Ok(CachedListingsSnapshot {
        revision,
        body: snapshot.body.into(),
        body_gzip: Some(snapshot.body_gzip.into()),
        etag: Some(snapshot.etag),
        content_encoding: None,
    })
}

#[cfg(test)]
pub(super) async fn get_or_build_cached_snapshot<F, Fut, E>(
    cache: &RwLock<ListingsSnapshotCacheState>,
    min_revision: u64,
    build: F,
) -> Result<CachedListingsSnapshot, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<CachedListingsSnapshot, E>>,
    E: std::fmt::Debug,
{
    get_or_build_cached_snapshot_with_stale_flag(cache, min_revision, |_| build()).await
}

pub(super) async fn get_or_build_cached_snapshot_with_stale_flag<F, Fut, E>(
    cache: &RwLock<ListingsSnapshotCacheState>,
    min_revision: u64,
    build: F,
) -> Result<CachedListingsSnapshot, E>
where
    F: Fn(bool) -> Fut,
    Fut: Future<Output = Result<CachedListingsSnapshot, E>>,
    E: std::fmt::Debug,
{
    loop {
        let maybe_wait = {
            let cache = cache.read().await;
            match &*cache {
                ListingsSnapshotCacheState::Ready(snapshot)
                    if snapshot.revision >= min_revision =>
                {
                    return Ok(snapshot.clone());
                }
                ListingsSnapshotCacheState::Building { done_rx, .. } => {
                    wait_for_active_snapshot_builder(done_rx)
                }
                ListingsSnapshotCacheState::Ready(_) | ListingsSnapshotCacheState::Empty => None,
            }
        };

        if let Some(mut rx) = maybe_wait {
            let already_done = *rx.borrow();
            if !already_done {
                let _ = rx.changed().await;
            }
            continue;
        }

        let (done_tx, done_rx) = watch::channel(false);
        enum BuildAction {
            Ready(CachedListingsSnapshot),
            Wait(watch::Receiver<bool>),
            Build { has_stale: bool },
        }

        let action = {
            let mut cache = cache.write().await;
            match &*cache {
                ListingsSnapshotCacheState::Ready(snapshot)
                    if snapshot.revision >= min_revision =>
                {
                    BuildAction::Ready(snapshot.clone())
                }
                ListingsSnapshotCacheState::Building {
                    done_rx: building_done_rx,
                    ..
                } => {
                    if let Some(rx) = wait_for_active_snapshot_builder(building_done_rx) {
                        BuildAction::Wait(rx)
                    } else {
                        let has_stale = install_snapshot_builder(&mut cache, done_rx.clone());
                        BuildAction::Build { has_stale }
                    }
                }
                ListingsSnapshotCacheState::Ready(_) | ListingsSnapshotCacheState::Empty => {
                    let has_stale = install_snapshot_builder(&mut cache, done_rx.clone());
                    BuildAction::Build { has_stale }
                }
            }
        };

        let has_stale = match action {
            BuildAction::Ready(snapshot) => return Ok(snapshot),
            BuildAction::Wait(mut rx) => {
                let already_done = *rx.borrow();
                if !already_done {
                    let _ = rx.changed().await;
                }
                continue;
            }
            BuildAction::Build { has_stale } => has_stale,
        };

        let build_guard = SnapshotBuildGuard::new(cache, done_tx, done_rx);
        let built = build(has_stale).await;

        let result = {
            let mut cache = cache.write().await;
            match built {
                Ok(snapshot) => {
                    *cache = ListingsSnapshotCacheState::Ready(snapshot.clone());
                    Ok(snapshot)
                }
                Err(error) => {
                    let stale =
                        if let ListingsSnapshotCacheState::Building { stale, .. } = &mut *cache {
                            stale.take()
                        } else {
                            None
                        };
                    if let Some(stale) = stale {
                        tracing::warn!(
                            error = ?error,
                            stale_revision = stale.revision,
                            "listings snapshot build failed; serving stale cached snapshot"
                        );
                        *cache = ListingsSnapshotCacheState::Ready(stale.clone());
                        Ok(stale)
                    } else {
                        *cache = ListingsSnapshotCacheState::Empty;
                        Err(error)
                    }
                }
            }
        };

        build_guard.complete();
        return result;
    }
}

async fn get_cached_api_snapshot(state: Arc<State>) -> anyhow::Result<CachedListingsSnapshot> {
    let min_revision = state.current_listings_revision();
    get_or_build_cached_snapshot_with_stale_flag(
        &state.listings_snapshot_cache,
        min_revision,
        |has_stale| {
            let fallback_mode = if has_stale {
                EnrichmentFallbackMode::RequireCompleteEnrichment
            } else {
                EnrichmentFallbackMode::AllowFallback
            };
            build_api_snapshot_with_policy(Arc::clone(&state), fallback_mode)
        },
    )
    .await
}

pub(super) async fn get_materialized_cached_snapshot_with_loader<F, Fut>(
    state: Arc<State>,
    load: F,
) -> anyhow::Result<Option<CachedListingsSnapshot>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<CachedListingsSnapshot>>>,
{
    let (cached_candidate, stale_candidate) = {
        let cache = state.listings_snapshot_cache.read().await;
        match &*cache {
            ListingsSnapshotCacheState::Ready(snapshot) => {
                let stale = Some(snapshot.clone());
                let fresh = (snapshot.revision >= state.current_listings_revision())
                    .then(|| snapshot.clone());
                (fresh, stale)
            }
            ListingsSnapshotCacheState::Building {
                stale: Some(snapshot),
                ..
            } => (None, Some(snapshot.clone())),
            ListingsSnapshotCacheState::Building { stale: None, .. }
            | ListingsSnapshotCacheState::Empty => (None, None),
        }
    };

    if let Some(snapshot) = &cached_candidate {
        if !should_check_materialized_snapshot_freshness(&state) {
            return Ok(Some(snapshot.clone()));
        }
    }

    let loaded = match load().await {
        Ok(snapshot) => snapshot,
        Err(error) if stale_candidate.is_some() => {
            tracing::warn!(
                error = ?error,
                "failed materialized listings snapshot load; serving stale cached snapshot"
            );
            return Ok(stale_candidate);
        }
        Err(error) => return Err(error),
    };

    let Some(snapshot) = loaded else {
        return Ok(cached_candidate);
    };

    let outcome = apply_materialized_snapshot_if_newer(&state, snapshot.clone()).await;
    if outcome.applied_revision.is_none() {
        let current_revision = state.current_listings_revision();
        if snapshot.revision < current_revision {
            let cache = state.listings_snapshot_cache.read().await;
            if let ListingsSnapshotCacheState::Ready(cached) = &*cache {
                if cached.revision >= current_revision {
                    return Ok(Some(cached.clone()));
                }
            }
            return Ok(None);
        }

        let mut cache = state.listings_snapshot_cache.write().await;
        if matches!(&*cache, ListingsSnapshotCacheState::Empty)
            || matches!(&*cache, ListingsSnapshotCacheState::Ready(cached) if cached.revision < snapshot.revision)
        {
            *cache = ListingsSnapshotCacheState::Ready(snapshot.clone());
        }
    }

    Ok(Some(snapshot))
}

async fn get_materialized_cached_api_snapshot(
    state: Arc<State>,
) -> anyhow::Result<Option<CachedListingsSnapshot>> {
    get_materialized_cached_snapshot_with_loader(Arc::clone(&state), || async {
        load_current_materialized_snapshot(&state).await
    })
    .await
}

pub(super) async fn get_api_snapshot(
    state: Arc<State>,
) -> anyhow::Result<Option<CachedListingsSnapshot>> {
    match state.listings_snapshot_source {
        ListingsSnapshotSource::Inline => get_cached_api_snapshot(state).await.map(Some),
        ListingsSnapshotSource::Materialized => get_materialized_cached_api_snapshot(state).await,
    }
}

pub(super) fn accepts_gzip(accept_encoding: Option<&str>) -> bool {
    let Some(header) = accept_encoding else {
        return false;
    };
    header.split(',').any(|entry| {
        let mut parts = entry.split(';');
        let Some(coding) = parts.next() else {
            return false;
        };
        if !coding.trim().eq_ignore_ascii_case("gzip") {
            return false;
        }
        !parts.any(|param| {
            let trimmed = param.trim();
            if let Some(q_val) = trimmed.strip_prefix("q=").or_else(|| trimmed.strip_prefix("Q=")) {
                q_val.trim().parse::<f32>().ok().is_some_and(|q| q <= 0.0)
            } else {
                false
            }
        })
    })
}

fn parse_rfc9110_entity_tag(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    let rest = trimmed.strip_prefix("W/").unwrap_or(trimmed);
    let inner = rest.strip_prefix('"')?.strip_suffix('"')?;
    if inner.is_empty() || inner.contains('"') {
        return None;
    }
    Some(inner)
}

fn split_if_none_match_members(header: &str) -> Vec<&str> {
    let mut members = Vec::new();
    let mut in_quotes = false;
    let mut start = 0usize;

    for (idx, ch) in header.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                members.push(&header[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    members.push(&header[start..]);
    members
}

pub(super) fn if_none_match_matches(if_none_match: Option<&str>, etag: Option<&str>) -> bool {
    let (Some(header), Some(current_etag)) = (if_none_match, etag) else {
        return false;
    };
    let current = parse_rfc9110_entity_tag(current_etag).or_else(|| {
        let trimmed = current_etag.trim();
        (!trimmed.is_empty() && !trimmed.contains('"')).then_some(trimmed)
    });
    let Some(current) = current else {
        return false;
    };

    split_if_none_match_members(header)
        .into_iter()
        .any(|candidate| {
            let trimmed = candidate.trim();
            trimmed == "*" || parse_rfc9110_entity_tag(trimmed) == Some(current)
        })
}

const SNAPSHOT_CACHE_CONTROL: &str = "no-cache, max-age=0, must-revalidate";

pub(super) fn snapshot_response_for_request(
    snapshot: CachedListingsSnapshot,
    accept_encoding: Option<&str>,
    if_none_match: Option<&str>,
) -> warp::reply::Response {
    let wants_gzip = accepts_gzip(accept_encoding);
    let already_gzipped = snapshot
        .content_encoding
        .as_deref()
        .is_some_and(|enc| enc.eq_ignore_ascii_case("gzip"));
    let serves_gzip = if already_gzipped {
        wants_gzip
    } else {
        wants_gzip && snapshot.body_gzip.is_some()
    };

    let response_etag = snapshot
        .etag
        .as_deref()
        .and_then(|raw| format_representation_etag(raw, serves_gzip));

    if if_none_match_matches(if_none_match, response_etag.as_deref()) {
        let mut response = warp::http::Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static(SNAPSHOT_CACHE_CONTROL));
        response
            .headers_mut()
            .insert(VARY, HeaderValue::from_static("Accept-Encoding"));
        if let Some(etag) = response_etag {
            if let Ok(value) = HeaderValue::from_str(&etag) {
                response.headers_mut().insert(ETAG, value);
            }
        }
        return response;
    }

    let (body, content_encoding) = if already_gzipped {
        if wants_gzip {
            (snapshot.body, snapshot.content_encoding)
        } else {
            match gunzip_body(&snapshot.body) {
                Ok(plain) => (plain.into(), None),
                Err(error) => {
                    tracing::error!(
                        error = ?error,
                        "failed to decompress cached gzip snapshot for non-gzip client"
                    );
                    return warp::reply::with_status(
                        "failed to load listings snapshot",
                        StatusCode::SERVICE_UNAVAILABLE,
                    )
                    .into_response();
                }
            }
        }
    } else if serves_gzip {
        (
            snapshot
                .body_gzip
                .expect("serves_gzip is only true for non-gzipped snapshots with body_gzip"),
            Some("gzip".to_string()),
        )
    } else {
        (snapshot.body, snapshot.content_encoding)
    };

    let mut response = warp::http::Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(SNAPSHOT_CACHE_CONTROL));
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("Accept-Encoding"));
    if let Some(etag) = response_etag {
        if let Ok(value) = HeaderValue::from_str(&etag) {
            response.headers_mut().insert(ETAG, value);
        }
    }
    if let Some(content_encoding) = content_encoding {
        if let Ok(value) = HeaderValue::from_str(&content_encoding) {
            response.headers_mut().insert(CONTENT_ENCODING, value);
        }
    }
    response
}

#[cfg(test)]
pub(super) fn snapshot_json_response(snapshot: CachedListingsSnapshot) -> warp::reply::Response {
    let is_gzip = snapshot
        .content_encoding
        .as_deref()
        .is_some_and(|enc| enc.eq_ignore_ascii_case("gzip"));
    let response_etag = snapshot
        .etag
        .as_deref()
        .and_then(|raw| format_representation_etag(raw, is_gzip));

    let mut response = warp::http::Response::new(Body::from(snapshot.body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(SNAPSHOT_CACHE_CONTROL));
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("Accept-Encoding"));
    if let Some(etag) = response_etag {
        if let Ok(value) = HeaderValue::from_str(&etag) {
            response.headers_mut().insert(ETAG, value);
        }
    }
    if let Some(content_encoding) = snapshot.content_encoding {
        if let Ok(value) = HeaderValue::from_str(&content_encoding) {
            response.headers_mut().insert(CONTENT_ENCODING, value);
        }
    }
    response
}

pub(super) fn materialized_snapshot_result_response_for_request(
    result: anyhow::Result<Option<CachedListingsSnapshot>>,
    accept_encoding: Option<&str>,
    if_none_match: Option<&str>,
) -> warp::reply::Response {
    match result {
        Ok(Some(snapshot)) => {
            snapshot_response_for_request(snapshot, accept_encoding, if_none_match)
        }
        Ok(None) => warp::reply::with_status("snapshot not ready", StatusCode::SERVICE_UNAVAILABLE)
            .into_response(),
        Err(error) => {
            tracing::error!(
                "failed to load materialized listings snapshot: {:#?}",
                error
            );
            warp::reply::with_status(
                "failed to load listings snapshot",
                StatusCode::SERVICE_UNAVAILABLE,
            )
            .into_response()
        }
    }
}

#[cfg(test)]
pub(super) fn materialized_snapshot_result_response(
    result: anyhow::Result<Option<CachedListingsSnapshot>>,
) -> warp::reply::Response {
    match result {
        Ok(Some(snapshot)) => snapshot_json_response(snapshot),
        Ok(None) => warp::reply::with_status("snapshot not ready", StatusCode::SERVICE_UNAVAILABLE)
            .into_response(),
        Err(error) => {
            tracing::error!(
                "failed to load materialized listings snapshot: {:#?}",
                error
            );
            warp::reply::with_status(
                "failed to load listings snapshot",
                StatusCode::SERVICE_UNAVAILABLE,
            )
            .into_response()
        }
    }
}
