use std::{
    future::Future,
    sync::{Arc, atomic::Ordering as AtomicOrdering},
    time::Duration,
};

use chrono::{TimeDelta, Utc};

use super::State;
use crate::config::ListingsSnapshotSource;
use crate::listings_snapshot::increment_listing_source_revision;
use crate::stats::CachedStatistics;
use crate::web::ListingsRevisionWebhook;

const BACKGROUND_FATAL_FAILURE_THRESHOLD: u32 = 5;
const STATS_REFRESH_EMPTY_INTERVAL: Duration = Duration::from_secs(60);
const STATS_REFRESH_POPULATED_INTERVAL: Duration = Duration::from_secs(60 * 15);
const STATS_FIRST_DATA_SETTLE_DELAY: Duration = Duration::from_secs(2);

pub(crate) fn format_supervised_task_exit(
    task_name: &'static str,
    result: Result<(), tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(()) => anyhow::anyhow!("supervised background task `{task_name}` exited unexpectedly"),
        Err(join_error) => {
            anyhow::anyhow!("supervised background task `{task_name}` terminated fatally: {join_error}")
        }
    }
}

fn exponential_backoff_seconds(
    base_seconds: u64,
    max_seconds: u64,
    consecutive_failures: u32,
) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(10);
    let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let secs = base_seconds.saturating_mul(multiplier).min(max_seconds);
    Duration::from_secs(secs)
}

fn stats_task_sleep_after_success(num_listings: usize) -> Duration {
    if num_listings == 0 {
        STATS_REFRESH_EMPTY_INTERVAL
    } else {
        STATS_REFRESH_POPULATED_INTERVAL
    }
}

fn stats_task_sleep_after_refresh_with_failures(
    succeeded: bool,
    consecutive_failures: u32,
) -> Duration {
    if succeeded {
        stats_task_sleep_after_success(1)
    } else {
        exponential_backoff_seconds(60, 60 * 15, consecutive_failures.max(1))
    }
}

#[cfg(test)]
fn stats_task_sleep_after_refresh(succeeded: bool) -> Duration {
    stats_task_sleep_after_refresh_with_failures(succeeded, 1)
}

fn publish_retry_backoff(consecutive_failures: u32) -> Duration {
    exponential_backoff_seconds(1, 60, consecutive_failures.max(1))
}

fn reconcile_retry_backoff(base_interval: Duration, consecutive_failures: u32) -> Duration {
    let backoff = exponential_backoff_seconds(5, 300, consecutive_failures.max(1));
    base_interval.max(backoff)
}

pub fn spawn_stats_task(state: Arc<State>) -> tokio::task::JoinHandle<()> {
    let stats_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        let mut listings_rx = stats_state.listings_change_channel.subscribe();
        let mut consecutive_failures = 0u32;
        loop {
            let all_time = match crate::stats::get_stats(&stats_state).await {
                Ok(stats) => stats,
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures >= BACKGROUND_FATAL_FAILURE_THRESHOLD {
                        tracing::error!(
                            consecutive_failures,
                            "repeatedly failing to generate stats (all_time): {:#?}",
                            e
                        );
                    } else {
                        tracing::warn!(
                            consecutive_failures,
                            "error generating stats (all_time): {:#?}",
                            e
                        );
                    }
                    tokio::time::sleep(stats_task_sleep_after_refresh_with_failures(
                        false,
                        consecutive_failures,
                    ))
                    .await;
                    continue;
                }
            };

            let seven_days = match crate::stats::get_stats_seven_days(&stats_state).await {
                Ok(stats) => stats,
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures >= BACKGROUND_FATAL_FAILURE_THRESHOLD {
                        tracing::error!(
                            consecutive_failures,
                            "repeatedly failing to generate stats (seven_days): {:#?}",
                            e
                        );
                    } else {
                        tracing::warn!(
                            consecutive_failures,
                            "error generating stats (seven_days): {:#?}",
                            e
                        );
                    }
                    tokio::time::sleep(stats_task_sleep_after_refresh_with_failures(
                        false,
                        consecutive_failures,
                    ))
                    .await;
                    continue;
                }
            };

            let num_listings = all_time.num_listings();
            consecutive_failures = 0;
            *stats_state.stats.write().await = Some(Arc::new(CachedStatistics {
                all_time,
                seven_days,
            }));

            while listings_rx.try_recv().is_ok() {}

            if num_listings == 0 {
                tokio::select! {
                    _ = tokio::time::sleep(stats_task_sleep_after_success(0)) => {}
                    recv_result = listings_rx.recv() => {
                        match recv_result {
                            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                tokio::time::sleep(STATS_FIRST_DATA_SETTLE_DELAY).await;
                                while listings_rx.try_recv().is_ok() {}
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                tokio::time::sleep(stats_task_sleep_after_success(0)).await;
                            }
                        }
                    }
                }
            } else {
                tokio::time::sleep(stats_task_sleep_after_success(num_listings)).await;
            }
        }
    })
}

pub fn spawn_fflogs_lease_sweeper_task(state: Arc<State>) -> tokio::task::JoinHandle<()> {
    let lease_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        let Some(lease_ttl) = TimeDelta::try_minutes(super::FFLOGS_LEASE_TTL_MINUTES) else {
            tracing::error!(
                ttl_minutes = super::FFLOGS_LEASE_TTL_MINUTES,
                "failed to initialize FFLogs lease sweeper: invalid lease ttl"
            );
            return;
        };

        loop {
            let now = Utc::now();
            let mut leases = lease_state.fflogs_job_leases.write().await;
            let before = leases.len();

            leases.retain(|_, lease| (lease.leased_at + lease_ttl) > now);

            let expired_count = before.saturating_sub(leases.len());
            drop(leases);

            if expired_count > 0 {
                tracing::debug!(
                    expired_count,
                    "fflogs lease sweeper released expired lease(s)"
                );
            }

            tokio::time::sleep(Duration::from_secs(
                super::FFLOGS_LEASE_SWEEP_INTERVAL_SECONDS,
            ))
            .await;
        }
    })
}

pub fn spawn_listings_revision_publisher_task(state: Arc<State>) -> tokio::task::JoinHandle<()> {
    let revision_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        let mut consecutive_failures = 0u32;
        loop {
            revision_state.listings_revision_notify.notified().await;

            loop {
                let sleep = tokio::time::sleep(revision_state.listings_revision_coalesce_window);
                tokio::pin!(sleep);
                let notified = revision_state.listings_revision_notify.notified();
                tokio::pin!(notified);

                tokio::select! {
                    _ = &mut sleep => {
                        match publish_coalesced_listings_change_with_incrementer(
                            &revision_state,
                            |state| async move { increment_listing_source_revision(&state).await },
                        )
                        .await
                        {
                            Ok(_) => {
                                consecutive_failures = 0;
                            }
                            Err(error) => {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                let backoff = publish_retry_backoff(consecutive_failures);
                                if consecutive_failures >= BACKGROUND_FATAL_FAILURE_THRESHOLD {
                                    tracing::error!(
                                        error = ?error,
                                        consecutive_failures,
                                        backoff_ms = backoff.as_millis() as u64,
                                        "repeatedly failed to publish coalesced listings revision change"
                                    );
                                } else {
                                    tracing::warn!(
                                        error = ?error,
                                        consecutive_failures,
                                        backoff_ms = backoff.as_millis() as u64,
                                        "failed to publish coalesced listings revision change"
                                    );
                                }
                                tokio::time::sleep(backoff).await;
                            }
                        }
                        break;
                    }
                    _ = &mut notified => {
                        continue;
                    }
                }
            }
        }
    })
}

async fn publish_coalesced_listings_change_with_incrementer<F, Fut>(
    state: &Arc<State>,
    increment_source_revision: F,
) -> anyhow::Result<Option<u64>>
where
    F: FnOnce(Arc<State>) -> Fut,
    Fut: Future<Output = anyhow::Result<i64>>,
{
    let had_pending = state
        .listings_revision_pending
        .swap(false, AtomicOrdering::Relaxed);
    if !had_pending {
        return Ok(None);
    }

    match state.listings_snapshot_source {
        ListingsSnapshotSource::Inline => {
            let revision = state
                .listings_revision
                .fetch_add(1, AtomicOrdering::Relaxed)
                + 1;
            let _ = state.listings_change_channel.send(revision);
            dispatch_listings_revision_webhook(state, revision);
            Ok(Some(revision))
        }
        ListingsSnapshotSource::Materialized => {
            let source_revision = match increment_source_revision(Arc::clone(state)).await {
                Ok(revision) => revision,
                Err(error) => {
                    state
                        .listings_revision_pending
                        .store(true, AtomicOrdering::Relaxed);
                    state.listings_revision_notify.notify_one();
                    return Err(error);
                }
            };
            tracing::debug!(
                source_revision,
                "incremented materialized listings source revision"
            );
            Ok(None)
        }
    }
}

fn dispatch_listings_revision_webhook(state: &Arc<State>, revision: u64) {
    let Some(webhook) = state.listings_revision_webhook.clone() else {
        return;
    };

    webhook
        .dispatch
        .latest_revision
        .fetch_max(revision, AtomicOrdering::Release);
    if webhook
        .dispatch
        .worker_started
        .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
        .is_ok()
    {
        let worker_webhook = webhook.clone();
        tokio::task::spawn(async move {
            run_listings_revision_webhook_worker(worker_webhook).await;
        });
    }
    webhook.dispatch.notify.notify_one();
}

async fn run_listings_revision_webhook_worker(webhook: ListingsRevisionWebhook) {
    let mut last_sent_revision = 0u64;
    let mut consecutive_failures = 0u32;

    loop {
        let pending = webhook.dispatch.latest_revision.load(AtomicOrdering::Acquire);
        if pending > last_sent_revision {
            match post_listings_revision_webhook(&webhook, pending).await {
                Ok(()) => {
                    last_sent_revision = pending;
                    consecutive_failures = 0;
                }
                Err(error) => {
                    // Advance past the failed revision only when a newer revision arrives;
                    // retrying bounded times avoids spinning on permanently broken targets.
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures >= BACKGROUND_FATAL_FAILURE_THRESHOLD {
                        tracing::error!(
                            error = ?error,
                            revision = pending,
                            consecutive_failures,
                            "repeatedly failed to notify listings revision webhook"
                        );
                        last_sent_revision = pending;
                        consecutive_failures = 0;
                    } else {
                        tracing::warn!(
                            error = ?error,
                            revision = pending,
                            consecutive_failures,
                            "failed to notify listings revision webhook"
                        );
                        tokio::time::sleep(exponential_backoff_seconds(1, 30, consecutive_failures))
                            .await;
                    }
                }
            }
            continue;
        }

        webhook.dispatch.notify.notified().await;
    }
}

async fn post_listings_revision_webhook(
    webhook: &ListingsRevisionWebhook,
    revision: u64,
) -> anyhow::Result<()> {
    let response = webhook
        .client
        .post(webhook.url.clone())
        .bearer_auth(&webhook.shared_secret)
        .json(&serde_json::json!({ "revision": revision }))
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("webhook returned HTTP {}", response.status());
    }

    Ok(())
}

pub fn spawn_materialized_snapshot_reconcile_task(
    state: Arc<State>,
) -> tokio::task::JoinHandle<()> {
    let reconcile_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        if reconcile_state.listings_snapshot_source != ListingsSnapshotSource::Materialized {
            tracing::debug!(
                "materialized listings snapshot reconciliation disabled in inline mode"
            );
            return;
        }

        let sleep_interval = Duration::from_secs(
            reconcile_state
                .materialized_snapshot_reconcile_interval_seconds
                .max(5),
        );
        let mut consecutive_failures = 0u32;

        loop {
            tokio::time::sleep(if consecutive_failures == 0 {
                sleep_interval
            } else {
                reconcile_retry_backoff(sleep_interval, consecutive_failures)
            })
            .await;

            match crate::api::reconcile_materialized_snapshot_once(Arc::clone(&reconcile_state))
                .await
            {
                Ok(_) => {
                    consecutive_failures = 0;
                }
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures >= BACKGROUND_FATAL_FAILURE_THRESHOLD {
                        tracing::error!(
                            error = ?error,
                            consecutive_failures,
                            "repeatedly failed to reconcile materialized listings snapshot"
                        );
                    } else {
                        tracing::warn!(
                            error = ?error,
                            consecutive_failures,
                            "failed to reconcile materialized listings snapshot"
                        );
                    }
                }
            }
        }
    })
}

pub fn spawn_monitor_snapshot_task(state: Arc<State>) -> tokio::task::JoinHandle<()> {
    let monitor_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        let interval_seconds = monitor_state.monitor_snapshot_interval_seconds;
        if interval_seconds == 0 {
            tracing::debug!("monitor snapshot disabled (monitor_snapshot_interval_seconds=0)");
            return;
        }

        let sleep_interval = Duration::from_secs(interval_seconds.max(5));

        loop {
            let active_leases = {
                let leases = monitor_state.fflogs_job_leases.read().await;
                leases.len() as u64
            };

            let jobs_dispatched = monitor_state
                .fflogs_jobs_dispatched_total
                .load(AtomicOrdering::Relaxed);
            let results_received = monitor_state
                .fflogs_results_received_total
                .load(AtomicOrdering::Relaxed);
            let results_rejected = monitor_state
                .fflogs_results_rejected_total
                .load(AtomicOrdering::Relaxed);
            let leases_abandoned = monitor_state
                .fflogs_leases_abandoned_total
                .load(AtomicOrdering::Relaxed);
            let leases_abandon_rejected = monitor_state
                .fflogs_leases_abandon_rejected_total
                .load(AtomicOrdering::Relaxed);
            let hidden_refresh = monitor_state
                .fflogs_hidden_refresh_total
                .load(AtomicOrdering::Relaxed);
            let leader_fallback = monitor_state
                .fflogs_leader_fallback_total
                .load(AtomicOrdering::Relaxed);

            tracing::info!(
                "[MONITOR] FFLogs dispatched={} received={} rejected={} lease_active={} lease_abandoned={} lease_abandon_rejected={} hidden_refresh={} leader_fallback={}",
                jobs_dispatched,
                results_received,
                results_rejected,
                active_leases,
                leases_abandoned,
                leases_abandon_rejected,
                hidden_refresh,
                leader_fallback,
            );

            tokio::time::sleep(sleep_interval).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        publish_coalesced_listings_change_with_incrementer, spawn_listings_revision_publisher_task,
    };
    use crate::config::ListingsSnapshotSource;
    use crate::web::{IngestRateLimits, IngestSecurityConfig, ListingsRevisionWebhook, State};
    use mongodb::Client;
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::{Notify, RwLock};
    use warp::Filter;

    #[test]
    fn stats_task_retries_failures_without_tight_looping() {
        assert_eq!(
            super::stats_task_sleep_after_refresh(false),
            Duration::from_secs(60)
        );
        assert_eq!(
            super::stats_task_sleep_after_success(0),
            Duration::from_secs(60)
        );
        assert_eq!(
            super::stats_task_sleep_after_success(85),
            Duration::from_secs(60 * 15)
        );
        assert_eq!(
            super::stats_task_sleep_after_refresh(true),
            Duration::from_secs(60 * 15)
        );
    }

    async fn test_state_with_coalesce_window(coalesce_window: Duration) -> Arc<State> {
        test_state_with_source(coalesce_window, ListingsSnapshotSource::Inline).await
    }

    async fn test_state_with_source(
        coalesce_window: Duration,
        listings_snapshot_source: ListingsSnapshotSource,
    ) -> Arc<State> {
        let mongo = Client::with_uri_str("mongodb://127.0.0.1:27017")
            .await
            .expect("construct test mongo client");
        let (change_tx, _) = tokio::sync::broadcast::channel(8);

        Arc::new(State {
            mongo,
            stats: Default::default(),
            listings_change_channel: change_tx,
            listings_revision: Default::default(),
            listings_revision_pending: Default::default(),
            listings_revision_notify: Notify::new(),
            listings_revision_coalesce_window: coalesce_window,
            listings_revision_webhook: None,
            listings_snapshot_cache: Default::default(),
            listings_snapshot_source,
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
            ingest_security: IngestSecurityConfig {
                require_signature: false,
                require_signature_for_fflogs: None,
                shared_secret: "test-shared-secret".to_string(),
                clock_skew_seconds: 300,
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
                },
                open_aggregate_rate_limits: IngestRateLimits::default_open_aggregate(),
            },
            ingest_rate_windows: RwLock::new(HashMap::new()),
            ingest_nonces: RwLock::new(HashMap::new()),
            snapshot_refresh_nonces: RwLock::new(HashMap::new()),
            fflogs_job_leases: RwLock::new(HashMap::new()),
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

    fn set_listings_revision_webhook(
        state: Arc<State>,
        url: String,
        shared_secret: &str,
    ) -> Arc<State> {
        let mut state = Arc::try_unwrap(state)
            .unwrap_or_else(|_| panic!("test state should have no extra strong refs"));
        state.listings_revision_webhook = Some(ListingsRevisionWebhook::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("test webhook client should build"),
            reqwest::Url::parse(&url).expect("test webhook URL should parse"),
            shared_secret.to_string(),
        ));
        Arc::new(state)
    }

    #[tokio::test]
    async fn listings_revision_publisher_coalesces_bursts_into_one_revision() {
        let state = test_state_with_coalesce_window(Duration::from_millis(25)).await;
        spawn_listings_revision_publisher_task(Arc::clone(&state));
        let mut receiver = state.listings_change_channel.subscribe();

        let initial_revision = state.current_listings_revision();
        state.notify_listings_changed(1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        state.notify_listings_changed(1);

        let revision = tokio::time::timeout(Duration::from_millis(150), receiver.recv())
            .await
            .expect("coalesced revision should arrive")
            .expect("broadcast should stay open");

        assert_eq!(revision, initial_revision + 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(60), receiver.recv())
                .await
                .is_err(),
            "burst should emit only one revision"
        );
    }

    #[tokio::test]
    async fn listings_revision_publisher_emits_new_revision_after_next_quiet_window() {
        let state = test_state_with_coalesce_window(Duration::from_millis(20)).await;
        spawn_listings_revision_publisher_task(Arc::clone(&state));
        let mut receiver = state.listings_change_channel.subscribe();

        let initial_revision = state.current_listings_revision();
        state.notify_listings_changed(1);
        let first = tokio::time::timeout(Duration::from_millis(120), receiver.recv())
            .await
            .expect("first revision should arrive")
            .expect("broadcast should stay open");

        state.notify_listings_changed(1);
        let second = tokio::time::timeout(Duration::from_millis(120), receiver.recv())
            .await
            .expect("second revision should arrive")
            .expect("broadcast should stay open");

        assert_eq!(first, initial_revision + 1);
        assert_eq!(second, initial_revision + 2);
    }

    #[tokio::test]
    async fn inline_revision_publisher_posts_revision_webhook_when_configured() {
        let delivered = Arc::new(std::sync::Mutex::new(None));
        let (delivery_tx, delivery_rx) = tokio::sync::oneshot::channel();
        *delivered.lock().expect("delivery lock should be available") = Some(delivery_tx);

        let route = {
            let delivered = Arc::clone(&delivered);
            warp::post()
                .and(warp::path("internal"))
                .and(warp::path("listings"))
                .and(warp::path("revision"))
                .and(warp::path::end())
                .and(warp::header::optional::<String>("authorization"))
                .and(warp::body::json())
                .map(
                    move |authorization: Option<String>, body: serde_json::Value| {
                        if let Some(sender) = delivered
                            .lock()
                            .expect("delivery lock should be available")
                            .take()
                        {
                            let _ = sender.send((authorization, body));
                        }
                        warp::reply()
                    },
                )
        };
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::task::spawn(server);

        let state = set_listings_revision_webhook(
            test_state_with_coalesce_window(Duration::from_millis(1)).await,
            format!("http://{addr}/internal/listings/revision"),
            "webhook-secret",
        );

        state.notify_listings_changed(1);
        let revision =
            publish_coalesced_listings_change_with_incrementer(&state, |_state| async { Ok(0) })
                .await
                .expect("inline revision publish should succeed")
                .expect("inline publish should advance revision");

        assert_eq!(revision, 1);

        let (authorization, body) = tokio::time::timeout(Duration::from_secs(2), delivery_rx)
            .await
            .expect("webhook should be delivered")
            .expect("webhook receiver should stay open");

        assert_eq!(authorization.as_deref(), Some("Bearer webhook-secret"));
        assert_eq!(body["revision"], serde_json::json!(1));
    }

    #[tokio::test]
    async fn materialized_revision_publisher_increments_source_revision_without_broadcasting() {
        let state = test_state_with_source(
            Duration::from_millis(1),
            ListingsSnapshotSource::Materialized,
        )
        .await;
        let mut receiver = state.listings_change_channel.subscribe();
        let incremented_source_revision = Arc::new(AtomicBool::new(false));

        state.notify_listings_changed(1);
        publish_coalesced_listings_change_with_incrementer(&state, {
            let incremented_source_revision = Arc::clone(&incremented_source_revision);
            move |_state| async move {
                incremented_source_revision.store(true, Ordering::SeqCst);
                Ok(17)
            }
        })
        .await
        .expect("source revision publish should succeed");

        assert!(incremented_source_revision.load(Ordering::SeqCst));
        assert_eq!(state.current_listings_revision(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err(),
            "materialized source changes must not broadcast browser revisions"
        );
    }

    #[tokio::test]
    async fn materialized_revision_publisher_restores_pending_flag_when_source_increment_fails() {
        let state = test_state_with_source(
            Duration::from_millis(1),
            ListingsSnapshotSource::Materialized,
        )
        .await;

        state.notify_listings_changed(1);
        let error = publish_coalesced_listings_change_with_incrementer(&state, |_state| async {
            Err(anyhow::anyhow!("mongo unavailable"))
        })
        .await
        .expect_err("source revision increment failure should be returned");

        assert!(error.to_string().contains("mongo unavailable"));
        assert!(state.listings_revision_pending.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn materialized_revision_publisher_renotifies_when_source_increment_fails() {
        let state = test_state_with_source(
            Duration::from_millis(1),
            ListingsSnapshotSource::Materialized,
        )
        .await;

        state
            .listings_revision_pending
            .store(true, Ordering::SeqCst);
        publish_coalesced_listings_change_with_incrementer(&state, |_state| async {
            Err(anyhow::anyhow!("transient mongo failure"))
        })
        .await
        .expect_err("first source revision increment should fail");

        tokio::time::timeout(
            Duration::from_millis(50),
            state.listings_revision_notify.notified(),
        )
        .await
        .expect("failed materialized source revision publish should schedule a retry");
    }

    #[test]
    fn supervised_backoff_increases_exponentially_and_caps() {
        assert_eq!(
            super::stats_task_sleep_after_refresh_with_failures(false, 1),
            Duration::from_secs(60)
        );
        assert_eq!(
            super::stats_task_sleep_after_refresh_with_failures(false, 2),
            Duration::from_secs(120)
        );
        assert_eq!(
            super::stats_task_sleep_after_refresh_with_failures(false, 4),
            Duration::from_secs(480)
        );
        assert_eq!(
            super::stats_task_sleep_after_refresh_with_failures(false, 10),
            Duration::from_secs(900)
        );
        assert_eq!(super::publish_retry_backoff(1), Duration::from_secs(1));
        assert_eq!(super::publish_retry_backoff(3), Duration::from_secs(4));
        assert_eq!(super::publish_retry_backoff(10), Duration::from_secs(60));
        assert_eq!(
            super::reconcile_retry_backoff(Duration::from_secs(30), 1),
            Duration::from_secs(30)
        );
        assert_eq!(
            super::reconcile_retry_backoff(Duration::from_secs(30), 5),
            Duration::from_secs(80)
        );
    }

    #[tokio::test]
    async fn dispatch_listings_revision_webhook_coalesces_in_flight_bursts() {
        let delivered = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let (release_first_tx, release_first_rx) = tokio::sync::oneshot::channel();
        let first_started = Arc::new(std::sync::Mutex::new(Some(first_started_tx)));
        let release_first = Arc::new(tokio::sync::Mutex::new(Some(release_first_rx)));

        let route = {
            let delivered = Arc::clone(&delivered);
            let first_started = Arc::clone(&first_started);
            let release_first = Arc::clone(&release_first);
            warp::post()
                .and(warp::path("internal"))
                .and(warp::path("listings"))
                .and(warp::path("revision"))
                .and(warp::path::end())
                .and(warp::body::json())
                .then(move |body: serde_json::Value| {
                    let delivered = Arc::clone(&delivered);
                    let first_started = Arc::clone(&first_started);
                    let release_first = Arc::clone(&release_first);
                    async move {
                        let rev = body["revision"].as_u64().unwrap_or_default();
                        delivered.lock().unwrap().push(rev);
                        if let Some(tx) = first_started.lock().unwrap().take() {
                            let _ = tx.send(());
                        }
                        if let Some(rx) = release_first.lock().await.take() {
                            let _ = rx.await;
                        }
                        warp::reply()
                    }
                })
        };
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::task::spawn(server);

        let state = set_listings_revision_webhook(
            test_state_with_coalesce_window(Duration::from_millis(1)).await,
            format!("http://{addr}/internal/listings/revision"),
            "webhook-secret",
        );

        super::dispatch_listings_revision_webhook(&state, 1);
        tokio::time::timeout(Duration::from_secs(2), first_started_rx)
            .await
            .expect("first webhook call should start")
            .expect("first signal should succeed");

        // While the first request is in-flight, queue several updates.
        super::dispatch_listings_revision_webhook(&state, 2);
        super::dispatch_listings_revision_webhook(&state, 3);
        super::dispatch_listings_revision_webhook(&state, 5);
        let _ = release_first_tx.send(());

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if delivered.lock().unwrap().last().copied() == Some(5) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("latest revision should be delivered");

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(*delivered.lock().unwrap(), vec![1, 5]);
    }
}
