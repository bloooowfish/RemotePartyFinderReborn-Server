use super::*;
use chrono::TimeZone;
use mongodb::bson::{from_document, to_document};

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 17, 8, 0, 0).unwrap()
}

fn failure(last_failed_at: DateTime<Utc>) -> FflogsJobFailureStateDoc {
    FflogsJobFailureStateDoc {
        id: "legacy-failure".into(),
        content_id: 42,
        zone_id: 73,
        difficulty_id: 101,
        processor_kind: "plugin".into(),
        reason: TOMESTONE_NO_DATA_REASON.into(),
        source_kind: Some("tomestone".into()),
        failure_code: "tomestone_requested_encounter_empty".into(),
        failure_fingerprint: "tomestone_requested_encounter_empty".into(),
        failure_count: 12,
        final_for_processor: true,
        first_failed_at: last_failed_at - chrono::Duration::days(2),
        last_failed_at,
        message: None,
    }
}

#[test]
fn final_failures_block_only_until_the_retry_boundary() {
    let last_failed = now();
    let state = failure(last_failed);
    assert!(failure_state_blocks_fetch_at(
        &state,
        last_failed + FAILURE_RETRY_COOLDOWN - chrono::Duration::milliseconds(1)
    ));
    assert!(!failure_state_blocks_fetch_at(
        &state,
        last_failed + FAILURE_RETRY_COOLDOWN
    ));
    assert!(!failure_state_blocks_fetch_at(
        &state,
        last_failed + chrono::Duration::days(1)
    ));
}

#[test]
fn old_documents_need_no_migration_to_allow_new_data_fetches() {
    // The legacy schema has timestamps but no retry_after/TTL metadata.
    let legacy = to_document(&failure(now() - chrono::Duration::days(10))).unwrap();
    assert!(!legacy.contains_key("retry_after"));
    let state: FflogsJobFailureStateDoc = from_document(legacy).unwrap();
    assert!(!failure_state_blocks_fetch_at(&state, now()));
    assert!(failure_state_becomes_final(
        &state.reason,
        state.failure_count,
        2
    ));
}

#[test]
fn retry_window_resets_count_and_final_status_before_the_new_failure() {
    let mut document = to_document(&failure(now() - FAILURE_RETRY_COOLDOWN)).unwrap();
    let reset = expired_failure_reset_update(now());
    for (field, value) in reset.get_document("$set").unwrap() {
        document.insert(field, value.clone());
    }
    let mut state: FflogsJobFailureStateDoc = from_document(document).unwrap();
    assert_eq!(state.failure_count, 0);
    assert_eq!(state.first_failed_at, now());
    assert_eq!(state.last_failed_at, now());
    assert!(!failure_state_blocks_fetch_at(&state, now()));

    state.failure_count += 1;
    state.final_for_processor = failure_state_becomes_final(&state.reason, state.failure_count, 2);
    assert!(!failure_state_blocks_fetch_at(&state, now()));
    state.failure_count += 1;
    state.final_for_processor = failure_state_becomes_final(&state.reason, state.failure_count, 2);
    assert!(failure_state_blocks_fetch_at(&state, now()));
}

#[test]
fn expired_reset_matches_boundary_and_missing_legacy_timestamp() {
    let filter = expired_failure_reset_filter("legacy-failure", now());
    assert_eq!(filter.get_str("_id").unwrap(), "legacy-failure");
    let alternatives = filter.get_array("$or").unwrap();
    assert_eq!(
        alternatives[0].as_document().unwrap(),
        &doc! { "last_failed_at": { "$lte": now() - FAILURE_RETRY_COOLDOWN } }
    );
    assert_eq!(
        alternatives[1].as_document().unwrap(),
        &doc! { "last_failed_at": { "$exists": false } }
    );
}

#[test]
fn fetch_failed_is_finite_and_deferred_rate_limits_remain_unrecorded() {
    let mut state = failure(now());
    state.reason = FETCH_FAILED_REASON.into();
    state.failure_code = "network_failure".into();
    assert!(failure_state_blocks_fetch_at(&state, now()));
    assert!(!failure_state_blocks_fetch_at(
        &state,
        now() + FAILURE_RETRY_COOLDOWN
    ));
    assert!(!should_record_failure_state(RATE_LIMIT_DEFERRED_REASON));
}
