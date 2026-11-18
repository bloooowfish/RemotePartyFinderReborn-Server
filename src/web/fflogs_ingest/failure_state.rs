use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection,
};
use std::collections::{HashMap, HashSet};

use super::jobs::normalized_lease_key;
use super::payloads::AbandonFflogsLease;
pub(super) use crate::web::parse_ingest_common::{
    build_pending_failure_state, is_non_recordable_common_reason, lease_matches,
    release_valid_abandoned_leases, upsert_failure_state_record, AbandonMetadata,
    AcceptedAbandonedLease, FailureTarget, JobFailureStateDoc as FflogsJobFailureStateDoc,
    PendingFailureState, FETCH_FAILED_REASON, FFLOGS_RATE_LIMITED_REASON, TOMESTONE_NO_DATA_REASON,
};
#[cfg(test)]
pub(super) use crate::web::parse_ingest_common::{
    expired_failure_reset_filter, expired_failure_reset_update, FAILURE_RETRY_COOLDOWN,
    RATE_LIMIT_DEFERRED_REASON, UPSTREAM_TRANSIENT_ERROR_REASON, WORKER_BATCH_LIMIT_OVERFLOW_REASON,
    WORKER_CYCLE_ABORTED_REASON, WORKER_SHUTDOWN_DEFERRED_REASON,
};

const TOMESTONE_NO_DATA_FAILURE_CODES: [&str; 3] = [
    "tomestone_autocomplete_not_found",
    "tomestone_requested_encounter_empty",
    "tomestone_missing_requested_encounter",
];

pub(super) fn should_record_failure_state(reason: &str) -> bool {
    !is_non_recordable_common_reason(reason)
        && !reason.trim().eq_ignore_ascii_case(FFLOGS_RATE_LIMITED_REASON)
}

pub(super) fn failure_state_becomes_final(
    reason: &str,
    failure_count: i64,
    threshold: u32,
) -> bool {
    should_record_failure_state(reason) && failure_count >= threshold.max(1) as i64
}

fn is_tomestone_no_data_failure_code(failure_code: &str) -> bool {
    TOMESTONE_NO_DATA_FAILURE_CODES
        .iter()
        .any(|code| failure_code.eq_ignore_ascii_case(code))
}

pub(super) fn final_failure_state_blocks_fetch(
    reason: &str,
    failure_code: &str,
    final_for_processor: bool,
) -> bool {
    if !final_for_processor {
        return false;
    }

    let legacy_fetch_failed_no_data = reason.eq_ignore_ascii_case(FETCH_FAILED_REASON)
        && is_tomestone_no_data_failure_code(failure_code);
    !legacy_fetch_failed_no_data
}

fn failure_state_blocks_fetch_at(doc: &FflogsJobFailureStateDoc, now: DateTime<Utc>) -> bool {
    doc.last_failed_at > now - crate::web::parse_ingest_common::FAILURE_RETRY_COOLDOWN
        && final_failure_state_blocks_fetch(&doc.reason, &doc.failure_code, doc.final_for_processor)
}

pub(super) fn legacy_tomestone_no_data_reset_filter(
    id: &str,
    pending: &PendingFflogsFailureState,
) -> Option<Document> {
    if !pending
        .reason
        .eq_ignore_ascii_case(TOMESTONE_NO_DATA_REASON)
        || !is_tomestone_no_data_failure_code(&pending.failure_code)
    {
        return None;
    }

    Some(doc! {
        "_id": id,
        "reason": FETCH_FAILED_REASON,
        "failure_code": {
            "$in": TOMESTONE_NO_DATA_FAILURE_CODES.to_vec(),
        },
    })
}

pub(super) fn legacy_tomestone_no_data_reset_update(
    pending: &PendingFflogsFailureState,
    now: DateTime<Utc>,
) -> Document {
    doc! {
        "$set": {
            "first_failed_at": now,
            "last_failed_at": now,
            "reason": &pending.reason,
            "source_kind": &pending.source_kind,
            "failure_code": &pending.failure_code,
            "failure_fingerprint": &pending.failure_fingerprint,
            "message": &pending.message,
            "failure_count": 0_i64,
            "final_for_processor": false,
        },
    }
}

fn failure_state_id(
    key: &crate::web::FflogsLeaseKey,
    processor_kind: &str,
    failure_fingerprint: &str,
) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        key.content_id, key.zone_id, key.difficulty_id, processor_kind, failure_fingerprint
    )
}

#[derive(Debug)]
pub(super) struct AbandonedLeaseValidation {
    pub(super) accepted: Vec<AcceptedAbandonedLease<crate::web::FflogsLeaseKey>>,
    pub(super) rejected_total: usize,
    pub(super) reasons: HashSet<String>,
    pub(super) failure_states: Vec<PendingFflogsFailureState>,
}

pub(super) fn validate_abandoned_leases(
    leases: &HashMap<crate::web::FflogsLeaseKey, crate::web::FflogsLeaseEntry>,
    abandoned_leases: Vec<AbandonFflogsLease>,
    client_id: &str,
    request_processor_kind: &str,
) -> AbandonedLeaseValidation {
    let mut accepted = Vec::new();
    let mut rejected_total = 0usize;
    let mut seen = HashSet::new();
    let mut reasons = HashSet::new();
    let mut failure_states = Vec::new();

    for abandoned in abandoned_leases {
        if let Some(reason) = abandoned.reason.as_deref() {
            let reason = reason.trim();
            if !reason.is_empty() {
                reasons.insert(reason.to_string());
            }
        }

        let key = normalized_lease_key(
            abandoned.content_id,
            abandoned.zone_id,
            abandoned.difficulty_id,
        );

        let dedup_key = format!(
            "{}:{}:{}:{}",
            key.content_id, key.zone_id, key.difficulty_id, abandoned.lease_token
        );

        if !seen.insert(dedup_key) {
            continue;
        }

        let Some(lease) = leases.get(&key) else {
            rejected_total += 1;
            continue;
        };
        if !lease_matches(Some(lease), client_id, &abandoned.lease_token) {
            rejected_total += 1;
            continue;
        }

        if let Some(failure_state) = build_pending_failure_state(
            key.clone(),
            FailureTarget {
                content_id: key.content_id,
                zone_id: key.zone_id,
                difficulty_id: key.difficulty_id,
                encounter_id: None,
            },
            AbandonMetadata {
                reason: abandoned.reason.as_deref(),
                source_kind: abandoned.source_kind.as_deref(),
                failure_code: abandoned.failure_code.as_deref(),
                failure_fingerprint: abandoned.failure_fingerprint.as_deref(),
                message: abandoned.message.as_deref(),
            },
            &lease.processor_kind,
            request_processor_kind,
            should_record_failure_state,
        ) {
            failure_states.push(failure_state);
        }

        accepted.push(AcceptedAbandonedLease {
            key,
            client_id: client_id.to_string(),
            lease_token: abandoned.lease_token,
        });
    }

    AbandonedLeaseValidation {
        accepted,
        rejected_total,
        reasons,
        failure_states,
    }
}

pub(super) async fn get_final_failure_keys(
    collection: Collection<mongodb::bson::Document>,
    content_ids: &[u64],
    processor_kind: &str,
) -> anyhow::Result<HashSet<(u64, u32, i32)>> {
    if content_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let ids = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let now = Utc::now();
    let mut cursor = collection
        .clone_with_type::<FflogsJobFailureStateDoc>()
        .find(
            doc! {
                "content_id": { "$in": ids },
                "processor_kind": processor_kind,
                "final_for_processor": true,
                "last_failed_at": { "$gt": now - crate::web::parse_ingest_common::FAILURE_RETRY_COOLDOWN },
            },
            None,
        )
        .await?;

    let mut final_keys = HashSet::new();
    while let Some(doc) = cursor.next().await.transpose()? {
        if doc.content_id >= 0 && doc.zone_id >= 0 && failure_state_blocks_fetch_at(&doc, now) {
            final_keys.insert((doc.content_id as u64, doc.zone_id as u32, doc.difficulty_id));
        }
    }
    Ok(final_keys)
}

pub(super) async fn record_failure_state(
    collection: Collection<mongodb::bson::Document>,
    pending: PendingFflogsFailureState,
    threshold: u32,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let id = failure_state_id(
        &pending.key,
        &pending.processor_kind,
        &pending.failure_fingerprint,
    );
    let legacy_reset = legacy_tomestone_no_data_reset_filter(&id, &pending)
        .map(|filter| (filter, legacy_tomestone_no_data_reset_update(&pending, now)));

    upsert_failure_state_record(
        collection,
        &id,
        &pending,
        threshold,
        legacy_reset,
        failure_state_becomes_final,
    )
    .await
}

pub(super) fn fflogs_failure_state_clear_filter(
    content_id: u64,
    zone_id: u32,
    difficulty_id: i32,
) -> Document {
    doc! {
        "content_id": content_id as i64,
        "zone_id": zone_id as i32,
        "difficulty_id": difficulty_id,
    }
}

pub(super) async fn clear_fflogs_failure_state(
    collection: Collection<mongodb::bson::Document>,
    content_id: u64,
    zone_id: u32,
    difficulty_id: i32,
) -> anyhow::Result<()> {
    collection
        .delete_many(
            fflogs_failure_state_clear_filter(content_id, zone_id, difficulty_id),
            None,
        )
        .await?;
    Ok(())
}

pub(super) type PendingFflogsFailureState = PendingFailureState<crate::web::FflogsLeaseKey>;

#[cfg(test)]
#[path = "failure_state/tests.rs"]
mod tests;
