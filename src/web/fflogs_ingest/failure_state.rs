use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{FindOneAndUpdateOptions, ReturnDocument},
    Collection,
};
use std::collections::{HashMap, HashSet};

use super::jobs::normalized_lease_key;
use super::payloads::AbandonFflogsLease;
use super::request::{
    normalize_failure_field, normalize_failure_message, normalize_processor_kind,
    FAILURE_FIELD_MAX_CHARS, RATE_LIMIT_DEFERRED_REASON,
};

pub(super) const FETCH_FAILED_REASON: &str = "fetch_failed";
pub(super) const TOMESTONE_NO_DATA_REASON: &str = "tomestone_no_data";
// Failure suppression is a retry cooldown, independent of successful-data freshness.
const FAILURE_RETRY_COOLDOWN: chrono::Duration = chrono::Duration::hours(1);
const TOMESTONE_NO_DATA_FAILURE_CODES: [&str; 3] = [
    "tomestone_autocomplete_not_found",
    "tomestone_requested_encounter_empty",
    "tomestone_missing_requested_encounter",
];

pub(super) fn should_record_failure_state(reason: &str) -> bool {
    !reason.eq_ignore_ascii_case(RATE_LIMIT_DEFERRED_REASON)
}

pub(super) fn failure_state_becomes_final(
    reason: &str,
    failure_count: i64,
    threshold: u32,
) -> bool {
    if reason.eq_ignore_ascii_case(RATE_LIMIT_DEFERRED_REASON) {
        return false;
    }
    if reason.eq_ignore_ascii_case(TOMESTONE_NO_DATA_REASON) {
        return failure_count >= threshold.max(1) as i64;
    }
    reason.eq_ignore_ascii_case(FETCH_FAILED_REASON) || failure_count >= threshold.max(1) as i64
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
    doc.last_failed_at > now - FAILURE_RETRY_COOLDOWN
        && final_failure_state_blocks_fetch(&doc.reason, &doc.failure_code, doc.final_for_processor)
}

fn expired_failure_reset_filter(id: &str, now: DateTime<Utc>) -> Document {
    doc! {
        "_id": id,
        "$or": [
            { "last_failed_at": { "$lte": now - FAILURE_RETRY_COOLDOWN } },
            { "last_failed_at": { "$exists": false } },
        ],
    }
}

fn expired_failure_reset_update(now: DateTime<Utc>) -> Document {
    doc! {
        "$set": {
            "first_failed_at": now,
            "last_failed_at": now,
            "failure_count": 0_i64,
            "final_for_processor": false,
        },
    }
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

fn failure_state_from_abandon(
    key: crate::web::FflogsLeaseKey,
    abandoned: &AbandonFflogsLease,
    request_processor_kind: &str,
) -> Option<PendingFflogsFailureState> {
    let reason = abandoned
        .reason
        .as_deref()
        .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    if !should_record_failure_state(&reason) {
        return None;
    }

    let processor_kind = abandoned
        .processor_kind
        .as_deref()
        .map(normalize_processor_kind)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| request_processor_kind.to_string());

    let failure_code = abandoned
        .failure_code
        .as_deref()
        .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| reason.clone());

    let failure_fingerprint = abandoned
        .failure_fingerprint
        .as_deref()
        .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| failure_code.clone());

    Some(PendingFflogsFailureState {
        key,
        processor_kind,
        reason,
        source_kind: abandoned
            .source_kind
            .as_deref()
            .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
            .filter(|value| !value.is_empty()),
        failure_code,
        failure_fingerprint,
        message: abandoned
            .message
            .as_deref()
            .map(normalize_failure_message)
            .filter(|value| !value.is_empty()),
    })
}

#[derive(Debug)]
pub(super) struct AcceptedAbandonedLease {
    key: crate::web::FflogsLeaseKey,
    client_id: String,
    lease_token: String,
}

#[derive(Debug)]
pub(super) struct AbandonedLeaseValidation {
    pub(super) accepted: Vec<AcceptedAbandonedLease>,
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

        let valid = matches!(
            leases.get(&key),
            Some(lease)
                if !abandoned.lease_token.is_empty()
                    && lease.client_id == client_id
                    && lease.lease_token == abandoned.lease_token
        );

        if !valid {
            rejected_total += 1;
            continue;
        }

        if let Some(failure_state) =
            failure_state_from_abandon(key.clone(), &abandoned, request_processor_kind)
        {
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

pub(super) fn release_valid_abandoned_leases(
    leases: &mut HashMap<crate::web::FflogsLeaseKey, crate::web::FflogsLeaseEntry>,
    accepted: &[AcceptedAbandonedLease],
) -> usize {
    let mut released_total = 0usize;

    for abandoned in accepted {
        let valid = matches!(
            leases.get(&abandoned.key),
            Some(lease)
                if lease.client_id == abandoned.client_id
                    && lease.lease_token == abandoned.lease_token
        );

        if valid && leases.remove(&abandoned.key).is_some() {
            released_total += 1;
        }
    }

    released_total
}

pub(super) async fn get_final_failure_content_ids(
    collection: Collection<mongodb::bson::Document>,
    content_ids: &[u64],
    zone_id: u32,
    difficulty_id: i32,
    processor_kind: &str,
) -> anyhow::Result<HashSet<u64>> {
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
                "zone_id": zone_id as i32,
                "difficulty_id": difficulty_id,
                "processor_kind": processor_kind,
                "final_for_processor": true,
                "last_failed_at": { "$gt": now - FAILURE_RETRY_COOLDOWN },
            },
            None,
        )
        .await?;

    let mut final_content_ids = HashSet::new();
    while let Some(doc) = cursor.next().await.transpose()? {
        if doc.content_id >= 0 && failure_state_blocks_fetch_at(&doc, now) {
            final_content_ids.insert(doc.content_id as u64);
        }
    }
    Ok(final_content_ids)
}

pub(super) async fn record_failure_state(
    collection: Collection<mongodb::bson::Document>,
    pending: PendingFflogsFailureState,
    threshold: u32,
) -> anyhow::Result<()> {
    let collection = collection.clone_with_type::<FflogsJobFailureStateDoc>();
    let now = Utc::now();
    let id = failure_state_id(
        &pending.key,
        &pending.processor_kind,
        &pending.failure_fingerprint,
    );

    // Reset the counter before recording the first failure of a new retry window.
    // This also applies to legacy documents without any expiry metadata.
    collection
        .update_one(
            expired_failure_reset_filter(&id, now),
            expired_failure_reset_update(now),
            None,
        )
        .await?;

    if let Some(filter) = legacy_tomestone_no_data_reset_filter(&id, &pending) {
        collection
            .update_one(
                filter,
                legacy_tomestone_no_data_reset_update(&pending, now),
                None,
            )
            .await?;
    }

    let updated = collection
        .find_one_and_update(
            doc! { "_id": &id },
            doc! {
                "$inc": { "failure_count": 1_i64 },
                "$set": {
                    "last_failed_at": now,
                    "reason": &pending.reason,
                    "source_kind": &pending.source_kind,
                    "failure_code": &pending.failure_code,
                    "message": &pending.message,
                },
                "$setOnInsert": {
                    "_id": &id,
                    "content_id": pending.key.content_id as i64,
                    "zone_id": pending.key.zone_id as i32,
                    "difficulty_id": pending.key.difficulty_id,
                    "processor_kind": &pending.processor_kind,
                    "failure_fingerprint": &pending.failure_fingerprint,
                    "first_failed_at": now,
                    "final_for_processor": false,
                },
            },
            FindOneAndUpdateOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::After)
                .build(),
        )
        .await?;

    let Some(updated) = updated else {
        return Ok(());
    };

    let final_for_processor =
        failure_state_becomes_final(&pending.reason, updated.failure_count, threshold);
    if updated.final_for_processor != final_for_processor {
        collection
            .update_one(
                doc! { "_id": &id },
                doc! {
                    "$set": {
                        "final_for_processor": final_for_processor,
                        "last_failed_at": now,
                    },
                },
                None,
            )
            .await?;
    }

    Ok(())
}

/// FFLogs lease 즉시 반납 요청 구조체 (Plugin -> Server request)

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FflogsJobFailureStateDoc {
    #[serde(rename = "_id")]
    id: String,
    content_id: i64,
    zone_id: i32,
    difficulty_id: i32,
    processor_kind: String,
    reason: String,
    source_kind: Option<String>,
    failure_code: String,
    failure_fingerprint: String,
    failure_count: i64,
    final_for_processor: bool,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    first_failed_at: chrono::DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    last_failed_at: chrono::DateTime<Utc>,
    message: Option<String>,
}

#[derive(Debug)]
pub(super) struct PendingFflogsFailureState {
    pub(super) key: crate::web::FflogsLeaseKey,
    pub(super) processor_kind: String,
    pub(super) reason: String,
    pub(super) source_kind: Option<String>,
    pub(super) failure_code: String,
    pub(super) failure_fingerprint: String,
    pub(super) message: Option<String>,
}

#[cfg(test)]
#[path = "failure_state/tests.rs"]
mod tests;
