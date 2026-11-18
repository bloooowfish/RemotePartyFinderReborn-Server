use std::collections::HashMap;

use chrono::{DateTime, TimeDelta, Utc};
use mongodb::{
    Collection,
    bson::{Bson, Document, doc},
};

use super::{MAX_PLAYER_NAME_CHARS, is_known_world_id};
use super::players::PlayerUpsertReport;

#[derive(Clone, Debug)]
pub struct PreparedCharacterIdentityUpsert {
    pub(super) content_id: u64,
    pub(super) name: String,
    pub(super) home_world: u16,
    pub(super) world_name: String,
    pub(super) source: String,
    pub(super) observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub enum CharacterIdentityRejectReason {
    MissingContentId,
    MissingName,
    NameTooLong,
    InvalidHomeWorld,
    ObservedAtTooFarInFuture,
}

pub fn prepare_character_identity_upsert(
    identity: &crate::player::UploadableCharacterIdentity,
    now: DateTime<Utc>,
    max_future_skew_seconds: i64,
) -> Result<PreparedCharacterIdentityUpsert, CharacterIdentityRejectReason> {
    if identity.content_id == 0 {
        return Err(CharacterIdentityRejectReason::MissingContentId);
    }

    let trimmed_name = identity.name.trim();
    if trimmed_name.is_empty() {
        return Err(CharacterIdentityRejectReason::MissingName);
    }
    if trimmed_name.chars().count() > MAX_PLAYER_NAME_CHARS {
        return Err(CharacterIdentityRejectReason::NameTooLong);
    }

    if !is_known_world_id(identity.home_world) {
        return Err(CharacterIdentityRejectReason::InvalidHomeWorld);
    }

    let max_allowed_observed_at = now + TimeDelta::seconds(max_future_skew_seconds.max(0));
    if identity.observed_at > max_allowed_observed_at {
        return Err(CharacterIdentityRejectReason::ObservedAtTooFarInFuture);
    }

    Ok(PreparedCharacterIdentityUpsert {
        content_id: identity.content_id,
        name: identity.name.trim().to_string(),
        home_world: identity.home_world,
        world_name: super::world_name_from_id(identity.home_world).unwrap_or_default(),
        source: identity.source.trim().to_string(),
        observed_at: identity.observed_at,
    })
}

pub(super) fn dedupe_prepared_identities(
    prepared: Vec<PreparedCharacterIdentityUpsert>,
) -> Vec<PreparedCharacterIdentityUpsert> {
    let mut order = Vec::with_capacity(prepared.len());
    let mut by_content_id: HashMap<u64, PreparedCharacterIdentityUpsert> = HashMap::new();

    for op in prepared {
        match by_content_id.get_mut(&op.content_id) {
            Some(existing) => {
                if op.observed_at >= existing.observed_at {
                    *existing = op;
                }
            }
            None => {
                order.push(op.content_id);
                by_content_id.insert(op.content_id, op);
            }
        }
    }

    order
        .into_iter()
        .filter_map(|content_id| by_content_id.remove(&content_id))
        .collect()
}

pub(super) fn build_identity_upsert_pipeline(
    op: &PreparedCharacterIdentityUpsert,
    now: DateTime<Utc>,
) -> Vec<Document> {
    let observed_bson = mongodb::bson::DateTime::from_chrono(op.observed_at);
    let now_bson = mongodb::bson::DateTime::from_chrono(now);
    let should_apply = super::should_apply_identity_expr(observed_bson);
    let source = if op.source.trim().is_empty() {
        "unknown".to_string()
    } else {
        op.source.trim().to_string()
    };

    let home_world_expr: Bson = if op.home_world != 0 {
        doc! {
            "$cond": [
                should_apply.clone(),
                op.home_world as u32,
                { "$ifNull": ["$home_world", 0_u32] },
            ]
        }
        .into()
    } else {
        doc! { "$ifNull": ["$home_world", 0_u32] }.into()
    };

    let world_name_expr: Bson = if !op.world_name.is_empty() {
        doc! {
            "$cond": [
                should_apply.clone(),
                super::bson_literal_str(&op.world_name),
                { "$ifNull": ["$world_name", "$$REMOVE"] },
            ]
        }
        .into()
    } else {
        doc! { "$ifNull": ["$world_name", "$$REMOVE"] }.into()
    };

    let current_world_expr: Bson = if op.home_world != 0 {
        doc! {
            "$cond": [
                { "$gt": [{ "$ifNull": ["$current_world", 0_i32] }, 0_i32] },
                "$current_world",
                {
                    "$cond": [
                        should_apply.clone(),
                        op.home_world as u32,
                        { "$ifNull": ["$current_world", 0_u32] },
                    ]
                },
            ]
        }
        .into()
    } else {
        doc! { "$ifNull": ["$current_world", 0_u32] }.into()
    };

    vec![doc! {
        "$set": {
            "content_id": { "$ifNull": ["$content_id", op.content_id as i64] },
            "name": {
                "$cond": [
                    should_apply.clone(),
                    super::bson_literal_str(&op.name),
                    "$name",
                ]
            },
            "home_world": home_world_expr,
            "world_name": world_name_expr,
            "identity_source": {
                "$cond": [
                    should_apply.clone(),
                    super::bson_literal_str(&source),
                    { "$ifNull": ["$identity_source", "$$REMOVE"] },
                ]
            },
            "identity_observed_at": {
                "$cond": [
                    should_apply,
                    { "$max": ["$identity_observed_at", observed_bson] },
                    { "$ifNull": ["$identity_observed_at", "$$REMOVE"] },
                ]
            },
            "current_world": current_world_expr,
            "last_seen": { "$ifNull": ["$last_seen", now_bson] },
            "seen_count": { "$ifNull": ["$seen_count", 0_i32] },
            "account_id": { "$ifNull": ["$account_id", "-1"] },
        }
    }]
}

pub async fn upsert_character_identities(
    collection: Collection<crate::player::Player>,
    identities: &[crate::player::UploadableCharacterIdentity],
    _concurrency: usize,
    max_future_skew_seconds: i64,
) -> anyhow::Result<PlayerUpsertReport> {
    let mut prepared = Vec::with_capacity(identities.len());
    let mut invalid_missing_content_id = 0usize;
    let mut invalid_missing_name = 0usize;
    let mut invalid_name_too_long = 0usize;
    let mut invalid_home_world = 0usize;
    let mut invalid_future_observed_at = 0usize;
    let now = Utc::now();

    for identity in identities {
        match prepare_character_identity_upsert(identity, now, max_future_skew_seconds) {
            Ok(op) => prepared.push(op),
            Err(CharacterIdentityRejectReason::MissingContentId) => invalid_missing_content_id += 1,
            Err(CharacterIdentityRejectReason::MissingName) => invalid_missing_name += 1,
            Err(CharacterIdentityRejectReason::NameTooLong) => invalid_name_too_long += 1,
            Err(CharacterIdentityRejectReason::InvalidHomeWorld) => invalid_home_world += 1,
            Err(CharacterIdentityRejectReason::ObservedAtTooFarInFuture) => {
                invalid_future_observed_at += 1
            }
        }
    }

    let invalid = invalid_missing_content_id
        + invalid_missing_name
        + invalid_name_too_long
        + invalid_home_world
        + invalid_future_observed_at;
    if invalid > 0 {
        tracing::debug!(
            requested = identities.len(),
            invalid,
            invalid_missing_content_id,
            invalid_missing_name,
            invalid_name_too_long,
            invalid_home_world,
            invalid_future_observed_at,
            "Dropped invalid character identity rows before MongoDB upsert"
        );
    }

    let accepted = prepared.len();
    let deduped = dedupe_prepared_identities(prepared);
    let batch_items = deduped
        .iter()
        .map(|op| {
            super::BatchUpdateItem::upsert_pipeline(
                doc! { "content_id": op.content_id as i64 },
                build_identity_upsert_pipeline(op, now),
            )
        })
        .collect::<Vec<_>>();

    let summary = super::run_batched_updates(&collection, &batch_items, true).await?;
    for failure in &summary.failures {
        let content_id = deduped
            .get(failure.item_index)
            .map(|op| op.content_id)
            .unwrap_or_default();
        tracing::error!(
            content_id,
            code = failure.code,
            error = %failure.errmsg,
            "MongoDB character identity upsert failed"
        );
    }

    let report = PlayerUpsertReport {
        requested: identities.len(),
        accepted,
        updated: summary.changed_count(),
        invalid,
        failed: summary.failures.len(),
    };

    if report.failed == 0 {
        tracing::debug!(
            requested = report.requested,
            accepted = report.accepted,
            updated = report.updated,
            invalid = report.invalid,
            failed = report.failed,
            "MongoDB character identity upsert batch completed"
        );
    } else {
        tracing::warn!(
            requested = report.requested,
            accepted = report.accepted,
            updated = report.updated,
            invalid = report.invalid,
            failed = report.failed,
            "MongoDB character identity upsert batch completed with write failures"
        );
    }

    Ok(report)
}
