use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use mongodb::{
    Collection,
    bson::{Bson, Document, doc},
    options::FindOptions,
};

use super::{MAX_PLAYER_NAME_CHARS, is_known_or_zero_world_id, is_known_world_id};

#[derive(Clone, Debug)]
pub(super) struct PreparedPlayerUpsert {
    pub(super) content_id: u64,
    pub(super) name: String,
    pub(super) home_world: u16,
    pub(super) current_world: u16,
    pub(super) account_id: String,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PlayerUpsertRejectReason {
    MissingContentId,
    MissingName,
    NameTooLong,
    InvalidHomeWorld,
}

#[derive(Debug, Clone, Copy)]
pub struct PlayerUpsertReport {
    pub requested: usize,
    pub accepted: usize,
    pub updated: usize,
    pub invalid: usize,
    pub failed: usize,
}

pub(super) fn prepare_player_upsert(
    player: &crate::player::UploadablePlayer,
) -> Result<PreparedPlayerUpsert, PlayerUpsertRejectReason> {
    if player.content_id == 0 {
        return Err(PlayerUpsertRejectReason::MissingContentId);
    }

    let trimmed_name = player.name.trim();
    if trimmed_name.is_empty() {
        return Err(PlayerUpsertRejectReason::MissingName);
    }
    if trimmed_name.chars().count() > MAX_PLAYER_NAME_CHARS {
        return Err(PlayerUpsertRejectReason::NameTooLong);
    }

    if !is_known_or_zero_world_id(player.home_world) {
        return Err(PlayerUpsertRejectReason::InvalidHomeWorld);
    }

    let current_world = if is_known_world_id(player.current_world) {
        player.current_world
    } else {
        0
    };

    let account_id = if player.account_id != 0 {
        player.account_id.to_string()
    } else {
        "-1".to_string()
    };

    Ok(PreparedPlayerUpsert {
        content_id: player.content_id,
        name: trimmed_name.to_string(),
        home_world: player.home_world,
        current_world,
        account_id,
    })
}

pub(super) fn dedupe_prepared_player_upserts(
    prepared: Vec<PreparedPlayerUpsert>,
) -> Vec<PreparedPlayerUpsert> {
    let mut order = Vec::with_capacity(prepared.len());
    let mut by_content_id: HashMap<u64, PreparedPlayerUpsert> = HashMap::new();

    for op in prepared {
        match by_content_id.get_mut(&op.content_id) {
            Some(existing) => {
                if existing.account_id == "-1" && op.account_id != "-1" {
                    existing.account_id = op.account_id;
                } else if op.account_id != "-1" {
                    existing.account_id = op.account_id;
                }
                if op.current_world != 0 {
                    existing.current_world = op.current_world;
                }
                if op.home_world != 0 || existing.home_world == 0 {
                    existing.name = op.name;
                    existing.home_world = op.home_world;
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

pub(super) fn build_player_upsert_pipeline(
    op: &PreparedPlayerUpsert,
    now: DateTime<Utc>,
) -> Vec<Document> {
    let now_bson = mongodb::bson::DateTime::from_chrono(now);
    let should_apply = super::should_apply_identity_expr(now_bson);
    let derived_world_name = super::world_name_from_id(op.home_world);

    let account_id_expr: Bson = if op.account_id == "-1" {
        doc! { "$ifNull": ["$account_id", "-1"] }.into()
    } else {
        op.account_id.clone().into()
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

    let world_name_expr: Bson = match derived_world_name {
        Some(world_name) => doc! {
            "$cond": [
                should_apply.clone(),
                super::bson_literal_str(&world_name),
                { "$ifNull": ["$world_name", "$$REMOVE"] },
            ]
        }
        .into(),
        None => doc! { "$ifNull": ["$world_name", "$$REMOVE"] }.into(),
    };

    let current_world_expr: Bson = if op.current_world != 0 {
        (op.current_world as u32).into()
    } else {
        doc! { "$ifNull": ["$current_world", 0_u32] }.into()
    };

    vec![doc! {
        "$set": {
            "content_id": { "$ifNull": ["$content_id", op.content_id as i64] },
            "account_id": account_id_expr,
            "name": {
                "$cond": [
                    should_apply.clone(),
                    super::bson_literal_str(&op.name),
                    "$name",
                ]
            },
            "last_seen": { "$max": ["$last_seen", now_bson] },
            "identity_observed_at": {
                "$cond": [
                    should_apply,
                    { "$max": ["$identity_observed_at", now_bson] },
                    { "$ifNull": ["$identity_observed_at", "$$REMOVE"] },
                ]
            },
            "seen_count": { "$add": [{ "$ifNull": ["$seen_count", 0_i32] }, 1_i32] },
            "home_world": home_world_expr,
            "world_name": world_name_expr,
            "current_world": current_world_expr,
        }
    }]
}

pub async fn upsert_players(
    collection: Collection<crate::player::Player>,
    players: &[crate::player::UploadablePlayer],
    _concurrency: usize,
) -> anyhow::Result<PlayerUpsertReport> {
    let mut prepared = Vec::with_capacity(players.len());
    let mut invalid_missing_content_id = 0usize;
    let mut invalid_missing_name = 0usize;
    let mut invalid_name_too_long = 0usize;
    let mut invalid_home_world = 0usize;

    for player in players {
        match prepare_player_upsert(player) {
            Ok(op) => prepared.push(op),
            Err(PlayerUpsertRejectReason::MissingContentId) => invalid_missing_content_id += 1,
            Err(PlayerUpsertRejectReason::MissingName) => invalid_missing_name += 1,
            Err(PlayerUpsertRejectReason::NameTooLong) => invalid_name_too_long += 1,
            Err(PlayerUpsertRejectReason::InvalidHomeWorld) => invalid_home_world += 1,
        }
    }

    let invalid = invalid_missing_content_id
        + invalid_missing_name
        + invalid_name_too_long
        + invalid_home_world;
    if invalid > 0 {
        tracing::debug!(
            requested = players.len(),
            invalid,
            invalid_missing_content_id,
            invalid_missing_name,
            invalid_name_too_long,
            invalid_home_world,
            "Dropped invalid player rows before MongoDB upsert"
        );
    }

    let now = Utc::now();
    let accepted = prepared.len();
    let deduped = dedupe_prepared_player_upserts(prepared);
    let batch_items = deduped
        .iter()
        .map(|op| {
            super::BatchUpdateItem::upsert_pipeline(
                doc! { "content_id": op.content_id as i64 },
                build_player_upsert_pipeline(op, now),
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
            "MongoDB player upsert failed"
        );
    }

    let report = PlayerUpsertReport {
        requested: players.len(),
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
            "MongoDB player upsert batch completed"
        );
    } else {
        tracing::warn!(
            requested = report.requested,
            accepted = report.accepted,
            updated = report.updated,
            invalid = report.invalid,
            failed = report.failed,
            "MongoDB player upsert batch completed with write failures"
        );
    }

    Ok(report)
}

pub async fn get_players_by_content_ids(
    collection: Collection<crate::player::Player>,
    content_ids: &[u64],
) -> anyhow::Result<Vec<crate::player::Player>> {
    let as_i64 = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(
            doc! { "content_id": { "$in": as_i64 } },
            Some(player_lookup_find_options()),
        )
        .await?;

    let players = cursor.try_collect::<Vec<_>>().await?;

    Ok(dedupe_players_by_preferred_order(players))
}

pub(super) fn player_lookup_find_options() -> FindOptions {
    FindOptions::builder()
        .sort(doc! {
            "content_id": 1,
            "identity_observed_at": -1,
            "last_seen": -1,
            "seen_count": -1,
            "_id": 1,
        })
        .build()
}

pub(super) fn dedupe_players_by_preferred_order(
    players: Vec<crate::player::Player>,
) -> Vec<crate::player::Player> {
    let mut seen = HashSet::new();
    players
        .into_iter()
        .filter(|player| seen.insert(player.content_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_prepared(account_id: &str) -> PreparedPlayerUpsert {
        PreparedPlayerUpsert {
            content_id: 9001,
            name: "Verified Player".to_string(),
            home_world: 73,
            current_world: 74,
            account_id: account_id.to_string(),
        }
    }

    #[test]
    fn player_upsert_pipeline_preserves_verified_account_id_when_incoming_is_unknown() {
        let now = Utc::now();
        let pipeline = build_player_upsert_pipeline(&sample_prepared("-1"), now);
        let set_doc = pipeline[0]
            .get_document("$set")
            .expect("pipeline must contain $set");

        assert_eq!(
            set_doc.get_document("account_id"),
            Ok(&doc! { "$ifNull": ["$account_id", "-1"] })
        );
        assert_eq!(
            set_doc.get_document("seen_count"),
            Ok(&doc! { "$add": [{ "$ifNull": ["$seen_count", 0_i32] }, 1_i32] })
        );
    }

    #[test]
    fn player_upsert_pipeline_updates_verified_account_id() {
        let now = Utc::now();
        let pipeline = build_player_upsert_pipeline(&sample_prepared("777888999"), now);
        let set_doc = pipeline[0]
            .get_document("$set")
            .expect("pipeline must contain $set");

        assert_eq!(set_doc.get_str("account_id"), Ok("777888999"));
    }
}
