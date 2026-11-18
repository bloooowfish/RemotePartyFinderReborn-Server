pub mod batch;
mod identities;
mod listings;
mod parse_cache;
mod players;

pub use batch::{
    BatchUpdateItem, BatchUpdateOp, BatchUpdateSummary, BatchWriteError, MONGO_BATCH_CHUNK_SIZE,
    build_batch_delete_command, build_batch_update_command, parse_batch_delete_response,
    parse_batch_update_response, run_batched_deletes, run_batched_updates,
};
pub use identities::{
    CharacterIdentityRejectReason, PreparedCharacterIdentityUpsert,
    prepare_character_identity_upsert, upsert_character_identities,
};
pub use listings::{
    build_listing_upsert_doc, get_current_listings, insert_listing,
    upsert_listing_stats_histories_batch, upsert_listing_stats_history,
};
pub use parse_cache::{
    EncounterParse, ParseCacheDoc, ParseCacheProvider, PhaseProgress, ZoneCache,
    ZoneCacheUpsertItem, get_parse_docs, get_zone_cache, get_zone_caches, is_zone_cache_expired,
    is_zone_cache_expired_with_hidden_ttl_hours, resolve_fflogs_rank_coverage, upsert_zone_cache,
    upsert_zone_cache_with_fflogs_coverage, upsert_zone_caches_batch,
};
pub use players::{PlayerUpsertReport, get_players_by_content_ids, upsert_players};

pub(crate) const MAX_PLAYER_NAME_CHARS: usize = 32;

pub fn bson_literal_str(value: &str) -> mongodb::bson::Document {
    mongodb::bson::doc! { "$literal": value }
}

pub(crate) fn is_known_world_id(world_id: u16) -> bool {
    world_id != 0 && crate::ffxiv::WORLDS.contains_key(&u32::from(world_id))
}

pub(crate) fn is_known_or_zero_world_id(world_id: u16) -> bool {
    world_id == 0 || crate::ffxiv::WORLDS.contains_key(&u32::from(world_id))
}

pub(super) fn world_name_from_id(home_world: u16) -> Option<String> {
    if home_world == 0 {
        return None;
    }

    crate::ffxiv::WORLDS
        .get(&u32::from(home_world))
        .map(|world| world.name().trim().to_string())
        .filter(|name: &String| !name.is_empty())
}

pub(super) fn should_apply_identity_expr(
    incoming_ts: mongodb::bson::DateTime,
) -> mongodb::bson::Document {
    let freshness = mongodb::bson::doc! { "$ifNull": ["$identity_observed_at", "$last_seen"] };
    mongodb::bson::doc! {
        "$or": [
            { "$eq": [{ "$ifNull": ["$name", ""] }, ""] },
            { "$eq": [{ "$ifNull": ["$home_world", 0_i32] }, 0_i32] },
            { "$eq": [freshness.clone(), mongodb::bson::Bson::Null] },
            { "$gte": [incoming_ts, freshness] },
        ]
    }
}

pub fn is_duplicate_key_error(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write_error)) => {
            write_error.code == 11_000
        }
        mongodb::error::ErrorKind::BulkWrite(bulk_failure) => bulk_failure
            .write_errors
            .as_ref()
            .is_some_and(|errors| errors.iter().any(|error| error.code == 11_000)),
        mongodb::error::ErrorKind::Command(command_error) => command_error.code == 11_000,
        _ => false,
    }
}

#[cfg(test)]
mod tests;
