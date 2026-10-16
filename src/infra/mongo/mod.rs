mod identities;
mod listings;
mod parse_cache;
mod players;

pub use identities::{
    CharacterIdentityRejectReason, PreparedCharacterIdentityUpsert,
    prepare_character_identity_upsert, upsert_character_identities,
};
pub use listings::{get_current_listings, insert_listing};
pub use parse_cache::{
    EncounterParse, ParseCacheDoc, PhaseProgress, ZoneCache, get_parse_docs, get_zone_cache,
    get_zone_caches, is_zone_cache_expired, is_zone_cache_expired_with_hidden_ttl_hours,
    upsert_zone_cache,
};
pub use players::{
    PlayerUpsertReport, get_all_active_players, get_players_by_content_ids, upsert_players,
};

fn is_valid_world_id(world_id: u16) -> bool {
    world_id < 1_000
}

fn is_duplicate_key_error(error: &mongodb::error::Error) -> bool {
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
