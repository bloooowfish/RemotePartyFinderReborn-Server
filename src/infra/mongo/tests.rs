use super::batch::{
    BatchUpdateItem, MONGO_BATCH_CHUNK_SIZE, build_batch_delete_command,
    build_batch_update_command, parse_batch_delete_response, parse_batch_update_response,
};
use super::identities::{
    build_identity_upsert_pipeline, dedupe_prepared_identities, prepare_character_identity_upsert,
};
use super::is_duplicate_key_error;
use super::parse_cache::{
    ParseCacheProvider, ZoneCache, ZoneCacheUpsertItem, build_zone_cache_batch_update_item,
    zone_cache_find_options,
};
use super::players::{
    PreparedPlayerUpsert, build_player_upsert_pipeline, dedupe_players_by_preferred_order,
    dedupe_prepared_player_upserts, player_lookup_find_options,
};
use chrono::Utc;
use mongodb::bson::{Bson, doc};

#[test]
fn fflogs_zone_cache_key_excludes_partition() {
    assert_eq!(crate::fflogs::make_zone_cache_key(73, 101), "73:101");
}

#[test]
fn compatible_zone_lookup_reads_legacy_partitioned_keys_without_partition_input() {
    let zones =
        std::collections::HashMap::from([("73:101:7".to_string(), 7), ("73:101:1".to_string(), 1)]);

    let value = crate::fflogs::get_compatible_zone(&zones, "73:101").copied();

    assert!(matches!(value, Some(1 | 7)));
}

#[test]
fn identity_player_upsert_sets_monotonic_freshness_and_world_name_from_worlds() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let now_bson = mongodb::bson::DateTime::from_chrono(now);
    let prepared = PreparedPlayerUpsert {
        content_id: 1001,
        name: "Known Player".to_string(),
        home_world: 74,
        current_world: 79,
        account_id: "123".to_string(),
    };

    let pipeline = build_player_upsert_pipeline(&prepared, now);
    let set_doc = pipeline[0]
        .get_document("$set")
        .expect("player upsert pipeline must start with $set");

    assert_eq!(
        set_doc.get_document("last_seen"),
        Ok(&doc! { "$max": ["$last_seen", now_bson] })
    );
    let identity_observed_cond = set_doc
        .get_document("identity_observed_at")
        .unwrap()
        .get_array("$cond")
        .expect("identity_observed_at must be gated monotonically");
    assert_eq!(
        identity_observed_cond[1].as_document().unwrap(),
        &doc! { "$max": ["$identity_observed_at", now_bson] }
    );
    assert_eq!(
        identity_observed_cond[2].as_document().unwrap(),
        &doc! { "$ifNull": ["$identity_observed_at", "$$REMOVE"] }
    );
    let world_name_cond = set_doc
        .get_document("world_name")
        .unwrap()
        .get_array("$cond")
        .expect("world_name should be derived from WORLDS and gated by freshness");
    assert_eq!(world_name_cond[1].as_str(), Some("Coeurl"));
    assert_eq!(
        set_doc.get_document("content_id"),
        Ok(&doc! { "$ifNull": ["$content_id", 1001_i64] })
    );
}

#[test]
fn prepare_character_identity_derives_world_name_from_worlds() {
    let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let identity = crate::player::UploadableCharacterIdentity {
        content_id: 6006,
        name: "  Server World  ".to_string(),
        home_world: 74,
        world_name: "Untrusted Client World".to_string(),
        source: "  chara_card ".to_string(),
        observed_at,
    };

    let prepared = prepare_character_identity_upsert(&identity, observed_at, 300)
        .expect("valid character identity should prepare");
    assert_eq!(prepared.name, "Server World");
    assert_eq!(prepared.world_name, "Coeurl");
    assert_eq!(prepared.source, "chara_card");
}

#[test]
fn identity_upsert_pipeline_preserves_existing_fields_when_stale_and_avoids_spurious_writes() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let now_bson = mongodb::bson::DateTime::from_chrono(now);
    let observed_at = now - chrono::TimeDelta::minutes(5);
    let observed_bson = mongodb::bson::DateTime::from_chrono(observed_at);

    let prepared = prepare_character_identity_upsert(
        &crate::player::UploadableCharacterIdentity {
            content_id: 8008,
            name: "Known Player".to_string(),
            home_world: 74,
            world_name: "Spoofed".to_string(),
            source: "plugin".to_string(),
            observed_at,
        },
        now,
        300,
    )
    .unwrap();

    let pipeline = build_identity_upsert_pipeline(&prepared, now);
    let set_doc = pipeline[0]
        .get_document("$set")
        .expect("identity pipeline must have $set stage");

    assert_eq!(
        set_doc.get_document("content_id"),
        Ok(&doc! { "$ifNull": ["$content_id", 8008_i64] })
    );
    assert_eq!(
        set_doc.get_document("last_seen"),
        Ok(&doc! { "$ifNull": ["$last_seen", now_bson] })
    );
    assert_eq!(
        set_doc.get_document("seen_count"),
        Ok(&doc! { "$ifNull": ["$seen_count", 0_i32] })
    );
    assert_eq!(
        set_doc.get_document("account_id"),
        Ok(&doc! { "$ifNull": ["$account_id", "-1"] })
    );
    let obs_cond = set_doc
        .get_document("identity_observed_at")
        .unwrap()
        .get_array("$cond")
        .unwrap();
    assert_eq!(
        obs_cond[1].as_document().unwrap(),
        &doc! { "$max": ["$identity_observed_at", observed_bson] }
    );
    assert_eq!(
        obs_cond[2].as_document().unwrap(),
        &doc! { "$ifNull": ["$identity_observed_at", "$$REMOVE"] }
    );
    let world_cond = set_doc
        .get_document("world_name")
        .unwrap()
        .get_array("$cond")
        .unwrap();
    assert_eq!(world_cond[1].as_str(), Some("Coeurl"));
}

#[test]
fn identity_and_player_batch_dedupe_keeps_freshest_per_content_id() {
    let t1 = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let t2 = t1 + chrono::TimeDelta::seconds(30);

    let newer = prepare_character_identity_upsert(
        &crate::player::UploadableCharacterIdentity {
            content_id: 5005,
            name: "Newer".to_string(),
            home_world: 74,
            world_name: "".to_string(),
            source: "chara_card".to_string(),
            observed_at: t2,
        },
        t2,
        300,
    )
    .unwrap();
    let older = prepare_character_identity_upsert(
        &crate::player::UploadableCharacterIdentity {
            content_id: 5005,
            name: "Older".to_string(),
            home_world: 73,
            world_name: "".to_string(),
            source: "chara_card".to_string(),
            observed_at: t1,
        },
        t2,
        300,
    )
    .unwrap();

    let deduped = dedupe_prepared_identities(vec![newer, older]);
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].name, "Newer");

    let players = dedupe_prepared_player_upserts(vec![
        PreparedPlayerUpsert {
            content_id: 1,
            name: "One".to_string(),
            home_world: 73,
            current_world: 0,
            account_id: "99".to_string(),
        },
        PreparedPlayerUpsert {
            content_id: 1,
            name: "One".to_string(),
            home_world: 73,
            current_world: 74,
            account_id: "-1".to_string(),
        },
    ]);
    assert_eq!(players.len(), 1);
    assert_eq!(players[0].account_id, "99");
    assert_eq!(players[0].current_world, 74);
}

#[test]
fn batch_update_and_delete_commands_and_responses_parse_counts_and_write_errors() {
    assert_eq!(MONGO_BATCH_CHUNK_SIZE, 100);

    let item = BatchUpdateItem::upsert_pipeline(
        doc! { "content_id": 42_i64 },
        vec![doc! { "$set": { "name": "Test" } }],
    );
    let cmd = build_batch_update_command("players", &[&item], None);
    assert_eq!(cmd.get_str("update"), Ok("players"));
    assert_eq!(cmd.get_bool("ordered"), Ok(false));
    let updates = cmd.get_array("updates").unwrap();
    assert_eq!(updates.len(), 1);
    let entry = updates[0].as_document().unwrap();
    assert_eq!(entry.get_bool("upsert"), Ok(true));
    assert!(matches!(entry.get("u"), Some(Bson::Array(_))));

    let parsed_update = parse_batch_update_response(&doc! {
        "ok": 1.0,
        "n": 3_i32,
        "nModified": 1_i32,
        "upserted": [{ "index": 0_i32, "_id": 1_i32 }],
        "writeErrors": [
            { "index": 2_i32, "code": 11000_i32, "errmsg": "duplicate key" }
        ]
    })
    .unwrap();
    assert_eq!(parsed_update.modified_count, 1);
    assert_eq!(parsed_update.upserted_count, 1);
    assert_eq!(
        parsed_update.write_errors,
        vec![(2, 11000, "duplicate key".to_string())]
    );

    let del_cmd =
        build_batch_delete_command("fflogs_job_failure_state", &[doc! { "content_id": 42_i64 }], 0, None);
    assert_eq!(del_cmd.get_str("delete"), Ok("fflogs_job_failure_state"));
    assert_eq!(del_cmd.get_bool("ordered"), Ok(false));
    let deletes = del_cmd.get_array("deletes").unwrap();
    assert_eq!(deletes[0].as_document().unwrap().get_i32("limit"), Ok(0));

    let parsed_del = parse_batch_delete_response(&doc! { "ok": 1_i32, "n": 2_i64 }).unwrap();
    assert_eq!(parsed_del.deleted_count, 2);

    assert!(parse_batch_update_response(&doc! { "ok": 0_i32, "errmsg": "boom" }).is_err());
}

#[test]
fn build_zone_cache_batch_item_targets_content_id() {
    let now = Utc::now();
    let item = build_zone_cache_batch_update_item(&ZoneCacheUpsertItem {
        content_id: 12345,
        zone_key: "73:101".to_string(),
        zone_cache: ZoneCache {
            fetched_at: now,
            fflogs_fetched_at: Some(now),
            tomestone_fetched_at: None,
            estimated: false,
            matched_server: None,
            source_kind: None,
            hidden: false,
            fflogs_hidden: false,
            encounters: Default::default(),
        },
        progress_coverage: None,
        fflogs_coverage: None,
        provider: ParseCacheProvider::Fflogs,
    })
    .unwrap();
    assert_eq!(item.filter, doc! { "content_id": 12345_i64 });
    assert!(item.upsert);
}

#[test]
fn identity_duplicate_key_errors_are_retryable() {
    let write_error: mongodb::error::WriteError = serde_json::from_str(
        r#"{"code":11000,"codeName":"DuplicateKey","errmsg":"E11000 duplicate key error"}"#,
    )
    .unwrap();
    let error = mongodb::error::Error::from(mongodb::error::ErrorKind::Write(
        mongodb::error::WriteFailure::WriteError(write_error),
    ));

    assert!(is_duplicate_key_error(&error));
}

#[test]
fn player_lookup_sort_matches_duplicate_cleanup_preference() {
    let options = player_lookup_find_options();

    assert_eq!(
        options.sort.unwrap(),
        doc! {
            "content_id": 1,
            "identity_observed_at": -1,
            "last_seen": -1,
            "seen_count": -1,
            "_id": 1,
        }
    );
}

#[test]
fn player_lookup_dedupe_keeps_first_preferred_content_id() {
    let now = Utc::now();
    let players = vec![
        crate::player::Player {
            content_id: 9009,
            name: "Preferred".to_string(),
            home_world: 74,
            current_world: 74,
            last_seen: now,
            seen_count: 7,
            account_id: "1".to_string(),
        },
        crate::player::Player {
            content_id: 9009,
            name: "Stale".to_string(),
            home_world: 80,
            current_world: 80,
            last_seen: now - chrono::TimeDelta::days(1),
            seen_count: 1,
            account_id: "2".to_string(),
        },
        crate::player::Player {
            content_id: 9010,
            name: "Other".to_string(),
            home_world: 75,
            current_world: 75,
            last_seen: now,
            seen_count: 1,
            account_id: "3".to_string(),
        },
    ];

    let deduped = dedupe_players_by_preferred_order(players);

    assert_eq!(deduped.len(), 2);
    assert_eq!(deduped[0].content_id, 9009);
    assert_eq!(deduped[0].name, "Preferred");
    assert_eq!(deduped[1].content_id, 9010);
}

#[test]
fn zone_cache_lookup_projects_zones_for_partition_free_compatibility() {
    let options = zone_cache_find_options("73:101");
    let projection = options.projection.expect("projection should be set");

    assert_eq!(projection.get_i32("content_id").unwrap(), 1);
    assert_eq!(projection.get_i32("zones").unwrap(), 1);
    assert_eq!(projection.len(), 2);
}
