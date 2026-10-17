use std::collections::HashMap;

use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{FindOneOptions, FindOptions, UpdateOptions},
    Collection,
};

pub use crate::fflogs::cache::{
    is_zone_cache_expired, is_zone_cache_expired_with_hidden_ttl_hours, EncounterParse,
    ParseCacheDoc, PhaseProgress, ZoneCache,
};
pub async fn get_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
) -> anyhow::Result<Option<ZoneCache>> {
    let cache_doc = collection
        .find_one(
            doc! { "content_id": content_id as i64 },
            Some(zone_cache_find_one_options(zone_key)),
        )
        .await?;

    Ok(cache_doc.and_then(|doc| crate::fflogs::get_compatible_zone(&doc.zones, zone_key).cloned()))
}

fn zone_cache_projection() -> Document {
    doc! {
        "content_id": 1,
        "zones": 1,
    }
}

fn zone_cache_find_one_options(zone_key: &str) -> FindOneOptions {
    let _ = zone_key;
    FindOneOptions::builder()
        .projection(zone_cache_projection())
        .build()
}

pub(super) fn zone_cache_find_options(zone_key: &str) -> FindOptions {
    let _ = zone_key;
    FindOptions::builder()
        .projection(zone_cache_projection())
        .build()
}

pub async fn get_zone_caches(
    collection: Collection<ParseCacheDoc>,
    content_ids: &[u64],
    zone_key: &str,
) -> anyhow::Result<HashMap<u64, ZoneCache>> {
    let ids = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(
            doc! { "content_id": { "$in": ids } },
            Some(zone_cache_find_options(zone_key)),
        )
        .await?;

    let docs = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    let mut caches = HashMap::new();
    for doc in docs {
        if let Some(zone_cache) = crate::fflogs::get_compatible_zone(&doc.zones, zone_key) {
            caches.insert(doc.content_id as u64, zone_cache.clone());
        }
    }

    Ok(caches)
}

pub async fn get_parse_docs(
    collection: Collection<ParseCacheDoc>,
    content_ids: &[u64],
) -> anyhow::Result<HashMap<u64, ParseCacheDoc>> {
    let ids = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(doc! { "content_id": { "$in": ids } }, None)
        .await?;

    let docs = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    let mut parse_docs = HashMap::new();
    for doc in docs {
        parse_docs.insert(doc.content_id as u64, doc);
    }

    Ok(parse_docs)
}

pub async fn upsert_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
    progress_coverage: Option<&[i32]>,
) -> anyhow::Result<()> {
    collection
        .update_one(
            doc! { "content_id": content_id as i64 },
            zone_cache_update_pipeline(content_id, zone_key, zone_cache, progress_coverage)?,
            UpdateOptions::builder().upsert(true).build(),
        )
        .await?;

    Ok(())
}

/// A single pipeline update reads the previous encounters and applies both provider scopes
/// atomically. FFLogs refreshes a zone; Tomestone refreshes only successfully queried fights.
fn zone_cache_update_pipeline(
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
    progress_coverage: Option<&[i32]>,
) -> anyhow::Result<Vec<Document>> {
    use mongodb::bson::{Bson, DateTime};
    let zone_path = format!("zones.{zone_key}");
    let old_path = format!("${zone_path}");
    let old_encounters = format!("${zone_path}.encounters");
    let old_fetched = format!("${zone_path}.fetched_at");
    let progress_only = zone_cache.source_kind.as_deref() == Some("tomestone_api");
    let mut metadata = mongodb::bson::to_document(zone_cache)?;
    metadata.remove("encounters");
    let incoming = mongodb::bson::to_bson(&zone_cache.encounters)?;
    let mut covered = progress_coverage
        .unwrap_or_default()
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>();
    // Older collectors have no coverage field; positive values remain safe to merge.
    for (key, value) in &zone_cache.encounters {
        if (value.boss_percentage.is_some() || !value.phase_progress.is_empty())
            && !covered.contains(key)
        {
            covered.push(key.clone());
        }
    }
    let covered = covered.into_iter().map(Bson::String).collect::<Vec<_>>();
    let now = DateTime::from_chrono(zone_cache.fetched_at);
    let cutoff = DateTime::from_chrono(zone_cache.fetched_at - chrono::TimeDelta::hours(24));
    let empty = doc! { "percentile": -1.0, "job_id": 0, "clear_count": Bson::Null };
    let rank = if progress_only { "$$old.v" } else { "$$new.v" };
    let base_metadata: Bson = if progress_only {
        doc! { "$ifNull": [ &old_path, { "$literal": &metadata } ] }.into()
    } else {
        doc! { "$literal": &metadata }.into()
    };
    let effective_hidden: Bson = if progress_only {
        doc! { "$ifNull": [format!("${zone_path}.hidden"), zone_cache.hidden] }.into()
    } else {
        zone_cache.hidden.into()
    };
    let encounter_value = doc! { "k": "$$key", "v": { "$mergeObjects": [
        { "$literal": &empty }, "$$rank",
        { "boss_percentage": { "$cond": [
            { "$or": [effective_hidden.clone(), { "$gt": ["$$rank.percentile", 0] }, { "$gt": [{ "$ifNull": ["$$rank.clear_count", 0] }, 0] }, { "$lt": ["$$progressTime", cutoff] }] },
            Bson::Null, { "$ifNull": ["$$progress.boss_percentage", Bson::Null] }
        ] },
          "phase_progress": { "$cond": [
            { "$or": [effective_hidden.clone(), { "$gt": ["$$rank.percentile", 0] }, { "$gt": [{ "$ifNull": ["$$rank.clear_count", 0] }, 0] }, { "$lt": ["$$progressTime", cutoff] }] },
            [], { "$ifNull": ["$$progress.phase_progress", []] }
          ] },
          "progress_fetched_at": { "$ifNull": ["$$progressTime", now] },
        }
    ] } };
    let encounters = doc! {
        "$let": {
            "vars": {
                "previous": { "$objectToArray": { "$ifNull": [&old_encounters, {}] } },
                "incoming": { "$objectToArray": { "$literal": incoming } },
                "coverage": { "$literal": covered },
            },
            "in": { "$arrayToObject": { "$map": {
                "input": { "$setUnion": ["$$previous.k", "$$incoming.k", "$$coverage"] },
                "as": "key",
                "in": { "$let": {
                    "vars": {
                        "old": { "$arrayElemAt": [{ "$filter": { "input": "$$previous", "as": "e", "cond": { "$eq": ["$$e.k", "$$key"] } } }, 0] },
                        "new": { "$arrayElemAt": [{ "$filter": { "input": "$$incoming", "as": "e", "cond": { "$eq": ["$$e.k", "$$key"] } } }, 0] },
                        "queried": { "$in": ["$$key", "$$coverage"] },
                    },
                    "in": { "$let": {
                        "vars": {
                            "rank": { "$ifNull": [rank, { "$literal": &empty }] },
                            "progress": { "$cond": ["$$queried", "$$new.v", "$$old.v"] },
                            // An unrelated zone update must not refresh a retained progress value.
                            "progressTime": { "$cond": ["$$queried", now, { "$ifNull": ["$$old.v.progress_fetched_at", &old_fetched] }] },
                        },
                        "in": encounter_value,

                    } }
                } }
            } } }
        }
    };
    // Match get_compatible_zone's existing legacy-key fallback before updating.
    // Both stages execute atomically within the same document update.
    let legacy_prefix = format!("{zone_key}:");
    let legacy_zone_path = format!("$zones.{}", zone_key.split(':').next().unwrap_or(zone_key));
    let legacy_zone = doc! { "$let": {
        "vars": { "legacy": { "$filter": {
            "input": { "$objectToArray": { "$ifNull": ["$zones", {}] } },
            "as": "zone",
            "cond": { "$eq": [{ "$indexOfBytes": ["$$zone.k", legacy_prefix] }, 0] },
        } } },
        "in": { "$ifNull": [{ "$arrayElemAt": ["$$legacy.v", 0] }, legacy_zone_path] },
    } };
    Ok(vec![
        doc! { "$set": { &zone_path: { "$ifNull": [&old_path, legacy_zone] } } },
        doc! { "$set": {
            "content_id": content_id as i64,
            &zone_path: { "$mergeObjects": [base_metadata, { "encounters": encounters }] },
            "updated_at": "$$NOW",
        } },
    ])
}

#[cfg(test)]
#[path = "parse_cache_merge_tests.rs"]
mod merge_tests;
