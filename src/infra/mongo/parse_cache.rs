use std::collections::{HashMap, HashSet};

use futures_util::TryStreamExt;
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

    let docs = cursor.try_collect::<Vec<ParseCacheDoc>>().await?;

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

    let docs = cursor.try_collect::<Vec<ParseCacheDoc>>().await?;

    let mut parse_docs = HashMap::new();
    for doc in docs {
        parse_docs.insert(doc.content_id as u64, doc);
    }

    Ok(parse_docs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseCacheProvider {
    Fflogs,
    Tomestone,
}

pub fn resolve_fflogs_rank_coverage(
    fflogs_queried_encounters: Option<&[i32]>,
    encounters_keys: impl IntoIterator<Item = i32>,
    clear_counts_keys: impl IntoIterator<Item = i32>,
    encounter_id: Option<u32>,
    secondary_encounter_id: Option<u32>,
) -> Vec<i32> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    if let Some(queried) = fflogs_queried_encounters.filter(|ids| !ids.is_empty()) {
        for &id in queried {
            if seen.insert(id) {
                out.push(id);
            }
        }
        return out;
    }

    for id in encounters_keys
        .into_iter()
        .chain(clear_counts_keys)
        .chain(encounter_id.filter(|&id| id > 0).map(|id| id as i32))
        .chain(secondary_encounter_id.filter(|&id| id > 0).map(|id| id as i32))
    {
        if seen.insert(id) {
            out.push(id);
        }
    }

    out
}

pub async fn upsert_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
    progress_coverage: Option<&[i32]>,
    provider: ParseCacheProvider,
) -> anyhow::Result<()> {
    upsert_zone_cache_with_fflogs_coverage(
        collection,
        content_id,
        zone_key,
        zone_cache,
        progress_coverage,
        None,
        provider,
    )
    .await
}

#[derive(Debug, Clone)]
pub struct ZoneCacheUpsertItem {
    pub content_id: u64,
    pub zone_key: String,
    pub zone_cache: ZoneCache,
    pub progress_coverage: Option<Vec<i32>>,
    pub fflogs_coverage: Option<Vec<i32>>,
    pub provider: ParseCacheProvider,
}

pub(crate) fn build_zone_cache_batch_update_item(
    item: &ZoneCacheUpsertItem,
) -> anyhow::Result<super::BatchUpdateItem> {
    let pipeline = zone_cache_update_pipeline_with_fflogs_coverage(
        item.content_id,
        &item.zone_key,
        &item.zone_cache,
        item.progress_coverage.as_deref(),
        item.fflogs_coverage.as_deref(),
        item.provider,
    )?;
    Ok(super::BatchUpdateItem::upsert_pipeline(
        doc! { "content_id": item.content_id as i64 },
        pipeline,
    ))
}

pub async fn upsert_zone_caches_batch(
    collection: Collection<ParseCacheDoc>,
    items: &[ZoneCacheUpsertItem],
) -> anyhow::Result<super::BatchUpdateSummary> {
    let batch_items = items
        .iter()
        .map(build_zone_cache_batch_update_item)
        .collect::<anyhow::Result<Vec<_>>>()?;
    super::run_batched_updates(&collection, &batch_items, true).await
}

pub async fn upsert_zone_cache_with_fflogs_coverage(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
    progress_coverage: Option<&[i32]>,
    fflogs_coverage: Option<&[i32]>,
    provider: ParseCacheProvider,
) -> anyhow::Result<()> {
    collection
        .update_one(
            doc! { "content_id": content_id as i64 },
            zone_cache_update_pipeline_with_fflogs_coverage(
                content_id,
                zone_key,
                zone_cache,
                progress_coverage,
                fflogs_coverage,
                provider,
            )?,
            UpdateOptions::builder().upsert(true).build(),
        )
        .await?;

    Ok(())
}

/// A single pipeline update reads the previous encounters and applies both provider scopes
/// atomically. FFLogs refreshes scoped encounters; Tomestone refreshes only successfully queried fights.
#[cfg_attr(not(test), allow(dead_code))]
fn zone_cache_update_pipeline(
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
    progress_coverage: Option<&[i32]>,
    provider: ParseCacheProvider,
) -> anyhow::Result<Vec<Document>> {
    zone_cache_update_pipeline_with_fflogs_coverage(
        content_id,
        zone_key,
        zone_cache,
        progress_coverage,
        None,
        provider,
    )
}

fn zone_cache_update_pipeline_with_fflogs_coverage(
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
    progress_coverage: Option<&[i32]>,
    fflogs_coverage: Option<&[i32]>,
    provider: ParseCacheProvider,
) -> anyhow::Result<Vec<Document>> {
    use mongodb::bson::{Bson, DateTime};
    let zone_path = format!("zones.{zone_key}");
    let old_path = format!("${zone_path}");
    let old_encounters = format!("${zone_path}.encounters");
    let old_fetched = format!("${zone_path}.fetched_at");
    let old_fflogs_fetched = format!("${zone_path}.fflogs_fetched_at");
    let old_tomestone_fetched = format!("${zone_path}.tomestone_fetched_at");
    let old_hidden = format!("${zone_path}.hidden");
    let old_fflogs_hidden = format!("${zone_path}.fflogs_hidden");
    let old_estimated = format!("${zone_path}.estimated");
    let old_matched_server = format!("${zone_path}.matched_server");
    let old_source_kind = format!("${zone_path}.source_kind");

    let mut metadata = mongodb::bson::to_document(zone_cache)?;
    metadata.remove("encounters");
    metadata.remove("fflogs_fetched_at");
    metadata.remove("tomestone_fetched_at");

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

    let rank_covered: Vec<Bson> = match provider {
        ParseCacheProvider::Fflogs => {
            let mut keys = Vec::new();
            let mut seen = HashSet::new();
            if let Some(explicit) = fflogs_coverage {
                for id in explicit {
                    let key = id.to_string();
                    if seen.insert(key.clone()) {
                        keys.push(Bson::String(key));
                    }
                }
            } else {
                for key in zone_cache.encounters.keys() {
                    if seen.insert(key.clone()) {
                        keys.push(Bson::String(key.clone()));
                    }
                }
            }
            keys
        }
        ParseCacheProvider::Tomestone => Vec::new(),
    };

    let now = DateTime::from_chrono(zone_cache.fetched_at);
    let cutoff = DateTime::from_chrono(zone_cache.fetched_at - chrono::TimeDelta::hours(24));
    let empty = doc! { "percentile": -1.0, "job_id": 0, "clear_count": Bson::Null };
    let fflogs_is_hidden = zone_cache.is_fflogs_hidden();

    let rank: Bson = match provider {
        ParseCacheProvider::Fflogs => {
            let new_rank = if fflogs_is_hidden {
                doc! {
                    "$mergeObjects": [
                        { "$literal": &empty },
                        {
                            "clear_count": "$$old.v.clear_count",
                        }
                    ]
                }
            } else {
                doc! {
                    "$mergeObjects": [
                        { "$literal": &empty },
                        { "$ifNull": ["$$new.v", { "$literal": &empty }] },
                        {
                            "clear_count": {
                                "$ifNull": ["$$new.v.clear_count", "$$old.v.clear_count"]
                            }
                        }
                    ]
                }
            };
            doc! {
                "$cond": [
                    "$$rankQueried",
                    new_rank,
                    { "$ifNull": ["$$old.v", { "$literal": &empty }] }
                ]
            }
            .into()
        }
        ParseCacheProvider::Tomestone => {
            let old_has_clear = doc! {
                "$or": [
                    { "$gt": [{ "$ifNull": ["$$old.v.percentile", -1.0] }, 0] },
                    { "$gt": [{ "$ifNull": ["$$old.v.clear_count", 0] }, 0] },
                ]
            };
            let new_has_clear = doc! {
                "$or": [
                    { "$gt": [{ "$ifNull": ["$$new.v.percentile", -1.0] }, 0] },
                    { "$gt": [{ "$ifNull": ["$$new.v.clear_count", 0] }, 0] },
                ]
            };
            doc! {
                "$cond": [
                    old_has_clear,
                    "$$old.v",
                    {
                        "$cond": [
                            new_has_clear,
                            "$$new.v",
                            { "$ifNull": ["$$old.v", { "$literal": &empty }] }
                        ]
                    }
                ]
            }
            .into()
        }
    };

    let base_metadata: Bson = match provider {
        ParseCacheProvider::Fflogs => doc! {
            "$mergeObjects": [
                { "$literal": &metadata },
                {
                    "fflogs_hidden": fflogs_is_hidden,
                    "fetched_at": now,
                    "fflogs_fetched_at": now,
                    "tomestone_fetched_at": { "$ifNull": [&old_tomestone_fetched, Bson::Null] },
                }
            ]
        }
        .into(),
        ParseCacheProvider::Tomestone => {
            let mut tomestone_init = metadata.clone();
            tomestone_init.insert("hidden", false);
            tomestone_init.insert("fflogs_hidden", false);
            tomestone_init.insert("fetched_at", now);
            tomestone_init.insert("fflogs_fetched_at", Bson::Null);
            tomestone_init.insert("tomestone_fetched_at", now);

            let incoming_matched_server = zone_cache
                .matched_server
                .as_deref()
                .map(Bson::from)
                .unwrap_or(Bson::Null);
            let incoming_source_kind = zone_cache
                .source_kind
                .as_deref()
                .map(Bson::from)
                .unwrap_or(Bson::Null);
            let old_source_is_tomestone = doc! {
                "$eq": [
                    { "$indexOfBytes": [{ "$ifNull": [&old_source_kind, ""] }, "tomestone"] },
                    0
                ]
            };
            let preserved_fflogs_fetched_at = doc! {
                "$ifNull": [
                    &old_fflogs_fetched,
                    { "$cond": [old_source_is_tomestone.clone(), Bson::Null, &old_fetched] }
                ]
            };
            let preserved_source_kind = doc! {
                "$cond": [
                    { "$and": [
                        { "$ne": [{ "$ifNull": [&old_source_kind, Bson::Null] }, Bson::Null] },
                        { "$not": [old_source_is_tomestone] },
                    ] },
                    &old_source_kind,
                    { "$ifNull": [incoming_source_kind, &old_source_kind] }
                ]
            };

            doc! {
                "$cond": [
                    { "$ne": [{ "$ifNull": [&old_path, Bson::Null] }, Bson::Null] },
                    {
                        "$mergeObjects": [
                            &old_path,
                            {
                                "fetched_at": now,
                                "tomestone_fetched_at": now,
                                "fflogs_fetched_at": preserved_fflogs_fetched_at,
                                "hidden": { "$ifNull": [&old_hidden, false] },
                                "fflogs_hidden": { "$ifNull": [&old_fflogs_hidden, false] },
                                "estimated": { "$ifNull": [&old_estimated, zone_cache.estimated] },
                                "matched_server": { "$ifNull": [&old_matched_server, incoming_matched_server] },
                                "source_kind": preserved_source_kind,
                            }
                        ]
                    },
                    { "$literal": &tomestone_init }
                ]
            }
            .into()
        }
    };

    let encounter_tomestone_fetched_at = match provider {
        ParseCacheProvider::Fflogs => {
            doc! { "$ifNull": ["$$old.v.tomestone_fetched_at", Bson::Null] }
        }
        ParseCacheProvider::Tomestone => doc! {
            "$cond": [
                "$$queried",
                now,
                { "$ifNull": ["$$old.v.tomestone_fetched_at", Bson::Null] }
            ]
        },
    };

    let encounter_value = doc! { "k": "$$key", "v": { "$mergeObjects": [
        { "$literal": &empty }, "$$rank",
        { "boss_percentage": { "$cond": [
            { "$or": [{ "$gt": ["$$rank.percentile", 0] }, { "$gt": [{ "$ifNull": ["$$rank.clear_count", 0] }, 0] }, { "$lt": ["$$progressTime", cutoff] }] },
            Bson::Null, { "$ifNull": ["$$progress.boss_percentage", Bson::Null] }
        ] },
          "phase_progress": { "$cond": [
            { "$or": [{ "$gt": ["$$rank.percentile", 0] }, { "$gt": [{ "$ifNull": ["$$rank.clear_count", 0] }, 0] }, { "$lt": ["$$progressTime", cutoff] }] },
            [], { "$ifNull": ["$$progress.phase_progress", []] }
          ] },
          "progress_fetched_at": { "$ifNull": ["$$progressTime", now] },
          "tomestone_fetched_at": encounter_tomestone_fetched_at,
        }
    ] } };
    let mut encounter_vars = doc! {
        "previous": { "$objectToArray": { "$ifNull": [&old_encounters, {}] } },
        "incoming": { "$objectToArray": { "$literal": incoming } },
        "coverage": { "$literal": covered },
    };
    let mut union_inputs = vec![
        Bson::from("$$previous.k"),
        Bson::from("$$incoming.k"),
        Bson::from("$$coverage"),
    ];
    let mut key_vars = doc! {
        "old": { "$arrayElemAt": [{ "$filter": { "input": "$$previous", "as": "e", "cond": { "$eq": ["$$e.k", "$$key"] } } }, 0] },
        "new": { "$arrayElemAt": [{ "$filter": { "input": "$$incoming", "as": "e", "cond": { "$eq": ["$$e.k", "$$key"] } } }, 0] },
        "queried": { "$in": ["$$key", "$$coverage"] },
    };
    if provider == ParseCacheProvider::Fflogs {
        encounter_vars.insert("rankCoverage", doc! { "$literal": rank_covered });
        union_inputs.push(Bson::from("$$rankCoverage"));
        key_vars.insert("rankQueried", doc! { "$in": ["$$key", "$$rankCoverage"] });
    }
    let encounters = doc! {
        "$let": {
            "vars": encounter_vars,
            "in": { "$arrayToObject": { "$map": {
                "input": { "$setUnion": union_inputs },
                "as": "key",
                "in": { "$let": {
                    "vars": key_vars,
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
