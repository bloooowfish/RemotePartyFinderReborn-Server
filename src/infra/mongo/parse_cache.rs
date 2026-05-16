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
) -> anyhow::Result<()> {
    collection
        .update_one(
            doc! { "content_id": content_id as i64 },
            zone_cache_update_doc(content_id, zone_key, zone_cache)?,
            UpdateOptions::builder().upsert(true).build(),
        )
        .await?;

    Ok(())
}

fn zone_cache_update_doc(
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
) -> anyhow::Result<Document> {
    let zone_path = format!("zones.{}", zone_key);
    let zone_bson = mongodb::bson::to_bson(zone_cache)?;

    Ok(doc! {
        "$set": { &zone_path: zone_bson },
        "$setOnInsert": { "content_id": content_id as i64 },
        "$currentDate": { "updated_at": true },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn zone_cache_update_refreshes_ttl_anchor() {
        let zone_cache = ZoneCache {
            fetched_at: Utc::now(),
            estimated: false,
            matched_server: None,
            source_kind: Some("fflogs_web".to_string()),
            hidden: false,
            encounters: HashMap::new(),
        };

        let update = zone_cache_update_doc(1234, "72:100", &zone_cache)
            .expect("zone cache update should serialize");
        let current_date = update
            .get_document("$currentDate")
            .expect("parse cache update must refresh the TTL anchor");

        assert_eq!(current_date.get_bool("updated_at"), Ok(true));
    }
}
