use super::*;
use chrono::{TimeDelta, Utc};
use mongodb::bson::doc;

fn cache(entries: &[(i32, f32, Option<f32>)], source: &str) -> ZoneCache {
    ZoneCache {
        fetched_at: Utc::now(),
        estimated: false,
        matched_server: None,
        source_kind: Some(source.into()),
        hidden: false,
        encounters: entries
            .iter()
            .map(|(id, percentile, hp)| {
                (
                    id.to_string(),
                    EncounterParse {
                        percentile: *percentile,
                        job_id: 0,
                        boss_percentage: *hp,
                        clear_count: None,
                        phase_progress: vec![],
                    },
                )
            })
            .collect(),
    }
}

#[test]
fn pipeline_uses_atomic_update_and_bounded_progress_age() {
    let value = cache(&[(101, -1.0, Some(35.0))], "fflogs_api");
    let pipeline = zone_cache_update_pipeline(42, "73:101", &value, Some(&[101, 102])).unwrap();
    assert_eq!(pipeline.len(), 2);
    let stage = pipeline[1].get_document("$set").unwrap();
    assert_eq!(stage.get_str("updated_at"), Ok("$$NOW"));
    let json = serde_json::to_string(&pipeline).unwrap();
    assert!(json.contains("$setUnion"));
    assert!(json.contains("progress_fetched_at"));
    assert!(json.contains("$$old.v.progress_fetched_at"));
    assert!(json.contains("coverage"));
}

// Run only against a disposable database. The configured URI must name a test-only MongoDB.
// This exercises MongoDB itself, including update expression and concurrent merge behavior.
#[tokio::test]
#[ignore = "requires RPF_TEST_MONGO_URI pointing at a test-only MongoDB"]
async fn mongo_atomic_provider_merge_regressions() {
    let uri = std::env::var("RPF_TEST_MONGO_URI").expect("set test MongoDB URI");
    let client = mongodb::Client::with_uri_str(uri).await.unwrap();
    let db = client.database(&format!("rpf_merge_test_{}", uuid::Uuid::new_v4().simple()));
    let collection = db.collection::<ParseCacheDoc>("parse_cache");
    let zone = "73:101";
    let cid = 42;
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[(101, -1.0, Some(35.0))], "fflogs_api"),
        Some(&[101]),
    )
    .await
    .unwrap();
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[(102, -1.0, Some(25.0))], "fflogs_api"),
        Some(&[102]),
    )
    .await
    .unwrap();
    let value = get_zone_cache(collection.clone(), cid, zone)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        value.encounters["101"].boss_percentage,
        Some(35.0),
        "M9 survives M10 update"
    );
    assert_eq!(value.encounters["102"].boss_percentage, Some(25.0));
    // New successful empty clears only its scoped target.
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[], "fflogs_api"),
        Some(&[102]),
    )
    .await
    .unwrap();
    let value = get_zone_cache(collection.clone(), cid, zone)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.encounters["101"].boss_percentage, Some(35.0));
    assert_eq!(value.encounters["102"].boss_percentage, None);
    // An omitted/empty coverage from a skipped, rate-limited or older FFLogs collector preserves progress.
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[], "fflogs_api"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        get_zone_cache(collection.clone(), cid, zone)
            .await
            .unwrap()
            .unwrap()
            .encounters["101"]
            .boss_percentage,
        Some(35.0)
    );
    // Tomestone-only updates must preserve FFLogs rankings and zone metadata.
    let mut ranked = cache(&[(102, 75.0, None)], "fflogs_api");
    ranked.estimated = true;
    ranked.matched_server = Some("bahamut".into());
    upsert_zone_cache(collection.clone(), cid, zone, &ranked, Some(&[]))
        .await
        .unwrap();
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[(101, -1.0, Some(30.0))], "tomestone_api"),
        Some(&[101]),
    )
    .await
    .unwrap();
    let value = get_zone_cache(collection.clone(), cid, zone)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.encounters["102"].percentile, 75.0);
    assert!(value.estimated);
    assert_eq!(value.matched_server.as_deref(), Some("bahamut"));
    // Clear evidence removes stale wipe progress even if Tomestone was skipped.
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[(101, 80.0, None)], "fflogs_api"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        get_zone_cache(collection.clone(), cid, zone)
            .await
            .unwrap()
            .unwrap()
            .encounters["101"]
            .boss_percentage,
        None
    );
    // Concurrent partial progress writes cannot erase each other's encounters.
    upsert_zone_cache(
        collection.clone(),
        43,
        zone,
        &cache(&[], "tomestone_api"),
        Some(&[]),
    )
    .await
    .unwrap();
    let first = cache(&[(101, -1.0, Some(10.0))], "tomestone_api");
    let second = cache(&[(103, -1.0, Some(20.0))], "tomestone_api");
    let (a, b) = tokio::join!(
        upsert_zone_cache(collection.clone(), 43, zone, &first, Some(&[101])),
        upsert_zone_cache(collection.clone(), 43, zone, &second, Some(&[103]))
    );
    a.unwrap();
    b.unwrap();
    let value = get_zone_cache(collection.clone(), 43, zone)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.encounters["101"].boss_percentage, Some(10.0));
    assert_eq!(value.encounters["103"].boss_percentage, Some(20.0));
    // Per-encounter age is not extended by unrelated zone writes.
    collection.clone_with_type::<Document>().update_one(doc!{"content_id":43_i64},doc!{"$set":{ "zones.73:101.encounters.101.progress_fetched_at":mongodb::bson::DateTime::from_chrono(Utc::now()-TimeDelta::hours(25))}},None).await.unwrap();
    upsert_zone_cache(collection.clone(), 43, zone, &second, Some(&[103]))
        .await
        .unwrap();
    assert_eq!(
        get_zone_cache(collection.clone(), 43, zone)
            .await
            .unwrap()
            .unwrap()
            .encounters["101"]
            .boss_percentage,
        None
    );
    let mut hidden = cache(&[], "fflogs_api");
    hidden.hidden = true;
    upsert_zone_cache(collection.clone(), 43, zone, &hidden, None)
        .await
        .unwrap();
    upsert_zone_cache(collection.clone(), 43, zone, &second, Some(&[103]))
        .await
        .unwrap();
    assert!(
        get_zone_cache(collection.clone(), 43, zone)
            .await
            .unwrap()
            .unwrap()
            .hidden
    );
    // Existing legacy namespaces migrate without dropping unrequested progress.
    let legacy = cache(&[(101, -1.0, Some(44.0))], "fflogs_api");
    collection
        .clone_with_type::<Document>()
        .insert_one(
            doc! {"content_id": 44_i64, "zones": {"73": mongodb::bson::to_bson(&legacy).unwrap()}},
            None,
        )
        .await
        .unwrap();
    upsert_zone_cache(
        collection.clone(),
        44,
        zone,
        &cache(&[(102, -1.0, Some(22.0))], "fflogs_api"),
        Some(&[102]),
    )
    .await
    .unwrap();
    let value = get_zone_cache(collection.clone(), 44, zone)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.encounters["101"].boss_percentage, Some(44.0));
    assert_eq!(value.encounters["102"].boss_percentage, Some(22.0));
    db.drop(None).await.unwrap();
}
