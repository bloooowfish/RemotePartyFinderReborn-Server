use super::*;
use chrono::{TimeDelta, Utc};
use mongodb::bson::doc;

fn cache(entries: &[(i32, f32, Option<f32>)], source: &str) -> ZoneCache {
    ZoneCache {
        fetched_at: Utc::now(),
        fflogs_fetched_at: None,
        tomestone_fetched_at: None,
        estimated: false,
        matched_server: None,
        source_kind: Some(source.into()),
        hidden: false,
        fflogs_hidden: false,
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
                        tomestone_fetched_at: None,
                    },
                )
            })
            .collect(),
    }
}

#[test]
fn pipeline_uses_atomic_update_and_bounded_progress_age() {
    let value = cache(&[(101, -1.0, Some(35.0))], "fflogs_api");
    let pipeline = zone_cache_update_pipeline(
        42,
        "73:101",
        &value,
        Some(&[101, 102]),
        ParseCacheProvider::Fflogs,
    )
    .unwrap();
    assert_eq!(pipeline.len(), 2);
    let stage = pipeline[1].get_document("$set").unwrap();
    assert_eq!(stage.get_str("updated_at"), Ok("$$NOW"));
    let json = serde_json::to_string(&pipeline).unwrap();
    assert!(json.contains("$setUnion"));
    assert!(json.contains("progress_fetched_at"));
    assert!(json.contains("$$old.v.progress_fetched_at"));
    assert!(json.contains("fflogs_fetched_at"));
    assert!(json.contains("$$old.v.tomestone_fetched_at"));
    assert!(json.contains("coverage"));
}

#[test]
fn tomestone_pipeline_supports_all_tomestone_sources_and_scoped_coverage() {
    for source in ["tomestone_api", "tomestone_web", "tomestone_cdp"] {
        let empty = cache(&[], source);
        let pipeline = zone_cache_update_pipeline(
            42,
            "59:100",
            &empty,
            Some(&[1079]),
            ParseCacheProvider::Tomestone,
        )
        .unwrap();
        let json = serde_json::to_string(&pipeline).unwrap();
        assert!(
            json.contains("\"coverage\":{\"$literal\":[\"1079\"]}"),
            "source={source} json={json}"
        );
        assert!(json.contains("tomestone_fetched_at"), "source={source}");
        assert!(json.contains("fflogs_fetched_at"), "source={source}");
        assert!(json.contains("$$old.v.clear_count"), "source={source}");
        assert!(!json.contains("1073"), "source={source}");
    }
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
        ParseCacheProvider::Fflogs,
    )
    .await
    .unwrap();
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[(102, -1.0, Some(25.0))], "fflogs_api"),
        Some(&[102]),
        ParseCacheProvider::Fflogs,
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
        ParseCacheProvider::Fflogs,
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
        ParseCacheProvider::Fflogs,
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
    // Tomestone-only updates must preserve FFLogs rankings and zone metadata across all tomestone source kinds.
    let mut ranked = cache(&[(102, 75.0, None)], "fflogs_api");
    ranked.estimated = true;
    ranked.matched_server = Some("bahamut".into());
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &ranked,
        Some(&[]),
        ParseCacheProvider::Fflogs,
    )
    .await
    .unwrap();
    for source in ["tomestone_api", "tomestone_web", "tomestone_cdp"] {
        upsert_zone_cache(
            collection.clone(),
            cid,
            zone,
            &cache(&[(101, -1.0, Some(30.0))], source),
            Some(&[101]),
            ParseCacheProvider::Tomestone,
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
        assert!(value.fflogs_fetched_at.is_some());
        assert!(value.tomestone_fetched_at.is_some());
        assert!(value.encounters["101"].tomestone_fetched_at.is_some());
    }
    // Clear evidence removes stale wipe progress even if Tomestone was skipped.
    upsert_zone_cache(
        collection.clone(),
        cid,
        zone,
        &cache(&[(101, 80.0, None)], "fflogs_api"),
        None,
        ParseCacheProvider::Fflogs,
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
        ParseCacheProvider::Tomestone,
    )
    .await
    .unwrap();
    let first = cache(&[(101, -1.0, Some(10.0))], "tomestone_api");
    let second = cache(&[(103, -1.0, Some(20.0))], "tomestone_api");
    let (a, b) = tokio::join!(
        upsert_zone_cache(
            collection.clone(),
            43,
            zone,
            &first,
            Some(&[101]),
            ParseCacheProvider::Tomestone
        ),
        upsert_zone_cache(
            collection.clone(),
            43,
            zone,
            &second,
            Some(&[103]),
            ParseCacheProvider::Tomestone
        )
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
    upsert_zone_cache(
        collection.clone(),
        43,
        zone,
        &second,
        Some(&[103]),
        ParseCacheProvider::Tomestone,
    )
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
    upsert_zone_cache(
        collection.clone(),
        43,
        zone,
        &hidden,
        None,
        ParseCacheProvider::Fflogs,
    )
    .await
    .unwrap();
    upsert_zone_cache(
        collection.clone(),
        43,
        zone,
        &second,
        Some(&[103]),
        ParseCacheProvider::Tomestone,
    )
    .await
    .unwrap();
    assert!(
        get_zone_cache(collection.clone(), 43, zone)
            .await
            .unwrap()
            .unwrap()
            .hidden
    );
    assert_eq!(
        get_zone_cache(collection.clone(), 43, zone)
            .await
            .unwrap()
            .unwrap()
            .encounters["103"]
            .boss_percentage,
        Some(20.0)
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
        ParseCacheProvider::Fflogs,
    )
    .await
    .unwrap();
    let value = get_zone_cache(collection.clone(), 44, zone)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.encounters["101"].boss_percentage, Some(44.0));
    assert_eq!(value.encounters["102"].boss_percentage, Some(22.0));

    // Tomestone-first then FFLogs-second preserves Tomestone timestamp & progress; empty covered=[1079] does not touch 1073.
    let ult_zone = "59:100";
    upsert_zone_cache(
        collection.clone(),
        45,
        ult_zone,
        &cache(&[(1073, -1.0, Some(15.0))], "tomestone_web"),
        Some(&[1073]),
        ParseCacheProvider::Tomestone,
    )
    .await
    .unwrap();
    upsert_zone_cache(
        collection.clone(),
        45,
        ult_zone,
        &cache(&[], "tomestone_cdp"),
        Some(&[1079]),
        ParseCacheProvider::Tomestone,
    )
    .await
    .unwrap();
    upsert_zone_cache(
        collection.clone(),
        45,
        ult_zone,
        &cache(&[(1073, -1.0, None)], "fflogs_api"),
        None,
        ParseCacheProvider::Fflogs,
    )
    .await
    .unwrap();
    let ult_value = get_zone_cache(collection.clone(), 45, ult_zone)
        .await
        .unwrap()
        .unwrap();
    assert!(ult_value.fflogs_fetched_at.is_some());
    assert!(ult_value.tomestone_fetched_at.is_some());
    assert!(ult_value.encounters["1079"].tomestone_fetched_at.is_some());
    assert_eq!(ult_value.encounters["1073"].boss_percentage, Some(15.0));
    assert!(ult_value.encounters["1073"].tomestone_fetched_at.is_some());
    db.drop(None).await.unwrap();
}

#[test]
fn resolve_fflogs_rank_coverage_prefers_non_empty_queried_and_falls_back_to_union() {
    assert_eq!(
        resolve_fflogs_rank_coverage(Some(&[102, 102, 103]), [101], [104], Some(105), Some(106)),
        vec![102, 103]
    );

    assert_eq!(
        resolve_fflogs_rank_coverage(Some(&[]), [101], [102, 101], Some(104), Some(105)),
        vec![101, 102, 104, 105]
    );

    assert_eq!(
        resolve_fflogs_rank_coverage(None, [1073], [], Some(1073), None),
        vec![1073]
    );
}

#[test]
fn fflogs_pipeline_scopes_rank_to_queried_preserves_clear_count_and_does_not_wipe_progress_when_hidden() {
    let mut value = cache(&[(102, 85.0, None)], "fflogs_api");
    value.hidden = true;
    value.fflogs_hidden = true;

    let pipeline = zone_cache_update_pipeline_with_fflogs_coverage(
        42,
        "73:101",
        &value,
        None,
        Some(&[102]),
        ParseCacheProvider::Fflogs,
    )
    .unwrap();
    let json = serde_json::to_string(&pipeline).unwrap();

    assert!(json.contains("\"rankCoverage\":{\"$literal\":[\"102\"]}"));
    assert!(json.contains("\"rankQueried\":{\"$in\":[\"$$key\",\"$$rankCoverage\"]}"));
    assert!(json.contains("\"clear_count\":\"$$old.v.clear_count\"") || json.contains("$$old.v.clear_count"));
    assert!(json.contains("\"fflogs_hidden\":true"));

    // Verify boss_percentage/phase_progress wipe conditions do not include `true` (effective_hidden).
    let set_doc = pipeline[1].get_document("$set").unwrap();
    let zone_expr = set_doc.get_document("zones.73:101").unwrap();
    let merge_arr = zone_expr.get_array("$mergeObjects").unwrap();
    let encounters_expr = merge_arr[1]
        .as_document()
        .unwrap()
        .get_document("encounters")
        .unwrap();
    let enc_json = serde_json::to_string(encounters_expr).unwrap();
    assert!(!enc_json.contains("\"$or\":[true,"));

    // Non-hidden FFLogs pipeline also preserves clear_count via $ifNull when incoming is null.
    value.hidden = false;
    value.fflogs_hidden = false;
    let visible_pipeline = zone_cache_update_pipeline_with_fflogs_coverage(
        42,
        "73:101",
        &value,
        None,
        Some(&[102]),
        ParseCacheProvider::Fflogs,
    )
    .unwrap();
    let visible_json = serde_json::to_string(&visible_pipeline).unwrap();
    assert!(visible_json.contains("[\"$$new.v.clear_count\",\"$$old.v.clear_count\"]"));
}

#[test]
fn tomestone_pipeline_does_not_block_clears_or_wipe_progress_due_to_fflogs_hidden() {
    let empty = cache(&[], "tomestone_api");
    let pipeline = zone_cache_update_pipeline_with_fflogs_coverage(
        42,
        "73:101",
        &empty,
        Some(&[101]),
        None,
        ParseCacheProvider::Tomestone,
    )
    .unwrap();
    let json = serde_json::to_string(&pipeline).unwrap();
    assert!(json.contains("fflogs_hidden"));
    assert!(!json.contains("\"$not\":[\"$zones.73:101.hidden\"]"));
    assert!(!json.contains("\"$not\":[{\"$ifNull\":[\"$zones.73:101.hidden\",false]}]"));
}
