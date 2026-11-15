use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;
use warp::http::{HeaderMap, HeaderValue, StatusCode};

use super::{
    assemble_tomestone_jobs, effective_tomestone_progress_coverage, is_tomestone_encounter_due,
    normalized_tomestone_lease_key, prepare_tomestone_listing_batches,
    prepare_tomestone_listings_needing_fetch, release_valid_abandoned_leases,
    resolve_tomestone_player_server_and_region, should_record_tomestone_failure_state,
    tomestone_failure_state_becomes_final, tomestone_failure_state_id,
    validate_abandoned_tomestone_leases, AbandonTomestoneLease, TomestoneJob, TomestoneResult,
    DEFAULT_TOMESTONE_PROCESSOR_KIND, TOMESTONE_JOBS_LIMIT_HEADER, TOMESTONE_JOB_MODE_HEADER,
    TOMESTONE_PROCESSOR_KIND_HEADER, TOMESTONE_WORKER_REGION_HEADER,
};
use crate::listing::PartyFinderListing;
use crate::listing_container::QueriedListing;
use crate::mongo::{EncounterParse, ParseCacheDoc, ZoneCache};
use crate::player::Player;
use crate::web::ingest_guard::{issue_capability_token, CapabilityClaims, CapabilityScope};
use crate::web::parse_ingest_common::{
    parse_jobs_limit, parse_processor_kind, parse_worker_region, snapshot_active_leases,
    WorkerRegion, FETCH_FAILED_REASON, RATE_LIMIT_DEFERRED_REASON,
    TOMESTONE_CHALLENGE_COOLDOWN_REASON, TOMESTONE_NO_DATA_REASON, TOMESTONE_RATE_LIMITED_REASON,
};
use crate::web::routes::router;
use crate::web::test_support::{
    sample_listing, signed_ingest_headers, state_with_signature_required,
};
use crate::web::{TomestoneLeaseEntry, TOMESTONE_LEASE_TTL_MINUTES};

fn sample_encounter(
    percentile: f32,
    clear_count: Option<u32>,
    tomestone_fetched_at: Option<DateTime<Utc>>,
) -> EncounterParse {
    EncounterParse {
        percentile,
        job_id: 37,
        boss_percentage: None,
        clear_count,
        phase_progress: Vec::new(),
        tomestone_fetched_at,
    }
}

fn sample_zone_cache(
    fetched_at: DateTime<Utc>,
    fflogs_fetched_at: Option<DateTime<Utc>>,
    tomestone_fetched_at: Option<DateTime<Utc>>,
) -> ZoneCache {
    ZoneCache {
        fetched_at,
        fflogs_fetched_at,
        tomestone_fetched_at,
        encounters: HashMap::new(),
        hidden: false,
        estimated: false,
        matched_server: None,
        source_kind: None,
    }
}

fn sample_player(content_id: u64, name: &str, home_world: u16) -> Player {
    Player {
        content_id,
        name: name.to_string(),
        home_world,
        current_world: home_world,
        last_seen: Utc::now(),
        seen_count: 1,
        account_id: "-1".to_string(),
    }
}

fn sample_queried_listing(duty: u16, anchor_world: u16, members: Vec<i64>) -> QueriedListing {
    let mut listing: PartyFinderListing = sample_listing();
    listing.duty = duty;
    listing.created_world = anchor_world;
    listing.home_world = anchor_world;
    listing.current_world = anchor_world;
    listing.member_content_ids = members;
    let now = Utc::now();
    QueriedListing {
        created_at: now,
        updated_at: now,
        updated_minute: now,
        time_left: 1500.0,
        listing,
    }
}

fn add_capability_header(
    state: &crate::web::State,
    req: warp::test::RequestBuilder,
    client_id: &str,
    scope: CapabilityScope,
) -> warp::test::RequestBuilder {
    let now = Utc::now().timestamp();
    let claims = CapabilityClaims::new(client_id.to_string(), scope, None, now, now + 300);
    let token = issue_capability_token(&state.ingest_security.capability_secret, &claims).unwrap();
    req.header("x-rpf-capability", token)
}

fn with_signed_headers(
    mut req: warp::test::RequestBuilder,
    headers: &HeaderMap,
) -> warp::test::RequestBuilder {
    for (name, value) in headers.iter() {
        req = req.header(name.as_str(), value.to_str().unwrap());
    }
    req
}

#[test]
fn tomestone_headers_parse_processor_kind_and_worker_region() {
    let mut headers = HeaderMap::new();
    assert_eq!(
        parse_processor_kind(
            &headers,
            TOMESTONE_PROCESSOR_KIND_HEADER,
            DEFAULT_TOMESTONE_PROCESSOR_KIND
        ),
        DEFAULT_TOMESTONE_PROCESSOR_KIND
    );
    assert_eq!(
        parse_worker_region(&headers, TOMESTONE_WORKER_REGION_HEADER),
        None
    );

    headers.insert(
        TOMESTONE_PROCESSOR_KIND_HEADER,
        HeaderValue::from_static(" custom_tomestone_worker "),
    );
    headers.insert(
        TOMESTONE_WORKER_REGION_HEADER,
        HeaderValue::from_static(" oce "),
    );

    assert_eq!(
        parse_processor_kind(
            &headers,
            TOMESTONE_PROCESSOR_KIND_HEADER,
            DEFAULT_TOMESTONE_PROCESSOR_KIND
        ),
        "custom_tomestone_worker"
    );
    assert_eq!(
        parse_worker_region(&headers, TOMESTONE_WORKER_REGION_HEADER),
        Some(WorkerRegion::Oc)
    );

    headers.insert(
        TOMESTONE_WORKER_REGION_HEADER,
        HeaderValue::from_static("global"),
    );
    assert_eq!(
        parse_worker_region(&headers, TOMESTONE_WORKER_REGION_HEADER),
        None
    );
    assert_eq!(TOMESTONE_JOB_MODE_HEADER, "x-rpf-tomestone-job-mode");
}

#[test]
fn tomestone_jobs_limit_header_parses_clamps_and_falls_back_to_max_limit() {
    assert_eq!(TOMESTONE_JOBS_LIMIT_HEADER, "x-rpf-tomestone-jobs-limit");
    let max_limit = 20;
    let mut headers = HeaderMap::new();
    assert_eq!(
        parse_jobs_limit(&headers, TOMESTONE_JOBS_LIMIT_HEADER, max_limit),
        max_limit
    );

    headers.insert(TOMESTONE_JOBS_LIMIT_HEADER, HeaderValue::from_static("0"));
    assert_eq!(
        parse_jobs_limit(&headers, TOMESTONE_JOBS_LIMIT_HEADER, max_limit),
        max_limit
    );

    headers.insert(
        TOMESTONE_JOBS_LIMIT_HEADER,
        HeaderValue::from_static("invalid"),
    );
    assert_eq!(
        parse_jobs_limit(&headers, TOMESTONE_JOBS_LIMIT_HEADER, max_limit),
        max_limit
    );

    headers.insert(TOMESTONE_JOBS_LIMIT_HEADER, HeaderValue::from_static("8"));
    assert_eq!(
        parse_jobs_limit(&headers, TOMESTONE_JOBS_LIMIT_HEADER, max_limit),
        8
    );

    headers.insert(TOMESTONE_JOBS_LIMIT_HEADER, HeaderValue::from_static("999"));
    assert_eq!(
        parse_jobs_limit(&headers, TOMESTONE_JOBS_LIMIT_HEADER, max_limit),
        max_limit
    );
}

#[test]
fn tomestone_lease_ttl_is_five_minutes_and_expires_after_ttl() {
    assert_eq!(TOMESTONE_LEASE_TTL_MINUTES, 5);
    let lease_ttl = TimeDelta::try_minutes(TOMESTONE_LEASE_TTL_MINUTES).unwrap();
    let now = Utc::now();

    let active_key = normalized_tomestone_lease_key(101, 59, 100, 1073);
    let boundary_key = normalized_tomestone_lease_key(102, 59, 100, 1073);
    let expired_key = normalized_tomestone_lease_key(103, 59, 100, 1073);

    let leases = HashMap::from([
        (
            active_key.clone(),
            TomestoneLeaseEntry {
                leased_at: now - TimeDelta::try_minutes(TOMESTONE_LEASE_TTL_MINUTES - 1).unwrap(),
                client_id: "worker-a".to_string(),
                lease_token: "tok-active".to_string(),
                secondary_encounter_id: None,
            },
        ),
        (
            boundary_key.clone(),
            TomestoneLeaseEntry {
                leased_at: now - TimeDelta::try_minutes(TOMESTONE_LEASE_TTL_MINUTES).unwrap(),
                client_id: "worker-a".to_string(),
                lease_token: "tok-boundary".to_string(),
                secondary_encounter_id: None,
            },
        ),
        (
            expired_key.clone(),
            TomestoneLeaseEntry {
                leased_at: now - TimeDelta::try_minutes(TOMESTONE_LEASE_TTL_MINUTES + 1).unwrap(),
                client_id: "worker-a".to_string(),
                lease_token: "tok-expired".to_string(),
                secondary_encounter_id: None,
            },
        ),
    ]);

    let active = snapshot_active_leases(&leases, lease_ttl, now);
    assert!(active.contains(&active_key));
    assert!(!active.contains(&boundary_key));
    assert!(!active.contains(&expired_key));
}

#[test]
fn tomestone_job_serializes_exact_wire_shape() {
    let job = TomestoneJob {
        content_id: 42,
        name: "Do Dive".to_string(),
        server: "Chocobo".to_string(),
        region: "JP".to_string(),
        candidate_servers: Vec::new(),
        zone_id: 59,
        difficulty_id: 100,
        encounter_id: 1073,
        secondary_encounter_id: None,
        lease_token: "lease-1".to_string(),
    };

    let json = serde_json::to_value(job).expect("TomestoneJob should serialize");
    assert_eq!(json["encounter_id"], 1073);
    assert_eq!(json["candidate_servers"], serde_json::json!([]));
    assert_eq!(json["secondary_encounter_id"], serde_json::Value::Null);
}

#[test]
fn cooldown_and_rate_limit_reasons_do_not_record_tomestone_failure_strikes() {
    for reason in [
        RATE_LIMIT_DEFERRED_REASON,
        TOMESTONE_RATE_LIMITED_REASON,
        TOMESTONE_CHALLENGE_COOLDOWN_REASON,
        "TOMESTONE_CHALLENGE_COOLDOWN",
    ] {
        assert!(!should_record_tomestone_failure_state(reason));
        assert!(!tomestone_failure_state_becomes_final(reason, 100, 1));
    }

    assert!(should_record_tomestone_failure_state(FETCH_FAILED_REASON));
    assert!(tomestone_failure_state_becomes_final(
        FETCH_FAILED_REASON,
        1,
        2
    ));

    assert!(should_record_tomestone_failure_state(TOMESTONE_NO_DATA_REASON));
    assert!(!tomestone_failure_state_becomes_final(
        TOMESTONE_NO_DATA_REASON,
        1,
        2
    ));
    assert!(tomestone_failure_state_becomes_final(
        TOMESTONE_NO_DATA_REASON,
        2,
        2
    ));
}

#[test]
fn tomestone_failure_state_id_scopes_independently_by_encounter_id() {
    let ucob = normalized_tomestone_lease_key(42, 59, 100, 1073);
    let uwu = normalized_tomestone_lease_key(42, 59, 100, 1074);

    assert_ne!(
        tomestone_failure_state_id(&ucob, DEFAULT_TOMESTONE_PROCESSOR_KIND, "fp"),
        tomestone_failure_state_id(&uwu, DEFAULT_TOMESTONE_PROCESSOR_KIND, "fp")
    );
}

#[test]
fn is_tomestone_encounter_due_checks_hidden_clears_and_per_encounter_freshness() {
    let now = Utc::now();
    assert_eq!(
        is_tomestone_encounter_due(None, 1073, None, 60, now),
        (true, None)
    );

    let mut hidden_cache = sample_zone_cache(now, None, Some(now));
    hidden_cache.hidden = true;
    assert_eq!(
        is_tomestone_encounter_due(Some(&hidden_cache), 1073, None, 60, now),
        (false, None)
    );

    // FFLogs-only cache leaves Tomestone due.
    let mut fflogs_only = sample_zone_cache(now, Some(now), None);
    fflogs_only.source_kind = Some("fflogs_api".to_string());
    assert_eq!(
        is_tomestone_encounter_due(Some(&fflogs_only), 1073, None, 60, now),
        (true, None)
    );

    // Dual ultimate 59:100 — 1073 fresh, 1074 still due without cross-contamination.
    let fresh_ts = now - TimeDelta::try_minutes(10).unwrap();
    let stale_ts = now - TimeDelta::try_minutes(75).unwrap();
    let mut dual_ult = sample_zone_cache(fresh_ts, None, Some(fresh_ts));
    dual_ult.encounters.insert(
        "1073".to_string(),
        sample_encounter(-1.0, None, Some(fresh_ts)),
    );
    assert_eq!(
        is_tomestone_encounter_due(Some(&dual_ult), 1073, None, 60, now),
        (false, Some(fresh_ts))
    );
    assert_eq!(
        is_tomestone_encounter_due(Some(&dual_ult), 1074, None, 60, now),
        (true, None)
    );

    // Dual ultimate cleared on 1073 skips 1073 only.
    dual_ult
        .encounters
        .insert("1073".to_string(), sample_encounter(80.0, Some(1), Some(stale_ts)));
    assert_eq!(
        is_tomestone_encounter_due(Some(&dual_ult), 1073, None, 60, now),
        (false, None)
    );
    assert_eq!(
        is_tomestone_encounter_due(Some(&dual_ult), 1074, None, 60, now),
        (true, None)
    );

    // Split boss 73:101 (104/105): clearing 104 does not skip 105; both timestamps required.
    let mut split_cache = sample_zone_cache(fresh_ts, None, Some(fresh_ts));
    split_cache.encounters.insert(
        "104".to_string(),
        sample_encounter(90.0, Some(1), Some(fresh_ts)),
    );
    assert_eq!(
        is_tomestone_encounter_due(Some(&split_cache), 104, Some(105), 60, now),
        (true, None)
    );
    split_cache.encounters.insert(
        "105".to_string(),
        sample_encounter(-1.0, None, Some(stale_ts)),
    );
    assert_eq!(
        is_tomestone_encounter_due(Some(&split_cache), 104, Some(105), 60, now),
        (true, Some(stale_ts))
    );
    split_cache.encounters.insert(
        "105".to_string(),
        sample_encounter(75.0, Some(1), Some(stale_ts)),
    );
    assert_eq!(
        is_tomestone_encounter_due(Some(&split_cache), 104, Some(105), 60, now),
        (false, None)
    );
}

#[test]
fn exact_server_resolution_uses_home_world_or_non_hidden_matched_server() {
    let now = Utc::now();
    let known_world_player = sample_player(1, "Known", 70);
    assert_eq!(
        resolve_tomestone_player_server_and_region(&known_world_player, None, "59:100"),
        Some(("Chocobo".to_string(), "JP".to_string()))
    );

    let zero_world_player = sample_player(2, "Fallback", 0);
    assert_eq!(
        resolve_tomestone_player_server_and_region(&zero_world_player, None, "59:100"),
        None
    );

    let mut current_zone = sample_zone_cache(now, Some(now), None);
    current_zone.matched_server = Some("chocobo".to_string());
    let mut doc = ParseCacheDoc {
        content_id: 2,
        zones: HashMap::from([("59:100".to_string(), current_zone)]),
    };
    assert_eq!(
        resolve_tomestone_player_server_and_region(&zero_world_player, Some(&doc), "59:100"),
        Some(("Chocobo".to_string(), "JP".to_string()))
    );

    // Hidden current zone blocks fallback even if another zone has a matched server.
    doc.zones.get_mut("59:100").unwrap().hidden = true;
    let mut other_zone = sample_zone_cache(now, Some(now), None);
    other_zone.matched_server = Some("Adamantoise".to_string());
    doc.zones.insert("73:101".to_string(), other_zone);
    assert_eq!(
        resolve_tomestone_player_server_and_region(&zero_world_player, Some(&doc), "59:100"),
        None
    );

    // Non-hidden current zone without matched_server falls back to latest non-hidden matched_server.
    let current_without_server = sample_zone_cache(now, Some(now), None);
    doc.zones.insert("59:100".to_string(), current_without_server);
    assert_eq!(
        resolve_tomestone_player_server_and_region(&zero_world_player, Some(&doc), "59:100"),
        Some(("Adamantoise".to_string(), "NA".to_string()))
    );
}

#[test]
fn prepare_and_assemble_dispatches_dual_ultimates_independently_without_candidates() {
    let now = Utc::now();
    let listings = vec![
        sample_queried_listing(280, 70, vec![101, 102]), // UCoB: 59:100 encounter 1073
        sample_queried_listing(539, 70, vec![101, 103]), // UwU:  59:100 encounter 1074
        sample_queried_listing(1075, 73, vec![104]),     // M12S: 73:101 encounter 104 (secondary 105)
    ];

    let active_leases = HashSet::from([normalized_tomestone_lease_key(101, 59, 100, 1073)]);
    let (batches, member_ids) = prepare_tomestone_listing_batches(&listings, &active_leases);
    assert!(member_ids.contains(&101)); // Still needed for 1074!

    let mut parse_docs = HashMap::new();
    let stale_old = now - TimeDelta::try_minutes(120).unwrap();
    let stale_newer = now - TimeDelta::try_minutes(90).unwrap();
    // Player 103 has stale tomestone cache on 1074; 101 has missing cache -> 101 comes before 103 on 1074.
    let mut p103_zone = sample_zone_cache(stale_newer, None, Some(stale_newer));
    p103_zone
        .encounters
        .insert("1074".to_string(), sample_encounter(-1.0, None, Some(stale_newer)));
    parse_docs.insert(
        103,
        ParseCacheDoc {
            content_id: 103,
            zones: HashMap::from([("59:100".to_string(), p103_zone)]),
        },
    );
    let mut p102_zone = sample_zone_cache(stale_old, None, Some(stale_old));
    p102_zone
        .encounters
        .insert("1073".to_string(), sample_encounter(-1.0, None, Some(stale_old)));
    parse_docs.insert(
        102,
        ParseCacheDoc {
            content_id: 102,
            zones: HashMap::from([("59:100".to_string(), p102_zone)]),
        },
    );

    // Final failure on 1073 for 101 doesn't affect 1074 for 101.
    let final_failures = HashSet::from([(101, 59, 100, 1073)]);
    let (prepared, needing_ids) = prepare_tomestone_listings_needing_fetch(
        batches,
        &parse_docs,
        &final_failures,
        &active_leases,
        60,
        now,
    );
    assert!(needing_ids.contains(&101));
    assert!(needing_ids.contains(&102));
    assert!(needing_ids.contains(&103));
    assert!(needing_ids.contains(&104));

    let players = vec![
        sample_player(101, "DualUlt", 70),
        sample_player(102, "UcobPlayer", 70),
        sample_player(103, "UwuPlayer", 70),
        sample_player(104, "NaSplit", 73),
    ];

    let jp_jobs = assemble_tomestone_jobs(
        prepared,
        players,
        &parse_docs,
        10,
        Some(WorkerRegion::Jp),
    );
    assert_eq!(jp_jobs.len(), 3);
    assert!(jp_jobs.iter().all(|job| job.candidate_servers.is_empty()));
    assert!(jp_jobs.iter().any(|job| job.content_id == 102 && job.encounter_id == 1073));
    assert!(jp_jobs.iter().any(|job| job.content_id == 101 && job.encounter_id == 1074));
    assert!(jp_jobs.iter().any(|job| job.content_id == 103 && job.encounter_id == 1074));
    assert!(!jp_jobs.iter().any(|job| job.content_id == 104));

    // Verify order on UwU listing puts missing cache (101) before stale cache (103).
    let uwu_order: Vec<u64> = jp_jobs
        .iter()
        .filter(|job| job.encounter_id == 1074)
        .map(|job| job.content_id)
        .collect();
    assert_eq!(uwu_order, vec![101, 103]);
}

#[test]
fn effective_tomestone_progress_coverage_defaults_only_for_authoritative_empty_and_respects_explicit() {
    let empty_result = TomestoneResult {
        content_id: 1,
        zone_id: 59,
        difficulty_id: 100,
        encounter_id: 1073,
        encounters: HashMap::new(),
        boss_percentages: HashMap::new(),
        clear_counts: HashMap::new(),
        phase_progress: HashMap::new(),
        tomestone_queried_encounters: None,
        is_hidden: false,
        is_estimated: false,
        matched_server: None,
        source_kind: Some("tomestone_api".to_string()),
        lease_token: "tok".to_string(),
    };

    let single = normalized_tomestone_lease_key(1, 59, 100, 1073);
    assert_eq!(
        effective_tomestone_progress_coverage(&single, None, &empty_result),
        vec![1073]
    );

    let explicit_empty = TomestoneResult {
        tomestone_queried_encounters: Some(Vec::new()),
        ..serde_json::from_value::<TomestoneResult>(serde_json::json!({
            "content_id": 1,
            "zone_id": 59,
            "difficulty_id": 100,
            "encounter_id": 1073,
            "lease_token": "tok"
        }))
        .unwrap()
    };
    assert!(effective_tomestone_progress_coverage(&single, None, &explicit_empty).is_empty());

    let explicit_1074 = TomestoneResult {
        tomestone_queried_encounters: Some(vec![1074]),
        ..explicit_empty
    };
    assert_eq!(
        effective_tomestone_progress_coverage(&single, None, &explicit_1074),
        vec![1074]
    );

    // Authoritative empty on split-boss covers both 104 and 105 when queried encounters is None.
    let split = normalized_tomestone_lease_key(1, 73, 101, 104);
    let split_empty: TomestoneResult = serde_json::from_value(serde_json::json!({
        "content_id": 1,
        "zone_id": 73,
        "difficulty_id": 101,
        "encounter_id": 104,
        "lease_token": "tok"
    }))
    .unwrap();
    assert_eq!(
        effective_tomestone_progress_coverage(&split, Some(105), &split_empty),
        vec![104, 105]
    );

    // Split-boss with 104 clear and unsettled 105 never falls back to stamping 105.
    let split_partial_none: TomestoneResult = serde_json::from_value(serde_json::json!({
        "content_id": 1,
        "zone_id": 73,
        "difficulty_id": 101,
        "encounter_id": 104,
        "encounters": { "104": 91.2 },
        "clear_counts": { "104": 1 },
        "lease_token": "tok"
    }))
    .unwrap();
    assert!(effective_tomestone_progress_coverage(&split, Some(105), &split_partial_none).is_empty());

    let mut split_partial_empty_vec = split_partial_none;
    split_partial_empty_vec.tomestone_queried_encounters = Some(Vec::new());
    assert!(
        effective_tomestone_progress_coverage(&split, Some(105), &split_partial_empty_vec)
            .is_empty()
    );

    let mut split_partial_primary_only: TomestoneResult = serde_json::from_value(serde_json::json!({
        "content_id": 1,
        "zone_id": 73,
        "difficulty_id": 101,
        "encounter_id": 104,
        "clear_counts": { "104": 2 },
        "tomestone_queried_encounters": [104],
        "lease_token": "tok"
    }))
    .unwrap();
    assert_eq!(
        effective_tomestone_progress_coverage(&split, Some(105), &split_partial_primary_only),
        vec![104]
    );
    split_partial_primary_only.tomestone_queried_encounters = None;
    assert!(
        effective_tomestone_progress_coverage(&split, Some(105), &split_partial_primary_only)
            .is_empty()
    );
}

#[test]
fn tomestone_result_deserializes_progress_only_payload_without_encounters_or_clear_counts() {
    let payload = serde_json::json!({
        "content_id": 42,
        "zone_id": 73,
        "difficulty_id": 101,
        "encounter_id": 104,
        "boss_percentages": { "105": 42.5 },
        "tomestone_queried_encounters": [105],
        "source_kind": "tomestone_api",
        "lease_token": "lease-token"
    });
    let parsed: TomestoneResult = serde_json::from_value(payload)
        .expect("progress-only TomestoneResult omitting encounters/clear_counts should deserialize");
    assert!(parsed.encounters.is_empty());
    assert!(parsed.clear_counts.is_empty());
    assert_eq!(parsed.boss_percentages.get(&105).copied(), Some(42.5));
    assert_eq!(parsed.tomestone_queried_encounters, Some(vec![105]));
}

#[test]
fn validate_abandoned_tomestone_leases_scopes_by_encounter_and_suppresses_cooldown_strikes() {
    let ucob_key = normalized_tomestone_lease_key(42, 59, 100, 1073);
    let uwu_key = normalized_tomestone_lease_key(42, 59, 100, 1074);
    let mut leases = HashMap::from([
        (
            ucob_key.clone(),
            TomestoneLeaseEntry {
                leased_at: Utc::now(),
                client_id: "worker-a".to_string(),
                lease_token: "tok-ucob".to_string(),
                secondary_encounter_id: None,
            },
        ),
        (
            uwu_key.clone(),
            TomestoneLeaseEntry {
                leased_at: Utc::now(),
                client_id: "worker-a".to_string(),
                lease_token: "tok-uwu".to_string(),
                secondary_encounter_id: None,
            },
        ),
    ]);

    let validation = validate_abandoned_tomestone_leases(
        &leases,
        vec![
            AbandonTomestoneLease {
                content_id: 42,
                zone_id: 59,
                difficulty_id: 100,
                encounter_id: 1073,
                lease_token: "tok-ucob".to_string(),
                reason: Some(TOMESTONE_CHALLENGE_COOLDOWN_REASON.to_string()),
                processor_kind: None,
                source_kind: Some("tomestone_web".to_string()),
                failure_code: None,
                failure_fingerprint: None,
                message: None,
            },
            AbandonTomestoneLease {
                content_id: 42,
                zone_id: 59,
                difficulty_id: 100,
                encounter_id: 1079, // Wrong encounter
                lease_token: "tok-uwu".to_string(),
                reason: Some(FETCH_FAILED_REASON.to_string()),
                processor_kind: None,
                source_kind: None,
                failure_code: None,
                failure_fingerprint: None,
                message: None,
            },
        ],
        "worker-a",
        DEFAULT_TOMESTONE_PROCESSOR_KIND,
    );

    assert_eq!(validation.accepted.len(), 1);
    assert_eq!(validation.rejected_total, 1);
    assert!(validation.failure_states.is_empty());

    let released = release_valid_abandoned_leases(&mut leases, &validation.accepted);
    assert_eq!(released, 1);
    assert!(!leases.contains_key(&ucob_key));
    assert!(leases.contains_key(&uwu_key));
}

#[tokio::test]
async fn tomestone_cooldown_abandon_route_releases_lease_without_recording_failure_or_touching_sibling() {
    let state = state_with_signature_required().await;
    let ucob_key = normalized_tomestone_lease_key(42, 59, 100, 1073);
    let uwu_key = normalized_tomestone_lease_key(42, 59, 100, 1074);
    {
        let mut leases = state.tomestone_job_leases.write().await;
        leases.insert(
            ucob_key.clone(),
            TomestoneLeaseEntry {
                leased_at: Utc::now(),
                client_id: "tomestone-worker".to_string(),
                lease_token: "tok-ucob".to_string(),
                secondary_encounter_id: None,
            },
        );
        leases.insert(
            uwu_key.clone(),
            TomestoneLeaseEntry {
                leased_at: Utc::now(),
                client_id: "tomestone-worker".to_string(),
                lease_token: "tok-uwu".to_string(),
                secondary_encounter_id: None,
            },
        );
    }

    let route = router(Arc::clone(&state));
    let headers = signed_ingest_headers(
        "test-shared-secret",
        "tomestone-worker",
        "ts-abandon-1",
        "POST",
        "/contribute/tomestone/leases/abandon",
    );
    let req = with_signed_headers(
        warp::test::request()
            .method("POST")
            .path("/contribute/tomestone/leases/abandon")
            .header("content-type", "application/json"),
        &headers,
    );
    let req = add_capability_header(
        &state,
        req,
        "tomestone-worker",
        CapabilityScope::TomestoneLeasesAbandon,
    );
    let response = req
        .body(serde_json::json!([
            {
                "content_id": 42,
                "zone_id": 59,
                "difficulty_id": 100,
                "encounter_id": 1073,
                "lease_token": "tok-ucob",
                "reason": "tomestone_challenge_cooldown"
            }
        ])
        .to_string())
        .reply(&route)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["submitted"], 1);
    assert_eq!(payload["released"], 1);
    assert_eq!(payload["rejected"], 0);
    assert_eq!(state.tomestone_leases_abandoned_total.load(Ordering::Relaxed), 1);

    let leases = state.tomestone_job_leases.read().await;
    assert!(!leases.contains_key(&ucob_key));
    assert!(leases.contains_key(&uwu_key));
}

#[tokio::test]
async fn tomestone_results_and_abandon_routes_reject_invalid_leases_and_oversized_batches() {
    let state = state_with_signature_required().await;
    let real_key = normalized_tomestone_lease_key(42, 73, 101, 104);
    {
        let mut leases = state.tomestone_job_leases.write().await;
        leases.insert(
            real_key.clone(),
            TomestoneLeaseEntry {
                leased_at: Utc::now(),
                client_id: "tomestone-worker".to_string(),
                lease_token: "valid-token".to_string(),
                secondary_encounter_id: Some(105),
            },
        );
    }

    let route = router(Arc::clone(&state));

    // Wrong encounter_id in /contribute/tomestone/results is rejected cleanly without touching valid lease.
    let res_headers = signed_ingest_headers(
        "test-shared-secret",
        "tomestone-worker",
        "ts-res-1",
        "POST",
        "/contribute/tomestone/results",
    );
    let res_req = with_signed_headers(
        warp::test::request()
            .method("POST")
            .path("/contribute/tomestone/results")
            .header("content-type", "application/json"),
        &res_headers,
    );
    let res_req = add_capability_header(
        &state,
        res_req,
        "tomestone-worker",
        CapabilityScope::TomestoneResults,
    );
    let res_response = res_req
        .body(serde_json::json!([
            {
                "content_id": 42,
                "zone_id": 73,
                "difficulty_id": 101,
                "encounter_id": 103, // wrong encounter
                "encounters": {},
                "lease_token": "valid-token"
            }
        ])
        .to_string())
        .reply(&route)
        .await;
    assert_eq!(res_response.status(), StatusCode::OK);
    let res_json: Value = serde_json::from_slice(res_response.body()).unwrap();
    assert_eq!(res_json["status"], "partial");
    assert_eq!(res_json["accepted"], 0);
    assert_eq!(res_json["rejected"], 1);
    assert_eq!(state.tomestone_results_rejected_total.load(Ordering::Relaxed), 1);
    assert!(state.tomestone_job_leases.read().await.contains_key(&real_key));

    // Batch size limit (> 10) on /contribute/tomestone/results returns 413 PAYLOAD_TOO_LARGE.
    let oversized_items: Vec<Value> = (0..11)
        .map(|i| {
            serde_json::json!({
                "content_id": i + 1,
                "zone_id": 73,
                "difficulty_id": 101,
                "encounter_id": 104,
                "encounters": {},
                "lease_token": "tok"
            })
        })
        .collect();
    let res_headers_2 = signed_ingest_headers(
        "test-shared-secret",
        "tomestone-worker",
        "ts-res-2",
        "POST",
        "/contribute/tomestone/results",
    );
    let res_req_2 = with_signed_headers(
        warp::test::request()
            .method("POST")
            .path("/contribute/tomestone/results")
            .header("content-type", "application/json"),
        &res_headers_2,
    );
    let res_req_2 = add_capability_header(
        &state,
        res_req_2,
        "tomestone-worker",
        CapabilityScope::TomestoneResults,
    );
    let oversized_res = res_req_2
        .body(Value::Array(oversized_items).to_string())
        .reply(&route)
        .await;
    assert_eq!(oversized_res.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // Batch size limit (> 10) on /contribute/tomestone/leases/abandon returns 413 PAYLOAD_TOO_LARGE.
    let oversized_abandons: Vec<Value> = (0..11)
        .map(|i| {
            serde_json::json!({
                "content_id": i + 1,
                "zone_id": 73,
                "difficulty_id": 101,
                "encounter_id": 104,
                "lease_token": "tok",
                "reason": "tomestone_rate_limited"
            })
        })
        .collect();
    let abn_headers = signed_ingest_headers(
        "test-shared-secret",
        "tomestone-worker",
        "ts-abn-2",
        "POST",
        "/contribute/tomestone/leases/abandon",
    );
    let abn_req = with_signed_headers(
        warp::test::request()
            .method("POST")
            .path("/contribute/tomestone/leases/abandon")
            .header("content-type", "application/json"),
        &abn_headers,
    );
    let abn_req = add_capability_header(
        &state,
        abn_req,
        "tomestone-worker",
        CapabilityScope::TomestoneLeasesAbandon,
    );
    let oversized_abn = abn_req
        .body(Value::Array(oversized_abandons).to_string())
        .reply(&route)
        .await;
    assert_eq!(oversized_abn.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
