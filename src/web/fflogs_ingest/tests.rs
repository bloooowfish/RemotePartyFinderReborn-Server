use std::collections::HashMap;

use super::{
    active_fflogs_fetch_candidates, build_fflogs_result_encounter_map, failure_state_becomes_final,
    fflogs_exact_home_world_only, fflogs_processor_kind, final_failure_state_blocks_fetch,
    fflogs_worker_region, legacy_tomestone_no_data_reset_filter,
    legacy_tomestone_no_data_reset_update, normalized_lease_key, release_valid_abandoned_leases,
    should_record_failure_state, validate_abandoned_leases, zone_cache_from_parse_result,
    AbandonFflogsLease, FflogsWorkerRegion, ParseJob, ParseResult, PendingFflogsFailureState,
    DEFAULT_FFLOGS_PROCESSOR_KIND, FETCH_FAILED_REASON, FFLOGS_PROCESSOR_KIND_HEADER,
    FFLOGS_WORKER_REGION_HEADER, RATE_LIMIT_DEFERRED_REASON, TOMESTONE_NO_DATA_REASON,
};
use crate::fflogs::PhaseProgress;
use crate::mongo::ZoneCache;
use chrono::{TimeDelta, Utc};
use warp::http::{HeaderMap, HeaderValue};

#[test]
fn fflogs_job_mode_header_requests_exact_home_world_only() {
    let mut headers = HeaderMap::new();
    assert!(!fflogs_exact_home_world_only(&headers));

    headers.insert(
        super::FFLOGS_JOB_MODE_HEADER,
        HeaderValue::from_static(" exact-only "),
    );
    assert!(fflogs_exact_home_world_only(&headers));

    headers.insert(
        super::FFLOGS_JOB_MODE_HEADER,
        HeaderValue::from_static("candidate-capable"),
    );
    assert!(!fflogs_exact_home_world_only(&headers));
}

#[test]
fn fflogs_processor_kind_defaults_to_docker_worker_and_accepts_explicit_header() {
    let mut headers = HeaderMap::new();
    assert_eq!(
        fflogs_processor_kind(&headers),
        DEFAULT_FFLOGS_PROCESSOR_KIND
    );

    headers.insert(
        FFLOGS_PROCESSOR_KIND_HEADER,
        HeaderValue::from_static(" plugin_fflogs_api "),
    );
    assert_eq!(fflogs_processor_kind(&headers), "plugin_fflogs_api");
}

#[test]
fn fflogs_worker_region_header_accepts_supported_regions() {
    let mut headers = HeaderMap::new();
    assert_eq!(fflogs_worker_region(&headers), None);

    headers.insert(
        FFLOGS_WORKER_REGION_HEADER,
        HeaderValue::from_static(" jp "),
    );
    assert_eq!(fflogs_worker_region(&headers), Some(FflogsWorkerRegion::Jp));

    headers.insert(
        FFLOGS_WORKER_REGION_HEADER,
        HeaderValue::from_static("OC"),
    );
    assert_eq!(fflogs_worker_region(&headers), Some(FflogsWorkerRegion::Oc));
}

#[test]
fn fflogs_worker_region_header_ignores_invalid_values_for_global_compatibility() {
    let mut headers = HeaderMap::new();
    headers.insert(
        FFLOGS_WORKER_REGION_HEADER,
        HeaderValue::from_static("all"),
    );

    assert_eq!(fflogs_worker_region(&headers), None);
}

#[test]
fn fflogs_job_payload_does_not_expose_partition() {
    let job = ParseJob {
        content_id: 42,
        name: "Do Dive".to_string(),
        server: "Chocobo".to_string(),
        region: "JP".to_string(),
        candidate_servers: Vec::new(),
        zone_id: 72,
        difficulty_id: 100,
        encounter_id: 1084,
        secondary_encounter_id: None,
        lease_token: "lease-token".to_string(),
    };

    let payload = serde_json::to_value(job).expect("job should serialize");

    assert!(payload.get("partition").is_none());
}

#[test]
fn fflogs_lease_key_is_partition_free() {
    let first = normalized_lease_key(42, 72, 100);
    let second = normalized_lease_key(42, 72, 100);

    assert_eq!(first, second);
}

#[test]
fn fflogs_result_payload_ignores_legacy_partition_field() {
    let result: ParseResult = serde_json::from_value(serde_json::json!({
        "content_id": 42,
        "zone_id": 72,
        "difficulty_id": 100,
        "partition": 99,
        "encounters": { "1084": 96.17 },
        "lease_token": "lease-token"
    }))
    .expect("legacy partition field should be ignored");

    assert_eq!(result.content_id, 42);
    assert_eq!(result.zone_id, 72);
    assert_eq!(result.difficulty_id, 100);
    assert_eq!(result.encounters.get(&1084), Some(&96.17));
}

#[test]
fn fflogs_result_payload_accepts_success_source_kind_for_debugging() {
    let result: ParseResult = serde_json::from_value(serde_json::json!({
        "content_id": 42,
        "zone_id": 72,
        "difficulty_id": 100,
        "encounters": { "1084": 96.17 },
        "source_kind": "fflogs_web_tomestone_web",
        "lease_token": "lease-token"
    }))
    .expect("success source metadata should deserialize");

    assert_eq!(
        result.source_kind.as_deref(),
        Some("fflogs_web_tomestone_web")
    );
}

#[test]
fn fflogs_result_payload_without_phase_progress_deserializes_as_empty() {
    let result: ParseResult = serde_json::from_value(serde_json::json!({
        "content_id": 42,
        "zone_id": 72,
        "difficulty_id": 100,
        "encounters": { "1084": 96.17 },
        "lease_token": "lease-token"
    }))
    .expect("old payload without phase_progress should deserialize");

    assert!(result.phase_progress.is_empty());
}

#[test]
fn zone_cache_records_phase_progress_vectors_from_parse_result() {
    let result: ParseResult = serde_json::from_value(serde_json::json!({
        "content_id": 42,
        "zone_id": 65,
        "difficulty_id": 100,
        "encounters": { "1079": 96.17 },
        "phase_progress": {
            "1079": [
                {
                    "phase_key": "p2",
                    "phase_name": "Diamond Dust",
                    "order": 2,
                    "boss_percentage": 52.4,
                    "display_text": "P2 52.4%",
                    "source": "tomestone_api"
                }
            ]
        },
        "lease_token": "lease-token"
    }))
    .expect("phase progress payload should deserialize");

    let zone_cache = zone_cache_from_parse_result(result, Utc::now());
    let encounter = zone_cache
        .encounters
        .get("1079")
        .expect("encounter should be stored");

    assert_eq!(
        encounter.phase_progress,
        vec![PhaseProgress {
            phase_key: "p2".to_string(),
            phase_name: "Diamond Dust".to_string(),
            order: 2,
            boss_percentage: Some(52.4),
            display_text: "P2 52.4%".to_string(),
            source: Some("tomestone_api".to_string()),
        }]
    );
}

#[test]
fn zone_cache_records_success_source_kind_without_marking_tomestone_as_estimated() {
    let result: ParseResult = serde_json::from_value(serde_json::json!({
        "content_id": 42,
        "zone_id": 72,
        "difficulty_id": 100,
        "encounters": { "1084": 96.17 },
        "is_hidden": false,
        "is_estimated": false,
        "source_kind": "fflogs_web_tomestone_web",
        "lease_token": "lease-token"
    }))
    .expect("success source metadata should deserialize");

    let zone_cache = zone_cache_from_parse_result(result, Utc::now());

    assert!(!zone_cache.estimated);
    assert_eq!(
        zone_cache.source_kind.as_deref(),
        Some("fflogs_web_tomestone_web")
    );
}

#[test]
fn fflogs_abandon_payload_ignores_legacy_partition_field() {
    let abandoned: AbandonFflogsLease = serde_json::from_value(serde_json::json!({
        "content_id": 42,
        "zone_id": 72,
        "difficulty_id": 100,
        "partition": 99,
        "lease_token": "lease-token",
        "reason": "site-down"
    }))
    .expect("legacy partition field should be ignored");

    assert_eq!(abandoned.content_id, 42);
    assert_eq!(abandoned.zone_id, 72);
    assert_eq!(abandoned.difficulty_id, 100);
    assert_eq!(abandoned.lease_token, "lease-token");
    assert_eq!(abandoned.reason.as_deref(), Some("site-down"));
}

#[test]
fn fflogs_abandon_payload_accepts_processor_scoped_failure_metadata() {
    let abandoned: AbandonFflogsLease = serde_json::from_value(serde_json::json!({
        "content_id": 42,
        "zone_id": 72,
        "difficulty_id": 100,
        "lease_token": "lease-token",
        "reason": "fetch_failed",
        "processor_kind": "docker_fflogs_worker",
        "source_kind": "fflogs_web_tomestone_web",
        "failure_code": "tomestone_missing_requested_encounter",
        "failure_fingerprint": "tomestone_missing_requested_encounter",
        "message": "tomestone response did not include requested encounters [1084]"
    }))
    .expect("processor-scoped failure metadata should deserialize");

    assert_eq!(
        abandoned.processor_kind.as_deref(),
        Some("docker_fflogs_worker")
    );
    assert_eq!(
        abandoned.failure_code.as_deref(),
        Some("tomestone_missing_requested_encounter")
    );
    assert_eq!(
        abandoned.failure_fingerprint.as_deref(),
        Some("tomestone_missing_requested_encounter")
    );
}

#[test]
fn fetch_failed_is_final_for_the_reporting_processor_immediately() {
    assert!(should_record_failure_state(FETCH_FAILED_REASON));
    assert!(failure_state_becomes_final(FETCH_FAILED_REASON, 1, 2));
}

#[test]
fn non_fetch_failure_becomes_final_after_threshold() {
    assert!(should_record_failure_state("submit_failed"));
    assert!(!failure_state_becomes_final("submit_failed", 1, 2));
    assert!(failure_state_becomes_final("submit_failed", 2, 2));
}

#[test]
fn tomestone_no_data_is_not_final_until_threshold() {
    assert!(should_record_failure_state(TOMESTONE_NO_DATA_REASON));
    assert!(!failure_state_becomes_final(TOMESTONE_NO_DATA_REASON, 1, 2));
    assert!(failure_state_becomes_final(TOMESTONE_NO_DATA_REASON, 2, 2));
}

#[test]
fn legacy_fetch_failed_tomestone_no_data_state_does_not_block_fetch() {
    assert!(!final_failure_state_blocks_fetch(
        FETCH_FAILED_REASON,
        "tomestone_requested_encounter_empty",
        true
    ));
    assert!(!final_failure_state_blocks_fetch(
        FETCH_FAILED_REASON,
        "tomestone_missing_requested_encounter",
        true
    ));
    assert!(!final_failure_state_blocks_fetch(
        FETCH_FAILED_REASON,
        "tomestone_autocomplete_not_found",
        true
    ));
}

#[test]
fn legacy_fetch_failed_tomestone_no_data_state_resets_before_reclassification() {
    let pending = PendingFflogsFailureState {
        key: normalized_lease_key(42, 72, 100),
        processor_kind: DEFAULT_FFLOGS_PROCESSOR_KIND.to_string(),
        reason: TOMESTONE_NO_DATA_REASON.to_string(),
        source_kind: Some("fflogs_web_tomestone_web".to_string()),
        failure_code: "tomestone_requested_encounter_empty".to_string(),
        failure_fingerprint: "tomestone_requested_encounter_empty".to_string(),
        message: Some("tomestone requested encounters had no usable clear data [1084]".to_string()),
    };

    let filter = legacy_tomestone_no_data_reset_filter("state-id", &pending)
        .expect("tomestone no-data should reset legacy fetch_failed state");
    assert_eq!(filter.get_str("_id"), Ok("state-id"));
    assert_eq!(filter.get_str("reason"), Ok(FETCH_FAILED_REASON));

    let codes = filter
        .get_document("failure_code")
        .expect("failure_code should be an $in filter")
        .get_array("$in")
        .expect("failure code filter should contain known no-data codes");
    assert!(codes
        .iter()
        .any(|code| { code.as_str() == Some("tomestone_requested_encounter_empty") }));

    let update = legacy_tomestone_no_data_reset_update(&pending, Utc::now());
    let set = update
        .get_document("$set")
        .expect("reset update should set fields");
    assert_eq!(set.get_i64("failure_count"), Ok(0));
    assert_eq!(set.get_bool("final_for_processor"), Ok(false));
    assert_eq!(set.get_str("reason"), Ok(TOMESTONE_NO_DATA_REASON));
}

#[test]
fn true_fetch_failed_state_does_not_use_legacy_tomestone_reset() {
    let pending = PendingFflogsFailureState {
        key: normalized_lease_key(42, 72, 100),
        processor_kind: DEFAULT_FFLOGS_PROCESSOR_KIND.to_string(),
        reason: FETCH_FAILED_REASON.to_string(),
        source_kind: Some("fflogs_web_tomestone_web".to_string()),
        failure_code: FETCH_FAILED_REASON.to_string(),
        failure_fingerprint: FETCH_FAILED_REASON.to_string(),
        message: Some("tomestone autocomplete request returned HTTP 503".to_string()),
    };

    assert!(legacy_tomestone_no_data_reset_filter("state-id", &pending).is_none());
}

#[test]
fn real_fetch_failed_and_final_tomestone_no_data_state_block_fetch() {
    assert!(final_failure_state_blocks_fetch(
        FETCH_FAILED_REASON,
        "fetch_failed",
        true
    ));
    assert!(final_failure_state_blocks_fetch(
        TOMESTONE_NO_DATA_REASON,
        "tomestone_requested_encounter_empty",
        true
    ));
    assert!(!final_failure_state_blocks_fetch(
        TOMESTONE_NO_DATA_REASON,
        "tomestone_requested_encounter_empty",
        false
    ));
}

#[test]
fn rate_limit_deferred_does_not_record_failure_state() {
    assert!(!should_record_failure_state(RATE_LIMIT_DEFERRED_REASON));
    assert!(!failure_state_becomes_final(
        RATE_LIMIT_DEFERRED_REASON,
        100,
        2
    ));
}

#[test]
fn accepted_abandon_keeps_lease_until_failure_state_can_be_recorded() {
    let key = normalized_lease_key(42, 72, 100);
    let mut leases = HashMap::from([(
        key.clone(),
        crate::web::FflogsLeaseEntry {
            leased_at: Utc::now(),
            client_id: "client-a".to_string(),
            lease_token: "lease-token".to_string(),
        },
    )]);

    let abandoned = AbandonFflogsLease {
        content_id: 42,
        zone_id: 72,
        difficulty_id: 100,
        lease_token: "lease-token".to_string(),
        reason: Some(FETCH_FAILED_REASON.to_string()),
        processor_kind: Some(DEFAULT_FFLOGS_PROCESSOR_KIND.to_string()),
        source_kind: Some("fflogs_web_tomestone_web".to_string()),
        failure_code: Some("tomestone_missing_requested_encounter".to_string()),
        failure_fingerprint: Some("tomestone_missing_requested_encounter".to_string()),
        message: Some("tomestone response did not include requested encounters".to_string()),
    };

    let validation = validate_abandoned_leases(
        &leases,
        vec![abandoned],
        "client-a",
        DEFAULT_FFLOGS_PROCESSOR_KIND,
    );

    assert_eq!(validation.accepted.len(), 1);
    assert_eq!(validation.rejected_total, 0);
    assert_eq!(validation.failure_states.len(), 1);
    assert!(leases.contains_key(&key));

    let released = release_valid_abandoned_leases(&mut leases, &validation.accepted);

    assert_eq!(released, 1);
    assert!(!leases.contains_key(&key));
}

#[test]
fn fflogs_result_encounter_map_skips_zero_percentile_without_clear_evidence() {
    let encounters = HashMap::from([(101, 0.0)]);
    let boss_percentages = HashMap::new();
    let clear_counts = HashMap::from([(101, 0)]);

    let encounter_map = build_fflogs_result_encounter_map(
        encounters,
        boss_percentages,
        clear_counts,
        HashMap::new(),
    );

    assert!(encounter_map.is_empty());
}

#[test]
fn fflogs_result_encounter_map_keeps_zero_percentile_with_positive_clear_count() {
    let encounters = HashMap::from([(101, 0.0)]);
    let boss_percentages = HashMap::new();
    let clear_counts = HashMap::from([(101, 1)]);

    let encounter_map = build_fflogs_result_encounter_map(
        encounters,
        boss_percentages,
        clear_counts,
        HashMap::new(),
    );

    let encounter = encounter_map.get("101").expect("encounter should be kept");
    assert_eq!(encounter.percentile, 0.0);
    assert_eq!(encounter.clear_count, Some(1));
}

#[test]
fn fflogs_result_encounter_map_stores_phase_progress_only_entry() {
    let encounters = HashMap::new();
    let boss_percentages = HashMap::new();
    let clear_counts = HashMap::from([(1079, 3)]);
    let phase_progress = HashMap::from([(
        1079,
        vec![PhaseProgress {
            phase_key: "p4".to_string(),
            phase_name: "Crystallize Time".to_string(),
            order: 4,
            boss_percentage: None,
            display_text: "P4 reached".to_string(),
            source: Some("tomestone_api".to_string()),
        }],
    )]);

    let encounter_map = build_fflogs_result_encounter_map(
        encounters,
        boss_percentages,
        clear_counts,
        phase_progress,
    );

    let encounter = encounter_map
        .get("1079")
        .expect("phase-progress-only encounter should be stored");
    assert_eq!(encounter.percentile, -1.0);
    assert_eq!(encounter.boss_percentage, None);
    assert_eq!(encounter.clear_count, Some(3));
    assert_eq!(encounter.phase_progress.len(), 1);
}

#[test]
fn fflogs_result_encounter_map_filters_zero_phase_progress() {
    let phase_progress = HashMap::from([(
        1079,
        vec![PhaseProgress {
            phase_key: "p2".to_string(),
            phase_name: "Diamond Dust".to_string(),
            order: 2,
            boss_percentage: Some(0.0),
            display_text: "P2 0.00%".to_string(),
            source: Some("tomestone_api".to_string()),
        }],
    )]);

    let encounter_map = build_fflogs_result_encounter_map(
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        phase_progress,
    );

    assert!(encounter_map.is_empty());
}

#[test]
fn fflogs_result_ingests_mechanic_progress_without_a_fabricated_phase_number() {
    let mechanic = PhaseProgress {
        phase_key: "mechanic-splattershed-2".into(),
        phase_name: "Splattershed #2".into(),
        order: 0,
        boss_percentage: Some(35.82),
        display_text: "Splattershed #2 35.82%".into(),
        source: Some("tomestone_api".into()),
    };
    let mut invalid = mechanic.clone();
    invalid.phase_name.clear();
    let mut fabricated_phase = mechanic.clone();
    fabricated_phase.order = 2;
    let phases = HashMap::from([(1079, vec![mechanic.clone(), invalid, fabricated_phase])]);
    let encounter_map =
        build_fflogs_result_encounter_map(HashMap::new(), HashMap::new(), HashMap::new(), phases);
    let stored = &encounter_map
        .get("1079")
        .expect("mechanic should be stored")
        .phase_progress;
    assert_eq!(stored, &vec![mechanic.clone()]);

    let cache = test_zone_cache(Utc::now(), false, false);
    let cache = ZoneCache {
        encounters: encounter_map,
        ..cache
    };
    let resolved = crate::parse_resolver::resolve_parse_data(Some(&cache), 65, 1079, None);
    assert_eq!(resolved.primary_phase_progress, vec![mechanic]);
}

fn test_zone_cache(fetched_at: chrono::DateTime<Utc>, estimated: bool, hidden: bool) -> ZoneCache {
    ZoneCache {
        fetched_at,
        estimated,
        matched_server: None,
        source_kind: None,
        hidden,
        encounters: HashMap::new(),
    }
}

#[test]
fn fflogs_active_fetch_candidates_prioritize_missing_before_stale_refreshes() {
    let now = Utc::now();
    let member_ids = vec![1, 2, 3, 4];
    let cached_zones = HashMap::from([
        (
            1,
            test_zone_cache(now - TimeDelta::try_minutes(40).unwrap(), false, false),
        ),
        (
            2,
            test_zone_cache(now - TimeDelta::try_hours(2).unwrap(), false, true),
        ),
        (
            4,
            test_zone_cache(now - TimeDelta::try_minutes(5).unwrap(), false, false),
        ),
    ]);

    let candidates = active_fflogs_fetch_candidates(member_ids, &cached_zones, 1, 15, now);

    assert_eq!(candidates.content_ids, vec![3, 1, 2]);
    assert_eq!(candidates.hidden_refresh_candidates, 1);
}

#[test]
fn fflogs_active_visible_refresh_candidates_are_oldest_first() {
    let now = Utc::now();
    let member_ids = vec![1, 2, 3];
    let cached_zones = HashMap::from([
        (
            1,
            test_zone_cache(now - TimeDelta::try_minutes(20).unwrap(), false, false),
        ),
        (
            2,
            test_zone_cache(now - TimeDelta::try_minutes(50).unwrap(), false, false),
        ),
        (
            3,
            test_zone_cache(now - TimeDelta::try_minutes(35).unwrap(), true, false),
        ),
    ]);

    let candidates = active_fflogs_fetch_candidates(member_ids, &cached_zones, 24, 15, now);

    assert_eq!(candidates.content_ids, vec![2, 3, 1]);
    assert_eq!(candidates.hidden_refresh_candidates, 0);
}

const FFLOGS_JOB_ORDER_STALE_ID: u64 = 9_031_001;
const FFLOGS_JOB_ORDER_MISSING_ID: u64 = 9_031_002;

fn test_player(
    content_id: u64,
    name: &str,
    home_world: u16,
    current_world: u16,
    last_seen: chrono::DateTime<Utc>,
) -> crate::player::Player {
    crate::player::Player {
        content_id,
        name: name.to_string(),
        home_world,
        current_world,
        last_seen,
        seen_count: 1,
        account_id: "-1".to_string(),
    }
}

#[test]
fn jp_worker_keeps_jp_anchor_and_jp_home_world_jobs() {
    let now = Utc::now();
    let players = vec![test_player(10, "JP Player", 23, 23, now)];
    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(1010).expect("duty should map");
    let mut jobs = Vec::new();

    super::append_fflogs_jobs_for_players(
        &mut jobs,
        &[10],
        players,
        super::FflogsJobAppendContext {
            limit: 10,
            exact_home_world_only: false,
            worker_region: Some(FflogsWorkerRegion::Jp),
            listing_anchor_world: 23,
            listing_candidates: &[],
            fflogs_info,
            difficulty_id: fflogs_info.difficulty_id.unwrap_or(0) as i32,
        },
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].region, "JP");
}

#[test]
fn jp_worker_skips_na_anchor_jobs() {
    let now = Utc::now();
    let players = vec![test_player(10, "NA Player", 73, 73, now)];
    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(1010).expect("duty should map");
    let mut jobs = Vec::new();

    super::append_fflogs_jobs_for_players(
        &mut jobs,
        &[10],
        players,
        super::FflogsJobAppendContext {
            limit: 10,
            exact_home_world_only: false,
            worker_region: Some(FflogsWorkerRegion::Jp),
            listing_anchor_world: 73,
            listing_candidates: &[],
            fflogs_info,
            difficulty_id: fflogs_info.difficulty_id.unwrap_or(0) as i32,
        },
    );

    assert!(jobs.is_empty());
}

#[test]
fn oc_worker_allows_jp_home_world_from_oce_anchor_listing() {
    let now = Utc::now();
    let players = vec![test_player(10, "JP Traveler", 23, 21, now)];
    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(1010).expect("duty should map");
    let mut jobs = Vec::new();

    super::append_fflogs_jobs_for_players(
        &mut jobs,
        &[10],
        players,
        super::FflogsJobAppendContext {
            limit: 10,
            exact_home_world_only: false,
            worker_region: Some(FflogsWorkerRegion::Oc),
            listing_anchor_world: 21,
            listing_candidates: &[],
            fflogs_info,
            difficulty_id: fflogs_info.difficulty_id.unwrap_or(0) as i32,
        },
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].region, "JP");
}

#[test]
fn oc_worker_skips_non_oce_anchor_listing_even_if_home_world_is_oce() {
    let now = Utc::now();
    let players = vec![test_player(10, "OCE Visitor", 21, 23, now)];
    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(1010).expect("duty should map");
    let mut jobs = Vec::new();

    super::append_fflogs_jobs_for_players(
        &mut jobs,
        &[10],
        players,
        super::FflogsJobAppendContext {
            limit: 10,
            exact_home_world_only: false,
            worker_region: Some(FflogsWorkerRegion::Oc),
            listing_anchor_world: 23,
            listing_candidates: &[],
            fflogs_info,
            difficulty_id: fflogs_info.difficulty_id.unwrap_or(0) as i32,
        },
    );

    assert!(jobs.is_empty());
}

#[test]
fn global_worker_keeps_existing_cross_region_behavior() {
    let now = Utc::now();
    let players = vec![test_player(10, "NA Player", 73, 73, now)];
    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(1010).expect("duty should map");
    let mut jobs = Vec::new();

    super::append_fflogs_jobs_for_players(
        &mut jobs,
        &[10],
        players,
        super::FflogsJobAppendContext {
            limit: 10,
            exact_home_world_only: false,
            worker_region: None,
            listing_anchor_world: 73,
            listing_candidates: &[],
            fflogs_info,
            difficulty_id: fflogs_info.difficulty_id.unwrap_or(0) as i32,
        },
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].region, "NA");
}

#[test]
fn contribute_fflogs_jobs_preserves_fetch_candidate_priority_under_limit() {
    let now = Utc::now();
    let players = vec![
        test_player(FFLOGS_JOB_ORDER_STALE_ID, "Stale First", 74, 74, now),
        test_player(
            FFLOGS_JOB_ORDER_MISSING_ID,
            "Missing Priority",
            74,
            74,
            now,
        ),
    ];
    let ordered_fetch_candidates = vec![FFLOGS_JOB_ORDER_MISSING_ID, FFLOGS_JOB_ORDER_STALE_ID];
    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(1010).expect("duty should map");

    let mut jobs = Vec::new();
    super::append_fflogs_jobs_for_players(
        &mut jobs,
        &ordered_fetch_candidates,
        players,
        super::FflogsJobAppendContext {
            limit: 1,
            exact_home_world_only: false,
            worker_region: None,
            listing_anchor_world: 74,
            listing_candidates: &[],
            fflogs_info,
            difficulty_id: fflogs_info.difficulty_id.unwrap_or(0) as i32,
        },
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].content_id, FFLOGS_JOB_ORDER_MISSING_ID);
}
