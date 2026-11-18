use chrono::{DateTime, TimeDelta, Utc};
use mongodb::{
    bson::{Bson, Document, doc},
    Collection,
};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::SocketAddr,
    sync::Arc,
};
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use super::ingest_guard::{self, CapabilityScope, GuardContext, IngestEndpoint};
use super::{FflogsLeaseEntry, State, TomestoneLeaseEntry};

pub(super) const RATE_LIMIT_DEFERRED_REASON: &str = "worker_rate_limit_deferred";
pub(super) const WORKER_CYCLE_ABORTED_REASON: &str = "worker_cycle_aborted";
pub(super) const WORKER_BATCH_LIMIT_OVERFLOW_REASON: &str = "worker_batch_limit_overflow";
pub(super) const WORKER_SHUTDOWN_DEFERRED_REASON: &str = "worker_shutdown_deferred";
pub(super) const UPSTREAM_TRANSIENT_ERROR_REASON: &str = "upstream_transient_error";
pub(super) const FFLOGS_RATE_LIMITED_REASON: &str = "fflogs_rate_limited";
pub(super) const TOMESTONE_RATE_LIMITED_REASON: &str = "tomestone_rate_limited";
pub(super) const TOMESTONE_CHALLENGE_COOLDOWN_REASON: &str = "tomestone_challenge_cooldown";
pub(super) const FETCH_FAILED_REASON: &str = "fetch_failed";
pub(super) const TOMESTONE_NO_DATA_REASON: &str = "tomestone_no_data";
pub(super) const FAILURE_FIELD_MAX_CHARS: usize = 256;
const FAILURE_MESSAGE_MAX_CHARS: usize = 512;
pub(super) const FAILURE_RETRY_COOLDOWN: chrono::Duration = chrono::Duration::hours(1);

const NON_RECORDABLE_COMMON_REASONS: [&str; 5] = [
    RATE_LIMIT_DEFERRED_REASON,
    WORKER_CYCLE_ABORTED_REASON,
    WORKER_BATCH_LIMIT_OVERFLOW_REASON,
    WORKER_SHUTDOWN_DEFERRED_REASON,
    UPSTREAM_TRANSIENT_ERROR_REASON,
];

pub(super) fn is_non_recordable_common_reason(reason: &str) -> bool {
    let trimmed = reason.trim();
    NON_RECORDABLE_COMMON_REASONS
        .iter()
        .any(|excluded| trimmed.eq_ignore_ascii_case(excluded))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkerRegion {
    Jp,
    Na,
    Eu,
    Oc,
}

impl WorkerRegion {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Jp => "JP",
            Self::Na => "NA",
            Self::Eu => "EU",
            Self::Oc => "OC",
        }
    }
}

#[cfg(test)]
pub(super) fn header_equals_ignore_ascii_case(
    headers: &HeaderMap,
    header_name: &str,
    expected: &str,
) -> bool {
    headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .map(|value| value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

pub(super) fn parse_processor_kind(
    headers: &HeaderMap,
    header_name: &str,
    default_kind: &str,
) -> String {
    headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .map(normalize_processor_kind)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default_kind.to_string())
}

pub(super) fn parse_jobs_limit(
    headers: &HeaderMap,
    header_name: &str,
    max_limit: usize,
) -> usize {
    headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|parsed| *parsed >= 1)
        .map(|parsed| parsed.min(max_limit.max(1)))
        .unwrap_or(max_limit)
}

pub(super) fn parse_worker_region(headers: &HeaderMap, header_name: &str) -> Option<WorkerRegion> {
    let value = headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())?
        .trim();

    if value.eq_ignore_ascii_case("JP") {
        Some(WorkerRegion::Jp)
    } else if value.eq_ignore_ascii_case("NA") {
        Some(WorkerRegion::Na)
    } else if value.eq_ignore_ascii_case("EU") {
        Some(WorkerRegion::Eu)
    } else if value.eq_ignore_ascii_case("OC") || value.eq_ignore_ascii_case("OCE") {
        Some(WorkerRegion::Oc)
    } else {
        None
    }
}

pub(super) fn normalize_processor_kind(value: &str) -> String {
    normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS)
}

pub(super) fn normalize_failure_field(value: &str, max_chars: usize) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
        .take(max_chars)
        .collect()
}

pub(super) fn normalize_failure_message(value: &str) -> String {
    value
        .trim()
        .chars()
        .take(FAILURE_MESSAGE_MAX_CHARS)
        .collect()
}

pub(super) fn first_valid_listing_world(listing: &crate::listing::PartyFinderListing) -> u16 {
    [
        listing.current_world,
        listing.created_world,
        listing.home_world,
    ]
    .into_iter()
    .find(|wid| *wid != 0 && crate::ffxiv::WORLDS.get(&(*wid as u32)).is_some())
    .unwrap_or(0)
}

pub(super) fn region_for_world_id(world_id: u16) -> Option<&'static str> {
    crate::ffxiv::WORLDS
        .get(&(world_id as u32))
        .map(|world| crate::fflogs::get_region_from_server(world.as_str()))
}

pub(super) fn resolve_exact_player_home_server_and_region(
    player: &crate::player::Player,
) -> Option<(String, String)> {
    if player.home_world == 0 {
        return None;
    }

    let world = crate::ffxiv::WORLDS.get(&u32::from(player.home_world))?;
    let server = world.as_str().to_string();
    let region = crate::fflogs::get_region_from_server(&server).to_string();
    Some((server, region))
}

pub(super) fn should_dispatch_for_worker_region(
    worker_region: Option<WorkerRegion>,
    listing_anchor_world: u16,
    resolved_job_region: &str,
) -> bool {
    let Some(worker_region) = worker_region else {
        return true;
    };

    let Some(anchor_region) = region_for_world_id(listing_anchor_world) else {
        return false;
    };

    if worker_region == WorkerRegion::Oc {
        return anchor_region == "OC";
    }

    worker_region.as_str().eq_ignore_ascii_case(anchor_region)
        && worker_region
            .as_str()
            .eq_ignore_ascii_case(resolved_job_region)
}

pub(super) async fn authorize_jobs_request(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    scope: CapabilityScope,
    headers: &HeaderMap,
    remote_addr: Option<SocketAddr>,
    path: &str,
) -> std::result::Result<GuardContext, warp::reply::Response> {
    let guard_ctx =
        match ingest_guard::authorize_request(state, endpoint, headers, remote_addr, "GET", path)
            .await
        {
            Ok(ctx) => ctx,
            Err(error) => return Err(ingest_guard::guard_error_reply(error)),
        };

    if let Err(error) =
        ingest_guard::require_capability(state, headers, &guard_ctx.client_id, scope, None)
    {
        if let Err(rate_err) =
            ingest_guard::record_capability_auth_failure(state, endpoint, &guard_ctx).await
        {
            return Err(ingest_guard::guard_error_reply(rate_err));
        }
        return Err(ingest_guard::guard_error_reply(error));
    }

    Ok(guard_ctx)
}

pub(super) fn payload_too_large_response(message: &'static str) -> warp::reply::Response {
    warp::reply::with_status(message, StatusCode::PAYLOAD_TOO_LARGE).into_response()
}

#[derive(Debug, serde::Serialize, Clone)]
pub struct ParseJobCandidateServer {
    pub server: String,
    pub region: String,
}

#[derive(Debug, serde::Serialize)]
pub struct ParseJob {
    pub content_id: u64,
    pub name: String,
    pub server: String,
    pub region: String,
    /// home_world를 모를 때 후보 서버(월드) 목록. 있으면 플러그인이 추정 매칭을 시도한다.
    #[serde(default)]
    pub candidate_servers: Vec<ParseJobCandidateServer>,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub encounter_id: u32,
    #[serde(default)]
    pub secondary_encounter_id: Option<u32>,
    #[serde(default)]
    pub lease_token: String,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ParseResultsResponse {
    pub status: &'static str,
    pub submitted: usize,
    pub accepted: usize,
    pub updated: usize,
    pub rejected: usize,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ParseLeaseAbandonResponse {
    pub status: &'static str,
    pub submitted: usize,
    pub released: usize,
    pub rejected: usize,
}

pub(super) trait ParseLeaseEntry {
    fn leased_at(&self) -> DateTime<Utc>;
    fn client_id(&self) -> &str;
    fn lease_token(&self) -> &str;
}

impl ParseLeaseEntry for FflogsLeaseEntry {
    fn leased_at(&self) -> DateTime<Utc> {
        self.leased_at
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }

    fn lease_token(&self) -> &str {
        &self.lease_token
    }
}

impl ParseLeaseEntry for TomestoneLeaseEntry {
    fn leased_at(&self) -> DateTime<Utc> {
        self.leased_at
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }

    fn lease_token(&self) -> &str {
        &self.lease_token
    }
}

pub(super) fn snapshot_active_leases<K, V>(
    leases: &HashMap<K, V>,
    lease_ttl: TimeDelta,
    now: DateTime<Utc>,
) -> HashSet<K>
where
    K: Clone + Eq + Hash,
    V: ParseLeaseEntry,
{
    leases
        .iter()
        .filter(|(_, lease)| (lease.leased_at() + lease_ttl) > now)
        .map(|(key, _)| key.clone())
        .collect()
}

pub(super) fn lease_matches<V: ParseLeaseEntry>(
    lease: Option<&V>,
    client_id: &str,
    lease_token: &str,
) -> bool {
    matches!(
        lease,
        Some(lease)
            if !lease_token.is_empty()
                && lease.client_id() == client_id
                && lease.lease_token() == lease_token
    )
}

pub(super) fn release_committed_leases<K, V>(
    leases: &mut HashMap<K, V>,
    committed: &[(K, String)],
    client_id: &str,
) -> usize
where
    K: Eq + Hash,
    V: ParseLeaseEntry,
{
    let mut released = 0usize;
    for (key, lease_token) in committed {
        let matches = leases
            .get(key)
            .is_some_and(|lease| lease.client_id() == client_id && lease.lease_token() == lease_token);
        if matches && leases.remove(key).is_some() {
            released += 1;
        }
    }
    released
}

#[derive(Debug)]
pub(super) struct AcceptedAbandonedLease<K> {
    pub key: K,
    pub client_id: String,
    pub lease_token: String,
}

pub(super) fn release_valid_abandoned_leases<K, V>(
    leases: &mut HashMap<K, V>,
    accepted: &[AcceptedAbandonedLease<K>],
) -> usize
where
    K: Eq + Hash,
    V: ParseLeaseEntry,
{
    let mut released_total = 0usize;
    for abandoned in accepted {
        let valid = lease_matches(
            leases.get(&abandoned.key),
            &abandoned.client_id,
            &abandoned.lease_token,
        );
        if valid && leases.remove(&abandoned.key).is_some() {
            released_total += 1;
        }
    }
    released_total
}

fn positive_clear_count(clear_counts: &HashMap<i32, i32>, enc_id: i32) -> Option<u32> {
    clear_counts
        .get(&enc_id)
        .copied()
        .filter(|value| *value > 0)
        .map(|value| value as u32)
}

fn normalize_clear_percentile(percentile: f64, clear_count: Option<u32>) -> Option<f32> {
    if !percentile.is_finite() {
        return None;
    }

    if percentile > 0.0 || (percentile == 0.0 && clear_count.is_some()) {
        Some(percentile.clamp(0.0, 100.0) as f32)
    } else {
        None
    }
}

fn positive_boss_percentage(value: f64) -> Option<f32> {
    if value.is_finite() && value > 0.0 {
        Some(value as f32)
    } else {
        None
    }
}

fn valid_phase_progress(phase: &crate::mongo::PhaseProgress) -> bool {
    if phase.order == 0 || phase.phase_key.starts_with("mechanic-") {
        return crate::parse_resolver::tomestone_mechanic_percentage(phase).is_some();
    }

    match phase.boss_percentage {
        Some(value) => value.is_finite() && value > 0.0,
        None => true,
    }
}

fn sanitize_phase_progress(
    phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
) -> HashMap<i32, Vec<crate::mongo::PhaseProgress>> {
    phase_progress
        .into_iter()
        .filter_map(|(enc_id, phases)| {
            let phases = phases
                .into_iter()
                .filter(valid_phase_progress)
                .collect::<Vec<_>>();
            (!phases.is_empty()).then_some((enc_id, phases))
        })
        .collect()
}

pub(super) fn build_result_encounter_map(
    encounters: HashMap<i32, f64>,
    boss_percentages: HashMap<i32, f64>,
    clear_counts: HashMap<i32, i32>,
    phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
) -> HashMap<String, crate::mongo::EncounterParse> {
    let mut encounter_map = HashMap::new();
    let mut phase_progress = sanitize_phase_progress(phase_progress);

    for (enc_id, percentile) in encounters {
        let boss_percentage = boss_percentages
            .get(&enc_id)
            .copied()
            .and_then(positive_boss_percentage);
        let clear_count = positive_clear_count(&clear_counts, enc_id);

        if let Some(normalized_percentile) = normalize_clear_percentile(percentile, clear_count) {
            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: normalized_percentile,
                    job_id: 0,
                    boss_percentage,
                    clear_count,
                    phase_progress: phase_progress.remove(&enc_id).unwrap_or_default(),
                    tomestone_fetched_at: None,
                },
            );
        } else if let Some(boss_percentage) = boss_percentage {
            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: -1.0,
                    job_id: 0,
                    boss_percentage: Some(boss_percentage),
                    clear_count,
                    phase_progress: phase_progress.remove(&enc_id).unwrap_or_default(),
                    tomestone_fetched_at: None,
                },
            );
        }
    }

    // progress-only 데이터가 들어온 경우도 저장
    for (enc_id, boss_percentage) in boss_percentages {
        let key = enc_id.to_string();
        if encounter_map.contains_key(&key) {
            continue;
        }
        let Some(boss_percentage) = positive_boss_percentage(boss_percentage) else {
            continue;
        };
        let clear_count = positive_clear_count(&clear_counts, enc_id);
        encounter_map.insert(
            key,
            crate::mongo::EncounterParse {
                percentile: -1.0,
                job_id: 0,
                boss_percentage: Some(boss_percentage),
                clear_count,
                phase_progress: phase_progress.remove(&enc_id).unwrap_or_default(),
                tomestone_fetched_at: None,
            },
        );
    }

    // phase-progress-only data is additive and should still create an encounter entry.
    for (enc_id, phases) in phase_progress {
        if phases.is_empty() {
            continue;
        }
        let key = enc_id.to_string();
        if encounter_map.contains_key(&key) {
            continue;
        }
        let clear_count = positive_clear_count(&clear_counts, enc_id);
        encounter_map.insert(
            key,
            crate::mongo::EncounterParse {
                percentile: -1.0,
                job_id: 0,
                boss_percentage: None,
                clear_count,
                phase_progress: phases,
                tomestone_fetched_at: None,
            },
        );
    }

    // 4th pass: encounters present only with positive clear_count.
    for (enc_id, clear_count) in clear_counts {
        if clear_count <= 0 {
            continue;
        }
        let key = enc_id.to_string();
        if encounter_map.contains_key(&key) {
            continue;
        }
        encounter_map.insert(
            key,
            crate::mongo::EncounterParse {
                percentile: -1.0,
                job_id: 0,
                boss_percentage: None,
                clear_count: Some(clear_count as u32),
                phase_progress: Vec::new(),
                tomestone_fetched_at: None,
            },
        );
    }

    encounter_map
}

pub(super) struct ParseResultFields {
    pub encounters: HashMap<i32, f64>,
    pub boss_percentages: HashMap<i32, f64>,
    pub clear_counts: HashMap<i32, i32>,
    pub phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
    pub is_hidden: bool,
    pub is_fflogs_hidden: bool,
    pub is_estimated: bool,
    pub matched_server: Option<String>,
    pub source_kind: Option<String>,
}

pub(super) fn zone_cache_from_fields(
    fields: ParseResultFields,
    fetched_at: DateTime<Utc>,
) -> crate::mongo::ZoneCache {
    let encounter_map = build_result_encounter_map(
        fields.encounters,
        fields.boss_percentages,
        fields.clear_counts,
        fields.phase_progress,
    );

    crate::mongo::ZoneCache {
        fetched_at,
        fflogs_fetched_at: None,
        tomestone_fetched_at: None,
        estimated: fields.is_estimated,
        matched_server: fields.matched_server.filter(|s| !s.trim().is_empty()),
        source_kind: fields.source_kind.filter(|s| !s.trim().is_empty()),
        hidden: fields.is_hidden,
        fflogs_hidden: fields.is_fflogs_hidden,
        encounters: encounter_map,
    }
}

pub(super) struct AbandonMetadata<'a> {
    pub reason: Option<&'a str>,
    pub source_kind: Option<&'a str>,
    pub failure_code: Option<&'a str>,
    pub failure_fingerprint: Option<&'a str>,
    pub message: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FailureTarget {
    pub content_id: u64,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub encounter_id: Option<u32>,
}

#[derive(Debug)]
pub(super) struct PendingFailureState<K> {
    pub key: K,
    pub content_id: u64,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub encounter_id: Option<u32>,
    pub processor_kind: String,
    pub reason: String,
    pub source_kind: Option<String>,
    pub failure_code: String,
    pub failure_fingerprint: String,
    pub message: Option<String>,
}

pub(super) fn build_pending_failure_state<K>(
    key: K,
    target: FailureTarget,
    abandoned: AbandonMetadata<'_>,
    lease_processor_kind: &str,
    request_processor_kind: &str,
    should_record: fn(&str) -> bool,
) -> Option<PendingFailureState<K>> {
    let reason = abandoned
        .reason
        .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    if !should_record(&reason) {
        return None;
    }

    let processor_kind = Some(lease_processor_kind)
        .map(normalize_processor_kind)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| request_processor_kind.to_string());

    let failure_code = abandoned
        .failure_code
        .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| reason.clone());

    let failure_fingerprint = abandoned
        .failure_fingerprint
        .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| failure_code.clone());

    Some(PendingFailureState {
        key,
        content_id: target.content_id,
        zone_id: target.zone_id,
        difficulty_id: target.difficulty_id,
        encounter_id: target.encounter_id,
        processor_kind,
        reason,
        source_kind: abandoned
            .source_kind
            .map(|value| normalize_failure_field(value, FAILURE_FIELD_MAX_CHARS))
            .filter(|value| !value.is_empty()),
        failure_code,
        failure_fingerprint,
        message: abandoned
            .message
            .map(normalize_failure_message)
            .filter(|value| !value.is_empty()),
    })
}

#[cfg(test)]
pub(super) fn expired_failure_reset_filter(id: &str, now: DateTime<Utc>) -> Document {
    doc! {
        "_id": id,
        "$or": [
            { "last_failed_at": { "$lte": now - FAILURE_RETRY_COOLDOWN } },
            { "last_failed_at": { "$exists": false } },
        ],
    }
}

#[cfg(test)]
pub(super) fn expired_failure_reset_update(now: DateTime<Utc>) -> Document {
    doc! {
        "$set": {
            "first_failed_at": now,
            "last_failed_at": now,
            "failure_count": 0_i64,
            "final_for_processor": false,
        },
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct JobFailureStateDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub content_id: i64,
    pub zone_id: i32,
    pub difficulty_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encounter_id: Option<i32>,
    pub processor_kind: String,
    pub reason: String,
    pub source_kind: Option<String>,
    pub failure_code: String,
    pub failure_fingerprint: String,
    pub failure_count: i64,
    pub final_for_processor: bool,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub first_failed_at: DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub last_failed_at: DateTime<Utc>,
    pub message: Option<String>,
}

fn bson_literal_str(value: &str) -> Bson {
    Bson::Document(doc! { "$literal": value })
}

pub(super) fn build_failure_state_upsert_pipeline<K>(
    pending: &PendingFailureState<K>,
    now: DateTime<Utc>,
    threshold: u32,
    can_become_final: bool,
    legacy_no_data_codes: Option<&[&str]>,
) -> Vec<Document> {
    let now_bson = mongodb::bson::DateTime::from_chrono(now);
    let cutoff_bson = mongodb::bson::DateTime::from_chrono(now - FAILURE_RETRY_COOLDOWN);

    let expired_or_missing = doc! {
        "$or": [
            { "$eq": [{ "$ifNull": ["$last_failed_at", Bson::Null] }, Bson::Null] },
            { "$lte": ["$last_failed_at", cutoff_bson] },
        ]
    };

    let is_reset = match legacy_no_data_codes {
        Some(codes)
            if pending.reason.eq_ignore_ascii_case(TOMESTONE_NO_DATA_REASON)
                && codes
                    .iter()
                    .any(|code| pending.failure_code.eq_ignore_ascii_case(code)) =>
        {
            let code_values = codes.iter().map(|code| (*code).to_string()).collect::<Vec<_>>();
            doc! {
                "$or": [
                    expired_or_missing,
                    {
                        "$and": [
                            { "$eq": ["$reason", FETCH_FAILED_REASON] },
                            { "$in": ["$failure_code", code_values] },
                        ]
                    },
                ]
            }
        }
        _ => expired_or_missing,
    };

    let mut stage1 = doc! {
        "content_id": { "$ifNull": ["$content_id", pending.content_id as i64] },
        "zone_id": { "$ifNull": ["$zone_id", pending.zone_id as i32] },
        "difficulty_id": { "$ifNull": ["$difficulty_id", pending.difficulty_id] },
        "processor_kind": {
            "$ifNull": ["$processor_kind", bson_literal_str(&pending.processor_kind)]
        },
        "failure_fingerprint": {
            "$ifNull": [
                "$failure_fingerprint",
                bson_literal_str(&pending.failure_fingerprint),
            ]
        },
        "first_failed_at": {
            "$cond": [
                is_reset.clone(),
                now_bson,
                { "$ifNull": ["$first_failed_at", now_bson] },
            ]
        },
        "failure_count": {
            "$cond": [
                is_reset,
                1_i64,
                { "$add": [{ "$ifNull": ["$failure_count", 0_i64] }, 1_i64] },
            ]
        },
        "last_failed_at": now_bson,
        "reason": bson_literal_str(&pending.reason),
        "source_kind": pending
            .source_kind
            .as_deref()
            .map(bson_literal_str)
            .unwrap_or(Bson::Null),
        "failure_code": bson_literal_str(&pending.failure_code),
        "message": pending
            .message
            .as_deref()
            .map(bson_literal_str)
            .unwrap_or(Bson::Null),
    };
    if let Some(encounter_id) = pending.encounter_id {
        stage1.insert(
            "encounter_id",
            doc! { "$ifNull": ["$encounter_id", encounter_id as i32] },
        );
    }

    let threshold_i64 = i64::from(threshold.max(1));
    let final_expr: Bson = if can_become_final {
        doc! { "$gte": ["$failure_count", threshold_i64] }.into()
    } else {
        Bson::Boolean(false)
    };

    vec![
        doc! { "$set": stage1 },
        doc! { "$set": { "final_for_processor": final_expr } },
    ]
}

pub(super) async fn upsert_failure_states_batch<K>(
    collection: Collection<Document>,
    entries: &[(String, &PendingFailureState<K>)],
    threshold: u32,
    should_record: fn(&str) -> bool,
    legacy_no_data_codes: Option<&[&str]>,
) -> anyhow::Result<crate::mongo::BatchUpdateSummary> {
    if entries.is_empty() {
        return Ok(crate::mongo::BatchUpdateSummary::default());
    }

    let now = Utc::now();
    let batch_items = entries
        .iter()
        .map(|(id, pending)| {
            let can_become_final = should_record(&pending.reason);
            let pipeline = build_failure_state_upsert_pipeline(
                pending,
                now,
                threshold,
                can_become_final,
                legacy_no_data_codes,
            );
            crate::mongo::BatchUpdateItem::upsert_pipeline(doc! { "_id": id }, pipeline)
        })
        .collect::<Vec<_>>();

    crate::mongo::run_batched_updates(&collection, &batch_items, true).await
}

#[cfg(test)]
mod tests {
    use warp::http::{HeaderMap, HeaderValue};

    use super::parse_jobs_limit;

    #[test]
    fn parse_jobs_limit_handles_missing_invalid_zero_valid_and_oversized_headers() {
        const HEADER: &str = "x-rpf-test-jobs-limit";
        let max_limit = 20;

        let mut headers = HeaderMap::new();
        assert_eq!(parse_jobs_limit(&headers, HEADER, max_limit), max_limit);

        headers.insert(HEADER, HeaderValue::from_static("0"));
        assert_eq!(parse_jobs_limit(&headers, HEADER, max_limit), max_limit);

        headers.insert(HEADER, HeaderValue::from_static("not-a-number"));
        assert_eq!(parse_jobs_limit(&headers, HEADER, max_limit), max_limit);

        headers.insert(HEADER, HeaderValue::from_static("-4"));
        assert_eq!(parse_jobs_limit(&headers, HEADER, max_limit), max_limit);

        headers.insert(HEADER, HeaderValue::from_static(""));
        assert_eq!(parse_jobs_limit(&headers, HEADER, max_limit), max_limit);

        headers.insert(HEADER, HeaderValue::from_static(" 8 "));
        assert_eq!(parse_jobs_limit(&headers, HEADER, max_limit), 8);

        headers.insert(HEADER, HeaderValue::from_static("999"));
        assert_eq!(parse_jobs_limit(&headers, HEADER, max_limit), max_limit);

        headers.insert(HEADER, HeaderValue::from_static("8"));
        assert_eq!(parse_jobs_limit(&headers, HEADER, 0), 1);
    }

    #[test]
    fn failure_state_upsert_pipeline_collapses_reset_legacy_increment_and_final_flag() {
        use super::{
            FETCH_FAILED_REASON, PendingFailureState, TOMESTONE_NO_DATA_REASON,
            build_failure_state_upsert_pipeline,
        };
        use chrono::{TimeZone, Utc};
        use mongodb::bson::{Bson, doc};

        let now = Utc.with_ymd_and_hms(2026, 11, 18, 5, 0, 0).unwrap();
        let now_bson = mongodb::bson::DateTime::from_chrono(now);
        let pending = PendingFailureState {
            key: 1_u32,
            content_id: 42,
            zone_id: 73,
            difficulty_id: 101,
            encounter_id: Some(97),
            processor_kind: "worker".to_string(),
            reason: TOMESTONE_NO_DATA_REASON.to_string(),
            source_kind: Some("tomestone".to_string()),
            failure_code: "tomestone_requested_encounter_empty".to_string(),
            failure_fingerprint: "fp".to_string(),
            message: Some("$literal_safe_msg".to_string()),
        };
        let legacy_codes = ["tomestone_requested_encounter_empty"];

        let pipeline = build_failure_state_upsert_pipeline(
            &pending,
            now,
            3,
            true,
            Some(&legacy_codes),
        );
        assert_eq!(pipeline.len(), 2);

        let stage1 = pipeline[0].get_document("$set").unwrap();
        assert_eq!(
            stage1.get_document("encounter_id"),
            Ok(&doc! { "$ifNull": ["$encounter_id", 97_i32] })
        );
        assert_eq!(
            stage1.get_datetime("last_failed_at").unwrap(),
            &now_bson
        );
        assert_eq!(
            stage1.get_document("message"),
            Ok(&doc! { "$literal": "$literal_safe_msg" })
        );
        let count_cond = stage1.get_document("failure_count").unwrap().get_array("$cond").unwrap();
        let reset_or = count_cond[0].as_document().unwrap().get_array("$or").unwrap();
        assert_eq!(reset_or.len(), 2);
        let legacy_and = reset_or[1].as_document().unwrap().get_array("$and").unwrap();
        assert_eq!(
            legacy_and[0].as_document().unwrap(),
            &doc! { "$eq": ["$reason", FETCH_FAILED_REASON] }
        );
        assert_eq!(count_cond[1], Bson::Int64(1));

        let stage2 = pipeline[1].get_document("$set").unwrap();
        assert_eq!(
            stage2.get_document("final_for_processor"),
            Ok(&doc! { "$gte": ["$failure_count", 3_i64] })
        );
    }
}
