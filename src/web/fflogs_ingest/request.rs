use warp::http::HeaderMap;

pub(super) const FFLOGS_JOB_MODE_HEADER: &str = "x-rpf-fflogs-job-mode";
const FFLOGS_EXACT_ONLY_JOB_MODE: &str = "exact-only";
pub(super) const FFLOGS_PROCESSOR_KIND_HEADER: &str = "x-rpf-fflogs-processor-kind";
pub(super) const FFLOGS_WORKER_REGION_HEADER: &str = "x-rpf-fflogs-worker-region";
pub(super) const DEFAULT_FFLOGS_PROCESSOR_KIND: &str = "docker_fflogs_worker";
pub(super) const RATE_LIMIT_DEFERRED_REASON: &str = "worker_rate_limit_deferred";
pub(super) const FAILURE_FIELD_MAX_CHARS: usize = 256;
const FAILURE_MESSAGE_MAX_CHARS: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FflogsWorkerRegion {
    Jp,
    Na,
    Eu,
    Oc,
}

impl FflogsWorkerRegion {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Jp => "JP",
            Self::Na => "NA",
            Self::Eu => "EU",
            Self::Oc => "OC",
        }
    }
}

pub(super) fn fflogs_exact_home_world_only(headers: &HeaderMap) -> bool {
    headers
        .get(FFLOGS_JOB_MODE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .map(|value| value.eq_ignore_ascii_case(FFLOGS_EXACT_ONLY_JOB_MODE))
        .unwrap_or(false)
}

pub(super) fn fflogs_processor_kind(headers: &HeaderMap) -> String {
    headers
        .get(FFLOGS_PROCESSOR_KIND_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(normalize_processor_kind)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_FFLOGS_PROCESSOR_KIND.to_string())
}

pub(super) fn fflogs_worker_region(headers: &HeaderMap) -> Option<FflogsWorkerRegion> {
    let value = headers
        .get(FFLOGS_WORKER_REGION_HEADER)
        .and_then(|value| value.to_str().ok())?
        .trim();

    if value.eq_ignore_ascii_case("JP") {
        Some(FflogsWorkerRegion::Jp)
    } else if value.eq_ignore_ascii_case("NA") {
        Some(FflogsWorkerRegion::Na)
    } else if value.eq_ignore_ascii_case("EU") {
        Some(FflogsWorkerRegion::Eu)
    } else if value.eq_ignore_ascii_case("OC") || value.eq_ignore_ascii_case("OCE") {
        Some(FflogsWorkerRegion::Oc)
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
