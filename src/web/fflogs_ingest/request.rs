use warp::http::HeaderMap;

pub(super) use crate::web::parse_ingest_common::{
    WorkerRegion as FflogsWorkerRegion, parse_jobs_limit, parse_processor_kind,
    parse_worker_region,
};
#[cfg(test)]
pub(super) use crate::web::parse_ingest_common::{
    header_equals_ignore_ascii_case, RATE_LIMIT_DEFERRED_REASON,
};

#[cfg(test)]
pub(super) const FFLOGS_JOB_MODE_HEADER: &str = "x-rpf-fflogs-job-mode";
#[cfg(test)]
const FFLOGS_EXACT_ONLY_JOB_MODE: &str = "exact-only";
pub(super) const FFLOGS_JOBS_LIMIT_HEADER: &str = "x-rpf-fflogs-jobs-limit";
pub(super) const FFLOGS_PROCESSOR_KIND_HEADER: &str = "x-rpf-fflogs-processor-kind";
pub(super) const FFLOGS_WORKER_REGION_HEADER: &str = "x-rpf-fflogs-worker-region";
pub(super) const DEFAULT_FFLOGS_PROCESSOR_KIND: &str = "docker_fflogs_worker";

#[cfg(test)]
pub(super) fn fflogs_exact_home_world_only(headers: &HeaderMap) -> bool {
    header_equals_ignore_ascii_case(headers, FFLOGS_JOB_MODE_HEADER, FFLOGS_EXACT_ONLY_JOB_MODE)
}

pub(super) fn fflogs_jobs_limit(headers: &HeaderMap, max_limit: usize) -> usize {
    parse_jobs_limit(headers, FFLOGS_JOBS_LIMIT_HEADER, max_limit)
}

pub(super) fn fflogs_processor_kind(headers: &HeaderMap) -> String {
    parse_processor_kind(
        headers,
        FFLOGS_PROCESSOR_KIND_HEADER,
        DEFAULT_FFLOGS_PROCESSOR_KIND,
    )
}

pub(super) fn fflogs_worker_region(headers: &HeaderMap) -> Option<FflogsWorkerRegion> {
    parse_worker_region(headers, FFLOGS_WORKER_REGION_HEADER)
}
