use std::collections::HashMap;

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

/// FFLogs 파싱 결과 구조체 (Plugin -> Server request)
#[derive(Debug, serde::Deserialize)]
pub struct ParseResult {
    pub content_id: u64,
    pub zone_id: u32,
    #[serde(default)]
    pub difficulty_id: i32,
    pub encounters: HashMap<i32, f64>,
    #[serde(default)]
    pub boss_percentages: HashMap<i32, f64>,
    #[serde(default)]
    pub clear_counts: HashMap<i32, i32>,
    #[serde(default)]
    pub phase_progress: HashMap<i32, Vec<crate::mongo::PhaseProgress>>,
    #[serde(default)]
    pub is_hidden: bool,
    /// 후보 서버(동명이인) 탐색을 통해 추정 매칭된 결과인지 여부
    #[serde(default)]
    pub is_estimated: bool,
    /// 매칭에 사용된 서버(월드) slug
    #[serde(default)]
    pub matched_server: Option<String>,
    /// 성공한 parse cache의 수집 경로. 화면 표시는 하지 않고 운영 디버깅용으로만 저장한다.
    #[serde(default)]
    pub source_kind: Option<String>,
    /// FFLogs job lease token (jobs 응답과 묶이는 단회성 토큰)
    #[serde(default)]
    pub lease_token: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct AbandonFflogsLease {
    pub content_id: u64,
    pub zone_id: u32,
    #[serde(default)]
    pub difficulty_id: i32,
    #[serde(default)]
    pub lease_token: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub processor_kind: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub failure_code: Option<String>,
    #[serde(default)]
    pub failure_fingerprint: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}
