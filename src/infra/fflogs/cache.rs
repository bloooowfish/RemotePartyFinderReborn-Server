//! FFLogs Parse 캐시 타입
//!
//! ContentID별 Parse 캐시 데이터 구조를 정의합니다.

use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Zone cache key (zone + difficulty)
///
/// Backward compatibility note:
/// - legacy key was just `zone_id.to_string()`
/// - partitioned legacy key was `{zone_id}:{difficulty_id}:{partition}`
pub fn make_zone_cache_key(zone_id: u32, difficulty_id: i32) -> String {
    format!("{}:{}", zone_id, difficulty_id.max(0))
}

pub fn get_compatible_zone<'a, T>(zones: &'a HashMap<String, T>, zone_key: &str) -> Option<&'a T> {
    zones
        .get(zone_key)
        .or_else(|| {
            let legacy_prefix = format!("{zone_key}:");
            zones
                .iter()
                .find_map(|(key, value)| key.starts_with(&legacy_prefix).then_some(value))
        })
        .or_else(|| {
            zone_key
                .split_once(':')
                .and_then(|(zone_id, _)| zones.get(zone_id))
        })
}

/// FFLogs Parse 캐시 문서 (ContentID당 1개)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseCacheDoc {
    /// 플레이어 ContentId
    pub content_id: i64,
    /// Zone별 캐시 데이터 (key: zone_id as string)
    #[serde(default)]
    pub zones: HashMap<String, ZoneCache>,
}

/// Zone별 캐시 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneCache {
    /// 이 Zone의 조회 시각
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub fetched_at: chrono::DateTime<Utc>,
    /// FFLogs 캐릭터 매칭이 추정(동명이인 후보 탐색) 결과인지 여부
    ///
    /// NOTE: ContentId에 대해 정확한 home_world를 모를 때, name+world 후보를 시도하여
    /// 가장 그럴듯한 캐릭터를 선택한다. 오탐 가능성이 있으므로 UI에서 표시한다.
    #[serde(default)]
    pub estimated: bool,
    /// 추정 매칭에 사용된 서버(월드) slug
    #[serde(default)]
    pub matched_server: Option<String>,
    /// 성공한 parse cache의 수집 경로. 화면 표시는 하지 않고 운영 디버깅용으로만 저장한다.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<String>,
    /// FFLogs에서 해당 캐릭터가 Hidden 처리되어 조회 불가한지 여부
    ///
    /// NOTE: Hidden은 FFLogs의 프라이버시 설정이며, OAuth(User Mode) 없이 우회 불가.
    #[serde(default)]
    pub hidden: bool,
    /// Encounter별 파싱 데이터 (key: encounter_id as string)
    #[serde(default)]
    pub encounters: HashMap<String, EncounterParse>,
}

/// Encounter별 파싱 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncounterParse {
    /// Best Percentile (0-100, -1이면 로그 없음)
    pub percentile: f32,
    /// 직업 ID (0이면 Best Job)
    #[serde(default)]
    pub job_id: u8,
    /// 해당 Encounter의 베스트 진행도(보스 남은 체력 %) (0-100)
    ///
    /// None이면 진행도 데이터를 아직 수집하지 않았거나, 조회할 리포트가 없는 상태.
    #[serde(default)]
    pub boss_percentage: Option<f32>,
    /// 해당 Encounter 클리어 횟수
    #[serde(default)]
    pub clear_count: Option<u32>,
    /// Ultimate encounter phase progress from additive providers such as Tomestone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phase_progress: Vec<PhaseProgress>,
}

/// Encounter phase progress for Ultimate-specific progression display.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PhaseProgress {
    pub phase_key: String,
    pub phase_name: String,
    pub order: u8,
    #[serde(default)]
    pub boss_percentage: Option<f32>,
    #[serde(default)]
    pub display_text: String,
    #[serde(default)]
    pub source: Option<String>,
}

/// Zone 캐시가 만료되었는지 확인 (갱신 기준: 24시간)
pub fn is_zone_cache_expired(zone_cache: &ZoneCache) -> bool {
    is_zone_cache_expired_with_hidden_ttl_hours(zone_cache, 24)
}

/// Zone 캐시가 만료되었는지 확인 (Hidden 캐시 TTL 조정 가능)
pub fn is_zone_cache_expired_with_hidden_ttl_hours(
    zone_cache: &ZoneCache,
    hidden_ttl_hours: i64,
) -> bool {
    // 추정 매칭 캐시는 오탐 가능성이 있으므로, 비교적 빠르게 만료시켜 재평가한다.
    // (특히 hidden 오탐을 영구 캐시하면 치명적)
    if zone_cache.estimated {
        let expire_threshold = Utc::now() - TimeDelta::try_hours(6).unwrap();
        return zone_cache.fetched_at < expire_threshold;
    }

    // Hidden 캐시는 무기한 유지하지 않고, 주기적으로 재확인한다.
    if zone_cache.hidden {
        let ttl = hidden_ttl_hours.max(1);
        let expire_threshold = Utc::now() - TimeDelta::try_hours(ttl).unwrap();
        return zone_cache.fetched_at < expire_threshold;
    }

    let expire_threshold = Utc::now() - TimeDelta::try_hours(24).unwrap();
    zone_cache.fetched_at < expire_threshold
}

/// Active Party Finder dispatch refresh check.
///
/// This intentionally does not change Mongo storage TTL semantics. It only decides whether an
/// active listing should receive a new FFLogs fetch job before the stored cache document expires.
pub fn is_zone_cache_refresh_due_for_active_listing(
    zone_cache: &ZoneCache,
    hidden_ttl_hours: i64,
    active_refresh_minutes: i64,
    now: chrono::DateTime<Utc>,
) -> bool {
    if zone_cache.estimated {
        let refresh_minutes = active_refresh_minutes.max(1);
        let refresh_threshold = now - TimeDelta::try_minutes(refresh_minutes).unwrap();
        return zone_cache.fetched_at < refresh_threshold;
    }

    if zone_cache.hidden {
        let ttl = hidden_ttl_hours.max(1);
        let refresh_threshold = now - TimeDelta::try_hours(ttl).unwrap();
        return zone_cache.fetched_at < refresh_threshold;
    }

    let refresh_minutes = active_refresh_minutes.max(1);
    let refresh_threshold = now - TimeDelta::try_minutes(refresh_minutes).unwrap();
    zone_cache.fetched_at < refresh_threshold
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn zone_cache(fetched_at: chrono::DateTime<Utc>, estimated: bool, hidden: bool) -> ZoneCache {
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
    fn fflogs_active_visible_cache_younger_than_refresh_window_is_not_due() {
        let now = Utc::now();
        let cache = zone_cache(now - TimeDelta::try_minutes(14).unwrap(), false, false);

        assert!(!is_zone_cache_refresh_due_for_active_listing(
            &cache, 24, 15, now
        ));
    }

    #[test]
    fn fflogs_active_visible_cache_older_than_refresh_window_is_due() {
        let now = Utc::now();
        let cache = zone_cache(now - TimeDelta::try_minutes(16).unwrap(), false, false);

        assert!(is_zone_cache_refresh_due_for_active_listing(
            &cache, 24, 15, now
        ));
    }

    #[test]
    fn fflogs_active_hidden_cache_still_uses_hidden_ttl() {
        let now = Utc::now();
        let younger_than_hidden_ttl =
            zone_cache(now - TimeDelta::try_minutes(30).unwrap(), false, true);
        let older_than_hidden_ttl = zone_cache(now - TimeDelta::try_hours(2).unwrap(), false, true);

        assert!(!is_zone_cache_refresh_due_for_active_listing(
            &younger_than_hidden_ttl,
            1,
            15,
            now
        ));
        assert!(is_zone_cache_refresh_due_for_active_listing(
            &older_than_hidden_ttl,
            1,
            15,
            now
        ));
    }

    #[test]
    fn fflogs_active_estimated_cache_uses_active_refresh_window() {
        let now = Utc::now();
        let younger_than_active_refresh =
            zone_cache(now - TimeDelta::try_minutes(14).unwrap(), true, false);
        let older_than_active_refresh =
            zone_cache(now - TimeDelta::try_minutes(16).unwrap(), true, false);

        assert!(!is_zone_cache_refresh_due_for_active_listing(
            &younger_than_active_refresh,
            24,
            15,
            now
        ));
        assert!(is_zone_cache_refresh_due_for_active_listing(
            &older_than_active_refresh,
            24,
            15,
            now
        ));
    }

    #[test]
    fn fflogs_active_hidden_estimated_cache_uses_active_refresh_window() {
        let now = Utc::now();
        let younger_than_active_refresh =
            zone_cache(now - TimeDelta::try_minutes(14).unwrap(), true, true);
        let older_than_active_refresh =
            zone_cache(now - TimeDelta::try_minutes(16).unwrap(), true, true);

        assert!(!is_zone_cache_refresh_due_for_active_listing(
            &younger_than_active_refresh,
            24,
            15,
            now
        ));
        assert!(is_zone_cache_refresh_due_for_active_listing(
            &older_than_active_refresh,
            24,
            15,
            now
        ));
    }
}
