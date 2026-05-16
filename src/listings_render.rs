use std::{
    cmp::Ordering as CmpOrdering,
    collections::HashMap,
    future::Future,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
    time::Duration,
};

use crate::fflogs::PhaseProgress;
use crate::mongo::{
    get_current_listings, get_parse_docs, get_players_by_content_ids, ParseCacheDoc,
};
use crate::sestring_ext::SeStringExt;
use crate::{ffxiv::Language, listing_container::QueriedListing, web::State};

const LISTINGS_PLAYER_LOOKUP_TIMEOUT: Duration = Duration::from_millis(1_500);
const LISTINGS_PARSE_DOC_LOOKUP_TIMEOUT: Duration = Duration::from_millis(1_500);
const LISTINGS_REPORT_PARSE_LOOKUP_TIMEOUT: Duration = Duration::from_millis(1_500);

fn is_known_member_name(name: &str) -> bool {
    let trimmed = name.trim();
    !trimmed.is_empty()
        && !trimmed.eq_ignore_ascii_case("Unknown Member")
        && !trimmed.eq_ignore_ascii_case("Party Leader")
}

fn should_lookup_member_fflogs_displays(player: &crate::player::Player) -> bool {
    player.home_world != 0 && is_known_member_name(&player.name)
}

fn encode_fflogs_path_segment(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut out = String::with_capacity(input.len());
    for b in input.as_bytes() {
        let b = *b;
        let is_unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if is_unreserved {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[(b >> 4) as usize]));
            out.push(char::from(HEX[(b & 0x0F) as usize]));
        }
    }

    out
}

#[derive(Debug)]
pub struct RenderableListing {
    pub container: QueriedListing,
    pub members: Vec<RenderableMember>,
    pub leader_parse: ParseDisplay,
}

impl std::ops::Deref for RenderableListing {
    type Target = QueriedListing;

    fn deref(&self) -> &Self::Target {
        &self.container
    }
}

#[derive(Debug, Clone, Default)]
pub struct ParseDisplay {
    pub primary_percentile: Option<u8>,
    pub primary_color_class: String,
    pub secondary_percentile: Option<u8>,
    pub secondary_color_class: String,
    pub has_secondary: bool,
    pub hidden: bool,
    pub originally_hidden: bool,
    pub estimated: bool,
    pub source: crate::parse_resolver::ParseSource,
}

impl ParseDisplay {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        p1: Option<u8>,
        p1_class: String,
        p2: Option<u8>,
        p2_class: String,
        has_secondary: bool,
        hidden: bool,
        originally_hidden: bool,
        estimated: bool,
        source: crate::parse_resolver::ParseSource,
    ) -> Self {
        Self {
            primary_percentile: p1,
            primary_color_class: p1_class,
            secondary_percentile: p2,
            secondary_color_class: p2_class,
            has_secondary,
            hidden,
            originally_hidden,
            estimated,
            source,
        }
    }

    fn none() -> Self {
        Self::new(
            None,
            "parse-none".to_string(),
            None,
            "parse-none".to_string(),
            false,
            false,
            false,
            false,
            crate::parse_resolver::ParseSource::None,
        )
    }

    fn from_resolved(resolved: &crate::parse_resolver::ResolvedParseData) -> Self {
        Self::new(
            resolved.primary_percentile,
            resolved.primary_color_class.clone(),
            resolved.secondary_percentile,
            resolved.secondary_color_class.clone(),
            resolved.has_secondary,
            resolved.hidden,
            resolved.originally_hidden,
            resolved.estimated,
            resolved.source,
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProgressDisplay {
    pub final_boss_percentage: Option<f32>,
    pub final_clear_count: Option<u16>,
    pub phase_progress: Vec<PhaseProgress>,
}

impl ProgressDisplay {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        p1_boss: Option<f32>,
        p2_boss: Option<f32>,
        p1_percentile: Option<u8>,
        p2_percentile: Option<u8>,
        p1_clear_count: Option<u16>,
        p2_clear_count: Option<u16>,
        p1_phase_progress: Vec<PhaseProgress>,
        p2_phase_progress: Vec<PhaseProgress>,
        has_secondary: bool,
        hidden: bool,
    ) -> Self {
        if hidden {
            return Self {
                final_boss_percentage: None,
                final_clear_count: None,
                phase_progress: Vec::new(),
            };
        }

        let (final_boss_raw, final_percentile, final_clear_raw, phase_progress) = if has_secondary {
            (p2_boss, p2_percentile, p2_clear_count, p2_phase_progress)
        } else {
            (p1_boss, p1_percentile, p1_clear_count, p1_phase_progress)
        };

        let final_clear_count = final_clear_raw.filter(|v| *v > 0);
        let final_boss_percentage = if final_percentile.is_some()
            || final_clear_count.is_some()
            || !phase_progress.is_empty()
        {
            None
        } else {
            match final_boss_raw {
                Some(v) if v <= 0.0 => None,
                Some(v) => Some(v),
                None => None,
            }
        };

        Self {
            final_boss_percentage,
            final_clear_count,
            phase_progress,
        }
    }
}

#[derive(Debug)]
pub struct RenderableMember {
    pub job_id: u8,
    pub player: crate::player::Player,
    pub parse: ParseDisplay,
    pub progress: ProgressDisplay,
    pub slot_index: usize,
    pub party_index: u8,
    pub identity_fallback: bool,
}

impl RenderableMember {
    pub fn job_code(&self) -> Option<&'static str> {
        crate::ffxiv::JOBS
            .get(&(self.job_id as u32))
            .map(|cj| cj.code())
    }

    pub fn role_class(&self) -> &'static str {
        use ffxiv_types::Role;
        if let Some(cj) = crate::ffxiv::JOBS.get(&(self.job_id as u32)) {
            match cj.role() {
                Some(Role::Tank) => "tank",
                Some(Role::Healer) => "healer",
                Some(Role::Dps) => "dps",
                None => "",
            }
        } else {
            ""
        }
    }

    pub fn fflogs_character_url(&self) -> Option<String> {
        if self.identity_fallback {
            return None;
        }

        let name = self.player.name.trim();
        if !is_known_member_name(name) {
            return None;
        }

        let world = crate::ffxiv::WORLDS.get(&(self.player.home_world as u32))?;
        let server = world.as_str();
        let region = crate::fflogs::get_region_from_server(server).to_ascii_lowercase();

        Some(format!(
            "https://www.fflogs.com/character/{}/{}/{}",
            region,
            encode_fflogs_path_segment(server),
            encode_fflogs_path_segment(name),
        ))
    }
}

async fn bounded_listing_lookup<T, Fut>(lookup: &'static str, timeout: Duration, future: Fut) -> T
where
    T: Default,
    Fut: Future<Output = anyhow::Result<T>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            tracing::warn!(lookup, error = ?error, "listing enrichment lookup failed");
            T::default()
        }
        Err(_) => {
            tracing::warn!(
                lookup,
                timeout_ms = timeout.as_millis(),
                "listing enrichment lookup timed out"
            );
            T::default()
        }
    }
}

fn resolve_member_player(
    players: &HashMap<u64, crate::player::Player>,
    uid: u64,
    is_leader_member: bool,
    leader_name: &str,
    leader_home_world: u16,
    leader_current_world: u16,
) -> (crate::player::Player, bool) {
    if let Some(player) = players.get(&uid) {
        return (player.clone(), false);
    }

    if is_leader_member {
        let leader_name = if leader_name.trim().is_empty() {
            "Party Leader".to_string()
        } else {
            leader_name.to_string()
        };

        return (
            crate::player::Player {
                content_id: uid,
                name: leader_name,
                home_world: leader_home_world,
                current_world: leader_current_world,
                last_seen: chrono::Utc::now(),
                seen_count: 0,
                account_id: "-1".to_string(),
            },
            true,
        );
    }

    (
        crate::player::Player {
            content_id: uid,
            name: "Unknown Member".to_string(),
            home_world: 0,
            current_world: 0,
            last_seen: chrono::Utc::now(),
            seen_count: 0,
            account_id: "-1".to_string(),
        },
        false,
    )
}

/// FFLogs 표시 데이터 조회 헬퍼
///
/// - Parse percentile (Best)
/// - Progress (boss remaining HP %)
/// - Hidden 처리
#[allow(clippy::too_many_arguments)]
fn lookup_fflogs_displays(
    parse_docs: &HashMap<u64, ParseCacheDoc>,
    report_parse_summaries: Option<
        &HashMap<crate::mongo::ReportParseIdentityKey, crate::mongo::ReportParseZoneSummary>,
    >,
    content_id: u64,
    fallback_name: Option<&str>,
    fallback_home_world: Option<u16>,
    zone_id: u32,
    zone_key: &str,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> (ParseDisplay, ProgressDisplay) {
    let plugin_zone_cache = parse_docs
        .get(&content_id)
        .and_then(|doc| crate::fflogs::get_compatible_zone(&doc.zones, zone_key));
    let fallback_summary = match (report_parse_summaries, fallback_name, fallback_home_world) {
        (Some(summaries), Some(name), Some(home_world)) if home_world != 0 => {
            summaries.get(&crate::mongo::ReportParseIdentityKey::new(name, home_world))
        }
        _ => None,
    };
    let resolved = crate::parse_resolver::resolve_parse_data(
        plugin_zone_cache,
        fallback_summary.map(|summary| &summary.encounters),
        fallback_summary.map(|summary| summary.updated_at),
        zone_id,
        encounter_id,
        secondary_encounter_id,
    );

    (
        ParseDisplay::from_resolved(&resolved),
        ProgressDisplay::new(
            resolved.primary_boss_percentage,
            resolved.secondary_boss_percentage,
            resolved.primary_percentile,
            resolved.secondary_percentile,
            resolved.primary_clear_count,
            resolved.secondary_clear_count,
            resolved.primary_phase_progress,
            resolved.secondary_phase_progress,
            resolved.has_secondary,
            resolved.hidden,
        ),
    )
}

pub(crate) async fn build_renderable_listings(state: Arc<State>) -> Vec<RenderableListing> {
    let lang = Language::English;

    let res = get_current_listings(state.collection()).await;
    match res {
        Ok(mut containers) => {
            // 단일 정렬로 통합: updated_minute DESC → pf_category DESC → time_left ASC
            containers.sort_by(|a, b| {
                b.updated_minute
                    .cmp(&a.updated_minute)
                    .then_with(|| b.listing.pf_category().cmp(&a.listing.pf_category()))
                    .then_with(|| {
                        a.time_left
                            .partial_cmp(&b.time_left)
                            .unwrap_or(CmpOrdering::Equal)
                    })
            });

            // Collect all member IDs + leader IDs
            let mut all_content_ids: Vec<u64> = containers
                .iter()
                .flat_map(|l| {
                    let member_ids = l.listing.member_content_ids.iter().map(|&id| id as u64);
                    let leader_id = std::iter::once(l.listing.leader_content_id);
                    member_ids.chain(leader_id)
                })
                .filter(|&id| id != 0)
                .collect();
            all_content_ids.sort_unstable();
            all_content_ids.dedup();

            // Fetch players; if enrichment is slow, keep the listings visible with fallbacks.
            let players_list: Vec<crate::player::Player> = bounded_listing_lookup(
                "players",
                LISTINGS_PLAYER_LOOKUP_TIMEOUT,
                get_players_by_content_ids(state.players_collection(), &all_content_ids),
            )
            .await;
            let players: HashMap<u64, crate::player::Player> = players_list
                .into_iter()
                .map(|p| (p.content_id, p))
                .collect();

            // Optimisation: Pre-fetch all parse docs for all visible players.
            let all_parse_docs: HashMap<u64, ParseCacheDoc> = bounded_listing_lookup(
                "parse docs",
                LISTINGS_PARSE_DOC_LOOKUP_TIMEOUT,
                get_parse_docs(state.parse_collection(), &all_content_ids),
            )
            .await;
            let mut report_parse_requests: HashMap<
                String,
                Vec<crate::mongo::ReportParseIdentityKey>,
            > = HashMap::new();

            for container in &containers {
                let duty_id = container.listing.duty;
                let Some(info) = (if container.listing.high_end() {
                    crate::fflogs::mapping::get_fflogs_encounter(duty_id)
                } else {
                    None
                }) else {
                    continue;
                };

                let zone_key = crate::fflogs::make_zone_cache_key(
                    info.zone_id,
                    info.difficulty_id.unwrap_or(0) as i32,
                );
                let leader_name_text = container.listing.name.full_text(&lang);

                if container.listing.home_world != 0 && is_known_member_name(&leader_name_text) {
                    report_parse_requests
                        .entry(zone_key.clone())
                        .or_default()
                        .push(crate::mongo::ReportParseIdentityKey::new(
                            &leader_name_text,
                            container.listing.home_world,
                        ));
                }

                for (i, member_id) in container.listing.member_content_ids.iter().enumerate() {
                    let uid = *member_id as u64;
                    if uid == 0 {
                        continue;
                    }

                    let is_leader_member = uid == container.listing.leader_content_id || i == 0;
                    let (player, _) = resolve_member_player(
                        &players,
                        uid,
                        is_leader_member,
                        &leader_name_text,
                        container.listing.home_world,
                        container.listing.current_world,
                    );

                    if should_lookup_member_fflogs_displays(&player) {
                        report_parse_requests
                            .entry(zone_key.clone())
                            .or_default()
                            .push(crate::mongo::ReportParseIdentityKey::new(
                                &player.name,
                                player.home_world,
                            ));
                    }
                }
            }

            let report_parse_summary_collection = state.report_parse_summary_collection();
            let all_report_parse_summaries: HashMap<
                String,
                HashMap<crate::mongo::ReportParseIdentityKey, crate::mongo::ReportParseZoneSummary>,
            > = bounded_listing_lookup(
                "report parse summaries",
                LISTINGS_REPORT_PARSE_LOOKUP_TIMEOUT,
                async move {
                    let mut all_report_parse_summaries = HashMap::new();
                    for (zone_key, identities) in report_parse_requests {
                        let summaries = crate::mongo::get_report_parse_summaries_by_zone(
                            report_parse_summary_collection.clone(),
                            &zone_key,
                            &identities,
                        )
                        .await
                        .unwrap_or_default();
                        all_report_parse_summaries.insert(zone_key, summaries);
                    }
                    Ok(all_report_parse_summaries)
                },
            )
            .await;

            // Match players to listings with job info
            let mut renderable_containers = Vec::new();
            let mut leader_fallback_total = 0usize;
            let mut leader_fallback_listing_count = 0usize;

            for container in containers {
                // Determine FFLogs Zone ID/Encounter ID
                let duty_id = container.listing.duty;
                let high_end = container.listing.high_end();
                let fflogs_info = if high_end {
                    crate::fflogs::mapping::get_fflogs_encounter(duty_id)
                } else {
                    None
                };

                let (zone_id, difficulty_id, encounter_id, secondary_encounter_id) =
                    if let Some(info) = fflogs_info {
                        (
                            info.zone_id,
                            info.difficulty_id.unwrap_or(0) as i32,
                            info.encounter_id,
                            info.secondary_encounter_id,
                        )
                    } else {
                        (0, 0, 0, None)
                    };

                let jobs = &container.listing.jobs_present;
                let detail_jobs = &container.listing.member_jobs;
                let content_ids = &container.listing.member_content_ids;
                let leader_name_text = container.listing.name.full_text(&lang);
                let zone_key = crate::fflogs::make_zone_cache_key(zone_id, difficulty_id);

                let mut listing_leader_fallback_hits = 0usize;
                let members: Vec<RenderableMember> = content_ids
                    .iter()
                    .enumerate()
                    .filter(|(_, id)| **id != 0) // 빈 슬롯 제외
                    .map(|(i, id)| {
                        let uid = *id as u64;
                        let party_index = (i / 8) as u8;
                        let job_id = detail_jobs
                            .get(i)
                            .copied()
                            .or_else(|| jobs.get(i).copied())
                            .unwrap_or(0);
                        let is_leader_member = uid == container.listing.leader_content_id || i == 0;

                        let (player, used_leader_fallback) = resolve_member_player(
                            &players,
                            uid,
                            is_leader_member,
                            &leader_name_text,
                            container.listing.home_world,
                            container.listing.current_world,
                        );
                        if used_leader_fallback {
                            listing_leader_fallback_hits += 1;
                        }

                        let (parse, progress) = if zone_id > 0
                            && should_lookup_member_fflogs_displays(&player)
                        {
                            lookup_fflogs_displays(
                                &all_parse_docs,
                                all_report_parse_summaries.get(&zone_key),
                                uid,
                                Some(&player.name),
                                Some(player.home_world),
                                zone_id,
                                &zone_key,
                                encounter_id,
                                secondary_encounter_id,
                            )
                        } else {
                            (
                                ParseDisplay::none(),
                                ProgressDisplay::new(
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    Vec::new(),
                                    Vec::new(),
                                    false,
                                    false,
                                ),
                            )
                        };

                        RenderableMember {
                            job_id,
                            player,
                            parse,
                            progress,
                            slot_index: i,
                            party_index,
                            identity_fallback: used_leader_fallback,
                        }
                    })
                    .collect();

                if listing_leader_fallback_hits > 0 {
                    leader_fallback_total += listing_leader_fallback_hits;
                    leader_fallback_listing_count += 1;

                    tracing::debug!(
                        listing_id = container.listing.id,
                        leader_content_id = container.listing.leader_content_id,
                        fallback_hits = listing_leader_fallback_hits,
                        "members: applied leader metadata fallback",
                    );
                }

                // 파티장 로그 계산 (leader_content_id 사용) - 헬퍼 함수 사용
                let leader_content_id = container.listing.leader_content_id;
                let (leader_parse, _) = if zone_id > 0
                    && leader_content_id != 0
                    && container.listing.home_world != 0
                    && is_known_member_name(&leader_name_text)
                {
                    lookup_fflogs_displays(
                        &all_parse_docs,
                        all_report_parse_summaries.get(&zone_key),
                        leader_content_id,
                        Some(&leader_name_text),
                        Some(container.listing.home_world),
                        zone_id,
                        &zone_key,
                        encounter_id,
                        secondary_encounter_id,
                    )
                } else {
                    (
                        ParseDisplay::none(),
                        ProgressDisplay::new(
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            Vec::new(),
                            Vec::new(),
                            false,
                            false,
                        ),
                    )
                };

                renderable_containers.push(RenderableListing {
                    container,
                    members,
                    leader_parse,
                });
            }

            if leader_fallback_total > 0 {
                let batch_fallback_hits = leader_fallback_total as u64;
                let total_fallback_hits = state
                    .fflogs_leader_fallback_total
                    .fetch_add(batch_fallback_hits, AtomicOrdering::Relaxed)
                    + batch_fallback_hits;

                tracing::info!(
                    listing_count = leader_fallback_listing_count,
                    fallback_hits = leader_fallback_total,
                    cumulative_fallback_hits = total_fallback_hits,
                    "members: leader metadata fallback used in listings response",
                );
            }

            renderable_containers
        }
        Err(e) => {
            tracing::error!("Failed to get listings: {:#?}", e);
            Default::default()
        }
    }
}

#[cfg(test)]
mod tests;
