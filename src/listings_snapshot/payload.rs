use crate::listing::{ConditionFlags, ObjectiveFlags, PartyFinderListing, SearchAreaFlags};
use crate::listing_container::QueriedListing;
use crate::listings_render::{
    build_renderable_listings, ParseDisplay, ProgressDisplay, RenderableListing, RenderableMember,
};
use crate::web::State;
use anyhow::Context;
use serde::Serialize;
use sestring::{Payload, SeString};
use sha2::{Digest, Sha256};
use std::sync::Arc;
#[derive(Debug, Serialize)]
pub struct ApiListingsSnapshot {
    pub revision: i64,
    pub listings: Vec<ApiReadableListingContainer>,
}

#[derive(Debug)]
pub struct BuiltListingsPayload {
    pub listings: Vec<ApiReadableListingContainer>,
    pub payload_hash: String,
}

#[derive(Debug)]
pub struct BuiltListingsSnapshot {
    pub revision: i64,
    pub body: Vec<u8>,
    pub etag: String,
    pub payload_hash: String,
}

pub async fn build_listings_payload(state: Arc<State>) -> anyhow::Result<BuiltListingsPayload> {
    let listings = build_renderable_listings(state)
        .await
        .into_iter()
        .map(ApiReadableListingContainer::from_renderable)
        .collect::<Vec<_>>();
    let listings_json =
        serde_json::to_vec(&listings).context("failed to serialize listings payload")?;
    let payload_hash = compute_payload_hash(&listings_json);

    Ok(BuiltListingsPayload {
        listings,
        payload_hash,
    })
}

pub fn serialize_snapshot(
    revision: i64,
    payload: BuiltListingsPayload,
) -> anyhow::Result<BuiltListingsSnapshot> {
    let snapshot = ApiListingsSnapshot {
        revision,
        listings: payload.listings,
    };
    let body = serde_json::to_vec(&snapshot).context("failed to serialize listings snapshot")?;
    let etag = compute_snapshot_etag(&body);

    Ok(BuiltListingsSnapshot {
        revision,
        body,
        etag,
        payload_hash: payload.payload_hash,
    })
}

pub fn compute_snapshot_etag(body: &[u8]) -> String {
    hash_bytes(body)
}

pub fn compute_payload_hash(listings_json: &[u8]) -> String {
    hash_bytes(listings_json)
}

fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!(
        "sha256-{}",
        base64::encode_config(digest, base64::URL_SAFE_NO_PAD)
    )
}

fn sestring_payloads(value: &SeString) -> Vec<ApiSeStringPayload> {
    value
        .0
        .iter()
        .filter_map(|payload| match payload {
            Payload::Text(text) => Some(ApiSeStringPayload::Text {
                text: text.0.clone(),
            }),
            Payload::AutoTranslate(auto_translate) => Some(ApiSeStringPayload::AutoTranslate {
                group: auto_translate.group,
                key: auto_translate.key,
            }),
            _ => None,
        })
        .collect()
}

fn description_badges(listing: &PartyFinderListing) -> (&'static str, Vec<&'static str>) {
    let mut colour_class = "";
    let mut badges = Vec::new();

    if listing.objective.contains(ObjectiveFlags::PRACTICE) {
        badges.push("practice");
        colour_class = "desc-green";
    }

    if listing.objective.contains(ObjectiveFlags::DUTY_COMPLETION) {
        badges.push("duty_completion");
        colour_class = "desc-blue";
    }

    if listing.objective.contains(ObjectiveFlags::LOOT) {
        badges.push("loot");
        colour_class = "desc-yellow";
    }

    if listing.conditions.contains(ConditionFlags::DUTY_COMPLETE) {
        badges.push("duty_complete");
    }

    if listing
        .conditions
        .contains(ConditionFlags::DUTY_COMPLETE_WEEKLY_REWARD_UNCLAIMED)
    {
        badges.push("weekly_reward_unclaimed");
    }

    if listing.conditions.contains(ConditionFlags::DUTY_INCOMPLETE) {
        badges.push("duty_incomplete");
    }

    if listing
        .search_area
        .contains(SearchAreaFlags::ONE_PLAYER_PER_JOB)
    {
        badges.push("one_player_per_job");
    }

    (colour_class, badges)
}

fn role_class_from_class_job(class_job: &ffxiv_types::jobs::ClassJob) -> &'static str {
    use ffxiv_types::Role;

    match class_job.role() {
        Some(Role::Tank) => "tank",
        Some(Role::Healer) => "healer",
        Some(Role::Dps) => "dps",
        None => "",
    }
}

fn build_display_slots(listing: &PartyFinderListing) -> Vec<ApiDisplaySlot> {
    listing
        .slots()
        .into_iter()
        .map(|slot| match slot {
            Ok(class_job) => ApiDisplaySlot {
                filled: true,
                role_class: role_class_from_class_job(&class_job).to_string(),
                title: class_job.code().to_string(),
                icon_code: Some(class_job.code().to_string()),
            },
            Err((role_class, title)) => ApiDisplaySlot {
                filled: false,
                role_class,
                title,
                icon_code: None,
            },
        })
        .collect()
}

/// A render-oriented JSON shape for client-side listings rendering.
#[derive(Debug, Serialize)]
pub struct ApiReadableListingContainer {
    pub time_left_seconds: i64,
    pub updated_at_timestamp: i64,
    pub listing: ApiReadableListing,
}

impl ApiReadableListingContainer {
    pub(crate) fn from_renderable(value: RenderableListing) -> Self {
        let RenderableListing {
            container,
            members,
            leader_parse,
        } = value;
        let QueriedListing {
            created_at: _,
            updated_at,
            updated_minute: _,
            time_left,
            listing,
        } = container;
        let time_left_seconds = time_left as i64;
        let updated_at_timestamp = updated_at.timestamp();

        Self {
            time_left_seconds,
            updated_at_timestamp,
            listing: ApiReadableListing::from_parts(listing, members, leader_parse),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiReadableListing {
    pub creator_name: Vec<ApiSeStringPayload>,
    pub description: Vec<ApiSeStringPayload>,
    pub duty_id: u16,
    pub duty_type: u8,
    pub category: u32,
    pub created_world: ApiReadableWorld,
    pub home_world: ApiReadableWorld,
    pub data_centre: Option<&'static str>,
    pub min_item_level: u16,
    pub num_parties: u8,
    pub slot_count: u8,
    pub slots_filled_count: usize,
    pub high_end: bool,
    pub cross_world: bool,
    pub content_kind: u32,
    pub joinable_roles: u32,
    pub objective_bits: u32,
    pub conditions_bits: u32,
    pub search_area_bits: u32,
    pub description_badge_class: &'static str,
    pub description_badges: Vec<&'static str>,
    pub display_slots: Vec<ApiDisplaySlot>,
    pub members: Vec<ApiReadableMember>,
    pub leader_parse: ApiParseDisplay,
    pub is_alliance_view: bool,
}

impl ApiReadableListing {
    pub(crate) fn from_parts(
        value: PartyFinderListing,
        members: Vec<RenderableMember>,
        leader_parse: ParseDisplay,
    ) -> Self {
        let (description_badge_class, description_badges) = description_badges(&value);
        let is_alliance_view =
            value.num_parties >= 3 || members.iter().any(|member| member.party_index > 0);

        Self {
            creator_name: sestring_payloads(&value.name),
            description: sestring_payloads(&value.description),
            duty_id: value.duty,
            duty_type: value.duty_type as u8,
            category: value.category as u32,
            created_world: value.created_world.into(),
            home_world: value.home_world.into(),
            data_centre: value.data_centre_name(),
            min_item_level: value.min_item_level,
            num_parties: value.num_parties,
            slot_count: value.slots_available,
            slots_filled_count: value.slots_filled(),
            high_end: value.high_end(),
            cross_world: value.is_cross_world(),
            content_kind: value.content_kind(),
            joinable_roles: value.joinable_roles(),
            objective_bits: value.objective.bits(),
            conditions_bits: value.conditions.bits(),
            search_area_bits: value.search_area.bits(),
            description_badge_class,
            description_badges,
            display_slots: build_display_slots(&value),
            members: members.into_iter().map(ApiReadableMember::from).collect(),
            leader_parse: leader_parse.into(),
            is_alliance_view,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiReadableMember {
    pub name: String,
    pub home_world: ApiReadableWorld,
    pub job_id: u8,
    pub job_code: Option<&'static str>,
    pub role_class: &'static str,
    pub parse: ApiParseDisplay,
    pub progress: ApiProgressDisplay,
    pub slot_index: usize,
    pub party_index: u8,
    pub fflogs_character_url: Option<String>,
}

impl From<RenderableMember> for ApiReadableMember {
    fn from(value: RenderableMember) -> Self {
        let job_code = value.job_code();
        let role_class = value.role_class();
        let fflogs_character_url = value.fflogs_character_url();
        let mut parse = ApiParseDisplay::from(value.parse);
        let mut progress = ApiProgressDisplay::from(value.progress);
        if !progress.phase_progress.is_empty() {
            parse.suppress_visible_percentiles();
            progress.final_boss_percentage = None;
        }

        Self {
            name: value.player.name,
            home_world: value.player.home_world.into(),
            job_id: value.job_id,
            job_code,
            role_class,
            parse,
            progress,
            slot_index: value.slot_index,
            party_index: value.party_index,
            fflogs_character_url,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiParseDisplay {
    pub primary_percentile: Option<u8>,
    pub primary_color_class: String,
    pub secondary_percentile: Option<u8>,
    pub secondary_color_class: String,
    pub has_secondary: bool,
    pub hidden: bool,
    pub originally_hidden: bool,
    pub estimated: bool,
}

impl From<ParseDisplay> for ApiParseDisplay {
    fn from(value: ParseDisplay) -> Self {
        Self {
            primary_percentile: value.primary_percentile,
            primary_color_class: value.primary_color_class,
            secondary_percentile: value.secondary_percentile,
            secondary_color_class: value.secondary_color_class,
            has_secondary: value.has_secondary,
            hidden: value.hidden,
            originally_hidden: value.originally_hidden,
            estimated: value.estimated,
        }
    }
}

impl ApiParseDisplay {
    fn suppress_visible_percentiles(&mut self) {
        self.primary_percentile = None;
        self.primary_color_class = "parse-none".to_string();
        self.secondary_percentile = None;
        self.secondary_color_class = "parse-none".to_string();
        self.estimated = false;
    }
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct ApiProgressDisplay {
    pub final_boss_percentage: Option<f32>,
    pub final_clear_count: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phase_progress: Vec<ApiPhaseProgress>,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct ApiPhaseProgress {
    pub phase_key: String,
    pub phase_name: String,
    pub order: u8,
    pub boss_percentage: Option<f32>,
    pub display_text: String,
    pub source: Option<String>,
}

impl From<ProgressDisplay> for ApiProgressDisplay {
    fn from(value: ProgressDisplay) -> Self {
        Self {
            final_boss_percentage: value.final_boss_percentage,
            final_clear_count: value.final_clear_count,
            phase_progress: value
                .phase_progress
                .into_iter()
                .map(ApiPhaseProgress::from)
                .collect(),
        }
    }
}

impl From<crate::fflogs::PhaseProgress> for ApiPhaseProgress {
    fn from(value: crate::fflogs::PhaseProgress) -> Self {
        Self {
            phase_key: value.phase_key,
            phase_name: value.phase_name,
            order: value.order,
            boss_percentage: value.boss_percentage,
            display_text: value.display_text,
            source: value.source,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiSeStringPayload {
    Text { text: String },
    AutoTranslate { group: u8, key: u32 },
}

#[derive(Debug, Serialize)]
pub struct ApiReadableWorld {
    pub name: &'static str,
}

impl From<u16> for ApiReadableWorld {
    fn from(value: u16) -> Self {
        Self {
            name: crate::ffxiv::WORLDS
                .get(&(value as u32))
                .map(|w| w.as_str())
                .unwrap_or("Unknown"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiDisplaySlot {
    pub filled: bool,
    pub role_class: String,
    pub title: String,
    pub icon_code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fflogs::PhaseProgress;

    #[test]
    fn api_progress_display_omits_empty_phase_progress() {
        let value = serde_json::to_value(ApiProgressDisplay::from(ProgressDisplay::default()))
            .expect("progress display should serialize");

        assert!(value.get("phase_progress").is_none());
    }

    #[test]
    fn api_progress_display_serializes_phase_progress_when_present() {
        let value = serde_json::to_value(ApiProgressDisplay::from(ProgressDisplay {
            final_boss_percentage: None,
            final_clear_count: None,
            phase_progress: vec![PhaseProgress {
                phase_key: "p2".to_string(),
                phase_name: "Diamond Dust".to_string(),
                order: 2,
                boss_percentage: Some(52.4),
                display_text: "P2 52.4%".to_string(),
                source: Some("tomestone_api".to_string()),
            }],
        }))
        .expect("progress display should serialize");

        assert_eq!(value["phase_progress"][0]["phase_key"], "p2");
        assert_eq!(value["phase_progress"][0]["phase_name"], "Diamond Dust");
        assert_eq!(value["phase_progress"][0]["order"], 2);
        let boss_percentage = value["phase_progress"][0]["boss_percentage"]
            .as_f64()
            .expect("boss_percentage should be numeric");
        assert!((boss_percentage - 52.4).abs() < 0.001);
        assert_eq!(value["phase_progress"][0]["display_text"], "P2 52.4%");
        assert_eq!(value["phase_progress"][0]["source"], "tomestone_api");
    }

    #[test]
    fn api_member_phase_progress_suppresses_parse_and_boss_percentages() {
        let member = RenderableMember {
            job_id: 19,
            player: crate::player::Player {
                content_id: 1,
                name: "Alice".to_string(),
                home_world: 73,
                current_world: 73,
                last_seen: chrono::Utc::now(),
                seen_count: 1,
                account_id: "-1".to_string(),
            },
            parse: ParseDisplay::new(
                Some(95),
                "parse-pink".to_string(),
                None,
                "parse-none".to_string(),
                false,
                false,
                false,
                true,
                crate::parse_resolver::ParseSource::Plugin,
            ),
            progress: ProgressDisplay {
                final_boss_percentage: Some(76.45),
                final_clear_count: None,
                phase_progress: vec![PhaseProgress {
                    phase_key: "p2".to_string(),
                    phase_name: "P2".to_string(),
                    order: 2,
                    boss_percentage: Some(75.71),
                    display_text: "P2 75.71%".to_string(),
                    source: Some("tomestone_api".to_string()),
                }],
            },
            slot_index: 0,
            party_index: 0,
            identity_fallback: false,
        };

        let value = ApiReadableMember::from(member);

        assert_eq!(value.parse.primary_percentile, None);
        assert_eq!(value.parse.primary_color_class, "parse-none");
        assert_eq!(value.parse.estimated, false);
        assert_eq!(value.progress.final_boss_percentage, None);
        assert_eq!(value.progress.phase_progress[0].display_text, "P2 75.71%");
        assert!(value.fflogs_character_url.is_some());
    }
}
