use crate::ffxiv::Language;
use crate::listing::{DutyCategory, DutyType};
use crate::web::State;
use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::TryStreamExt;
use mongodb::{
    Collection,
    bson::{Document, doc},
    options::AggregateOptions,
};
use serde::{Deserialize, de::DeserializeOwned};
use sestring::SeString;
use std::borrow::Cow;
use std::collections::HashMap;

const DUTIES_STATS_LIMIT: i64 = 1000;
const HOSTS_STATS_LIMIT: i64 = 200;
const HOURS_STATS_LIMIT: i64 = 24;
const DAYS_STATS_LIMIT: i64 = 7;

#[derive(Debug, Clone, Deserialize)]
pub struct CachedStatistics {
    pub all_time: Statistics,
    pub seven_days: Statistics,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Statistics {
    pub count: Vec<Count>,
    #[serde(default)]
    pub aliases: HashMap<u32, Alias>,
    pub duties: Vec<DutyInfo>,
    pub hosts: Vec<HostInfo>,
    pub hours: Vec<HourInfo>,
    pub days: Vec<DayInfo>,
}

impl Statistics {
    pub fn num_listings(&self) -> usize {
        if self.count.is_empty() {
            return 0;
        }

        self.count[0].count
    }

    pub fn player_name(&self, cid: &u32) -> Cow<'_, str> {
        let alias = match self.aliases.get(cid) {
            Some(a) => a,
            None => return "<unknown>".into(),
        };

        let world = match crate::ffxiv::WORLDS.get(&alias.home_world) {
            Some(world) => world.name(),
            None => "<unknown>",
        };

        format!("{} @ {}", alias.name.text(), world).into()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Count {
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AliasInfo {
    #[serde(rename = "_id")]
    pub content_id: u32,
    pub alias: Alias,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Alias {
    #[serde(with = "crate::base64_sestring")]
    pub name: SeString,
    pub home_world: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DutyInfo {
    #[serde(rename = "_id")]
    pub info: (u8, u32, u16),
    pub count: usize,
}

impl DutyInfo {
    pub fn name(&self, lang: &Language) -> Cow<'_, str> {
        let kind = match DutyType::from_u8(self.info.0) {
            Some(k) => k,
            None => return Cow::from("<unknown>"),
        };
        let category = match DutyCategory::from_u32(self.info.1) {
            Some(c) => c,
            None => return Cow::from("<unknown>"),
        };
        crate::ffxiv::duty_name(kind, category, self.info.2, *lang)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HostInfo {
    #[serde(rename = "_id")]
    pub created_world: u32,
    pub count: usize,
    pub content_ids: Vec<HostInfoInfo>,
}

impl HostInfo {
    pub fn num_other(&self) -> usize {
        let top15: usize = self.content_ids.iter().map(|info| info.count).sum();
        self.count.saturating_sub(top15)
    }

    pub fn world_name(&self) -> &'static str {
        match crate::ffxiv::WORLDS.get(&self.created_world) {
            Some(world) => world.name(),
            None => "<unknown>",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HostInfoInfo {
    pub content_id: u32,
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HourInfo {
    #[serde(rename = "_id")]
    pub hour: u8,
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DayInfo {
    #[serde(rename = "_id")]
    pub day: u8,
    pub count: usize,
}

impl DayInfo {
    pub fn name(&self) -> &'static str {
        match self.day {
            1 => "Sunday",
            2 => "Monday",
            3 => "Tuesday",
            4 => "Wednesday",
            5 => "Thursday",
            6 => "Friday",
            7 => "Saturday",
            _ => "<unknown>",
        }
    }
}

fn base_stats_match_stages(since: Option<DateTime<Utc>>) -> Vec<Document> {
    let mut stages = Vec::with_capacity(2);
    if let Some(since) = since {
        stages.push(doc! {
            "$match": {
                "created_at": {
                    "$gte": mongodb::bson::DateTime::from_chrono(since),
                },
            },
        });
    }
    // Filter out private PFs.
    stages.push(doc! {
        "$match": {
            "listing.search_area": { "$bitsAllClear": 2 },
        },
    });
    stages
}

fn count_pipeline(since: Option<DateTime<Utc>>) -> Vec<Document> {
    let mut pipeline = base_stats_match_stages(since);
    pipeline.push(doc! {
        "$count": "count",
    });
    pipeline
}

fn duties_pipeline(since: Option<DateTime<Utc>>) -> Vec<Document> {
    let mut pipeline = base_stats_match_stages(since);
    pipeline.extend([
        doc! {
            "$group": {
                "_id": [
                    "$listing.duty_type",
                    "$listing.category",
                    "$listing.duty",
                ],
                "count": { "$sum": 1 },
            },
        },
        doc! {
            "$sort": { "count": -1 },
        },
        doc! {
            "$limit": DUTIES_STATS_LIMIT,
        },
    ]);
    pipeline
}

fn hosts_pipeline(since: Option<DateTime<Utc>>) -> Vec<Document> {
    let mut pipeline = base_stats_match_stages(since);
    pipeline.extend([
        doc! {
            "$group": {
                "_id": {
                    "world": "$listing.created_world",
                    "content_id": "$listing.content_id_lower",
                },
                "count": { "$sum": 1 },
            },
        },
        doc! {
            "$sort": { "count": -1 },
        },
        doc! {
            "$group": {
                "_id": "$_id.world",
                "count": { "$sum": "$count" },
                "content_ids": {
                    "$push": {
                        "content_id": "$_id.content_id",
                        "count": "$count",
                    },
                },
            },
        },
        doc! {
            "$addFields": {
                "content_ids": {
                    "$slice": ["$content_ids", 0, 15],
                },
            },
        },
        doc! {
            "$sort": { "count": -1 },
        },
        doc! {
            "$limit": HOSTS_STATS_LIMIT,
        },
    ]);
    pipeline
}

fn hours_pipeline(since: Option<DateTime<Utc>>) -> Vec<Document> {
    let mut pipeline = base_stats_match_stages(since);
    pipeline.extend([
        doc! {
            "$group": {
                "_id": {
                    "$hour": "$created_at",
                },
                "count": { "$sum": 1 },
            },
        },
        doc! {
            "$sort": { "_id": 1 },
        },
        doc! {
            "$limit": HOURS_STATS_LIMIT,
        },
    ]);
    pipeline
}

fn days_pipeline(since: Option<DateTime<Utc>>) -> Vec<Document> {
    let mut pipeline = base_stats_match_stages(since);
    pipeline.extend([
        doc! {
            "$group": {
                "_id": {
                    "$dayOfWeek": "$created_at",
                },
                "count": { "$sum": 1 },
            },
        },
        doc! {
            "$sort": { "_id": 1 },
        },
        doc! {
            "$limit": DAYS_STATS_LIMIT,
        },
    ]);
    pipeline
}

fn aliases_pipeline(ids: &[u32]) -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "listing.content_id_lower": {
                    "$in": ids,
                },
            },
        },
        doc! {
            "$sort": {
                "created_at": -1,
            },
        },
        doc! {
            "$group": {
                "_id": "$listing.content_id_lower",
                "alias": {
                    "$first": {
                        "name": "$listing.name",
                        "home_world": "$listing.home_world",
                    },
                },
            },
        },
        doc! {
            "$limit": ids.len().max(1) as i64,
        },
    ]
}

async fn run_aggregation<T: DeserializeOwned>(
    collection: &Collection<Document>,
    pipeline: Vec<Document>,
) -> Result<Vec<T>> {
    let cursor = collection
        .aggregate(
            pipeline,
            AggregateOptions::builder().allow_disk_use(true).build(),
        )
        .await?;

    cursor
        .try_collect::<Vec<Document>>()
        .await?
        .into_iter()
        .map(mongodb::bson::from_document::<T>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub async fn get_stats(state: &State) -> Result<Statistics> {
    get_stats_internal(state, None).await
}

pub async fn get_stats_seven_days(state: &State) -> Result<Statistics> {
    let last_week = Utc::now() - TimeDelta::try_days(7).unwrap();
    get_stats_internal(state, Some(last_week)).await
}

async fn get_stats_internal(state: &State, since: Option<DateTime<Utc>>) -> Result<Statistics> {
    let history = state.listing_stats_history_collection();
    let (count, duties, hosts, hours, days) = tokio::try_join!(
        run_aggregation::<Count>(&history, count_pipeline(since)),
        run_aggregation::<DutyInfo>(&history, duties_pipeline(since)),
        run_aggregation::<HostInfo>(&history, hosts_pipeline(since)),
        run_aggregation::<HourInfo>(&history, hours_pipeline(since)),
        run_aggregation::<DayInfo>(&history, days_pipeline(since)),
    )?;

    let mut ids: Vec<u32> = hosts
        .iter()
        .flat_map(|host| host.content_ids.iter().map(|entry| entry.content_id))
        .collect();
    ids.sort_unstable();
    ids.dedup();

    let aliases = if ids.is_empty() {
        HashMap::new()
    } else {
        run_aggregation::<AliasInfo>(&history, aliases_pipeline(&ids))
            .await?
            .into_iter()
            .map(|info| (info.content_id, info.alias))
            .collect()
    };

    Ok(Statistics {
        count,
        aliases,
        duties,
        hosts,
        hours,
        days,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duty_info_name_decodes_other_duty_categories_from_client_values() {
        let fate = DutyInfo {
            info: (DutyType::Other as u8, DutyCategory::Fate as u32, u16::MAX),
            count: 1,
        };
        let deep_dungeon = DutyInfo {
            info: (DutyType::Other as u8, DutyCategory::DeepDungeon as u32, 1),
            count: 1,
        };

        assert_eq!(fate.name(&Language::English).as_ref(), "FATEs");
        assert_eq!(
            deep_dungeon.name(&Language::English).as_ref(),
            "The Palace of the Dead"
        );
    }

    #[test]
    fn stats_pipelines_avoid_facet_and_enforce_limits() {
        let now = Utc::now();
        let pipelines = [
            count_pipeline(None),
            duties_pipeline(Some(now)),
            hosts_pipeline(Some(now)),
            hours_pipeline(None),
            days_pipeline(Some(now)),
            aliases_pipeline(&[101, 202]),
        ];

        for pipeline in &pipelines {
            assert!(
                pipeline.iter().all(|stage| !stage.contains_key("$facet")),
                "stats pipelines must not use $facet"
            );
        }

        assert_eq!(
            duties_pipeline(Some(now))
                .first()
                .and_then(|stage| stage.get_document("$match").ok())
                .and_then(|m| m.get("created_at")),
            Some(&mongodb::bson::Bson::Document(doc! {
                "$gte": mongodb::bson::DateTime::from_chrono(now),
            }))
        );
        assert_eq!(
            duties_pipeline(None).last(),
            Some(&doc! { "$limit": DUTIES_STATS_LIMIT })
        );
        assert_eq!(
            hosts_pipeline(None).last(),
            Some(&doc! { "$limit": HOSTS_STATS_LIMIT })
        );
        assert_eq!(
            hours_pipeline(None).last(),
            Some(&doc! { "$limit": HOURS_STATS_LIMIT })
        );
        assert_eq!(
            days_pipeline(None).last(),
            Some(&doc! { "$limit": DAYS_STATS_LIMIT })
        );
        assert_eq!(
            aliases_pipeline(&[101, 202]).last(),
            Some(&doc! { "$limit": 2_i64 })
        );
    }

    #[test]
    fn host_info_num_other_saturates_if_subcounts_exceed_total() {
        let host = HostInfo {
            created_world: 44,
            count: 5,
            content_ids: vec![HostInfoInfo {
                content_id: 1,
                count: 9,
            }],
        };

        assert_eq!(host.num_other(), 0);
    }
}
