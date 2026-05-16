use std::{convert::Infallible, sync::Arc};

use serde::Serialize;
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use crate::{ffxiv::Language, stats::Statistics, web::State};

#[derive(Debug, Serialize)]
struct CountRow {
    label: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct HostRow {
    world: String,
    name: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct HourRow {
    hour: u8,
    count: usize,
}

#[derive(Debug, Serialize)]
struct StatsResponse {
    num_listings: usize,
    duties: Vec<CountRow>,
    hosts: Vec<HostRow>,
    hours: Vec<HourRow>,
    days: Vec<CountRow>,
}

fn stats_response(stats: &Statistics, lang: &Language) -> StatsResponse {
    let duties = stats
        .duties
        .iter()
        .map(|info| CountRow {
            label: info.name(lang).into_owned(),
            count: info.count,
        })
        .collect();

    let mut hosts = Vec::new();
    for info in &stats.hosts {
        let world = info.world_name().to_string();
        for entry in &info.content_ids {
            hosts.push(HostRow {
                world: world.clone(),
                name: stats.player_name(&entry.content_id).into_owned(),
                count: entry.count,
            });
        }
        hosts.push(HostRow {
            world,
            name: "Other".to_string(),
            count: info.num_other(),
        });
    }

    let hours = stats
        .hours
        .iter()
        .map(|info| HourRow {
            hour: info.hour,
            count: info.count,
        })
        .collect();

    let days = stats
        .days
        .iter()
        .map(|info| CountRow {
            label: info.name().to_string(),
            count: info.count,
        })
        .collect();

    StatsResponse {
        num_listings: stats.num_listings(),
        duties,
        hosts,
        hours,
        days,
    }
}

pub async fn stats_json_handler(
    state: Arc<State>,
    headers: HeaderMap,
    seven_days: bool,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let lang = Language::from_codes(language_codes(&headers).as_deref());
    let stats = state.stats.read().await.clone();

    Ok(match stats {
        Some(stats) => {
            let selected = if seven_days {
                &stats.seven_days
            } else {
                &stats.all_time
            };
            warp::reply::json(&stats_response(selected, &lang)).into_response()
        }
        None => warp::reply::with_status(
            "stats have not been calculated yet",
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .into_response(),
    })
}

fn language_codes(headers: &HeaderMap) -> Option<String> {
    cookie_lang(headers).or_else(|| {
        headers
            .get("accept-language")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    })
}

fn cookie_lang(headers: &HeaderMap) -> Option<String> {
    let cookie = headers.get("cookie")?.to_str().ok()?;
    cookie.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        (name == "lang").then(|| value.to_string())
    })
}
