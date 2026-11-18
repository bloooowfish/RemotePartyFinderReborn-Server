use std::{convert::Infallible, sync::Arc};

use warp::{
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, ETAG, VARY},
    },
    hyper::Body,
    Reply,
};

use crate::{ffxiv::Language, web::State};

use super::snapshot::{accepts_gzip, if_none_match_matches};

const STATS_CACHE_CONTROL: &str = "public, max-age=60, stale-while-revalidate=300";
const STATS_VARY: &str = "Accept-Encoding, Accept-Language, Cookie";

pub async fn stats_json_handler(
    state: Arc<State>,
    headers: HeaderMap,
    seven_days: bool,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let lang = Language::from_codes(language_codes(&headers).as_deref());
    let stats = state.stats.read().await.clone();

    Ok(match stats {
        Some(stats) => {
            let artifacts = if seven_days {
                &stats.seven_days_artifacts
            } else {
                &stats.all_time_artifacts
            };
            let Some(prebuilt) = artifacts.get(&lang).or_else(|| artifacts.get(&Language::English))
            else {
                return Ok(warp::reply::with_status(
                    "stats artifacts are not available",
                    StatusCode::SERVICE_UNAVAILABLE,
                )
                .into_response());
            };

            let accept_encoding = headers
                .get(warp::http::header::ACCEPT_ENCODING)
                .and_then(|value| value.to_str().ok());
            let if_none_match = headers
                .get(warp::http::header::IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok());
            let wants_gzip = accepts_gzip(accept_encoding);
            let response_etag = if wants_gzip {
                &prebuilt.etag_gzip
            } else {
                &prebuilt.etag
            };

            if if_none_match_matches(if_none_match, Some(response_etag.as_str())) {
                let mut response = warp::http::Response::new(Body::empty());
                *response.status_mut() = StatusCode::NOT_MODIFIED;
                response
                    .headers_mut()
                    .insert(CACHE_CONTROL, HeaderValue::from_static(STATS_CACHE_CONTROL));
                response
                    .headers_mut()
                    .insert(VARY, HeaderValue::from_static(STATS_VARY));
                if let Ok(value) = HeaderValue::from_str(response_etag) {
                    response.headers_mut().insert(ETAG, value);
                }
                return Ok(response);
            }

            let body = if wants_gzip {
                prebuilt.body_gzip.clone()
            } else {
                prebuilt.body.clone()
            };
            let mut response = warp::http::Response::new(Body::from(body));
            *response.status_mut() = StatusCode::OK;
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
            response
                .headers_mut()
                .insert(CACHE_CONTROL, HeaderValue::from_static(STATS_CACHE_CONTROL));
            response
                .headers_mut()
                .insert(VARY, HeaderValue::from_static(STATS_VARY));
            if let Ok(value) = HeaderValue::from_str(response_etag) {
                response.headers_mut().insert(ETAG, value);
            }
            if wants_gzip {
                response
                    .headers_mut()
                    .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
            }
            response
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
