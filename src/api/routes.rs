use std::{convert::Infallible, sync::Arc};

use crate::config::ListingsSnapshotSource;
use crate::listings_snapshot::load_current_materialized_snapshot;
use crate::web::State;
use warp::filters::BoxedFilter;
use warp::http::{
    header::{HeaderValue, CACHE_CONTROL},
    HeaderMap, StatusCode,
};
use warp::hyper::Body;
use warp::{Filter, Reply};

use super::refresh::{
    authorize_snapshot_refresh_request, refresh_materialized_snapshot_with_loader,
};
use super::snapshot::{
    get_api_snapshot, materialized_snapshot_result_response, snapshot_json_response,
};
use super::stats::stats_json_handler;
use super::ws::ws;
pub fn api(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    warp::path("api")
        .and(
            health()
                .or(ws(state.clone()))
                .or(listings_revision(state.clone()))
                .or(listings_snapshot(state.clone()))
                .or(stats_json(state.clone(), false))
                .or(stats_json(state.clone(), true)),
        )
        .boxed()
}

pub fn internal_routes(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    snapshot_refresh(state).boxed()
}

fn health() -> BoxedFilter<(impl Reply,)> {
    warp::get()
        .and(warp::path("health"))
        .and(warp::path::end())
        .map(|| warp::reply::json(&serde_json::json!({ "ok": true })))
        .boxed()
}

fn listings_revision(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("listings")
        .and(warp::path("revision"))
        .and(warp::path::end())
        .map(move || {
            let body = serde_json::json!({ "revision": state.current_listings_revision() });
            let mut response = warp::http::Response::new(Body::from(body.to_string()));
            response.headers_mut().insert(
                warp::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
            response
                .headers_mut()
                .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        });

    warp::get().and(route).boxed()
}

fn listings_snapshot(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    async fn logic(state: Arc<State>) -> Result<warp::reply::Response, Infallible> {
        let response = match state.listings_snapshot_source {
            ListingsSnapshotSource::Inline => match get_api_snapshot(state).await {
                Ok(Some(snapshot)) => snapshot_json_response(snapshot),
                Ok(None) => warp::reply::with_status(
                    "failed to build listings snapshot",
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
                .into_response(),
                Err(error) => {
                    tracing::error!("failed to build listings snapshot json: {:#?}", error);
                    warp::reply::with_status(
                        "failed to build listings snapshot",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                    .into_response()
                }
            },
            ListingsSnapshotSource::Materialized => {
                materialized_snapshot_result_response(get_api_snapshot(state).await)
            }
        };

        Ok(response)
    }

    warp::get()
        .and(warp::path("listings"))
        .and(warp::path("snapshot"))
        .and(warp::path::end())
        .and_then(move || logic(state.clone()))
        .boxed()
}

fn stats_json(state: Arc<State>, seven_days: bool) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("stats")
        .and(if seven_days {
            warp::path("7days").boxed()
        } else {
            warp::path::end().boxed()
        })
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and_then(move |headers| stats_json_handler(state.clone(), headers, seven_days));

    warp::get().and(route).boxed()
}

fn snapshot_refresh(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    async fn logic(
        state: Arc<State>,
        headers: HeaderMap,
    ) -> Result<warp::reply::Response, Infallible> {
        let response = if state.listings_snapshot_source != ListingsSnapshotSource::Materialized {
            warp::reply::with_status("snapshot refresh disabled", StatusCode::NOT_FOUND)
                .into_response()
        } else {
            let request = match authorize_snapshot_refresh_request(&state, &headers).await {
                Ok(request) => request,
                Err(error) => {
                    return Ok(
                        warp::reply::with_status(error.message, error.status).into_response()
                    );
                }
            };

            match refresh_materialized_snapshot_with_loader(Arc::clone(&state), request, || async {
                load_current_materialized_snapshot(&state).await
            })
            .await
            {
                Ok(_) => warp::reply::with_status("ok", StatusCode::OK).into_response(),
                Err(error) => {
                    tracing::error!(
                        "failed to refresh materialized listings snapshot: {:#?}",
                        error
                    );
                    warp::reply::with_status(
                        "failed to refresh listings snapshot",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                    .into_response()
                }
            }
        };

        Ok(response)
    }

    warp::post()
        .and(warp::path("internal"))
        .and(warp::path("listings"))
        .and(warp::path("snapshot"))
        .and(warp::path("refresh"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and_then(move |headers| logic(state.clone(), headers))
        .boxed()
}
