use std::sync::Arc;

use crate::web::State;
use crate::ws::WsApiClient;
use warp::filters::BoxedFilter;
use warp::{Filter, Reply};
pub(super) fn ws(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route =
        warp::path("ws")
            .and(warp::ws())
            .and(warp::path::end())
            .map(move |ws: warp::ws::Ws| {
                let Some(permit) = state.ws_clients.try_acquire() else {
                    return warp::reply::with_status(
                        "too many websocket connections",
                        warp::http::StatusCode::SERVICE_UNAVAILABLE,
                    )
                    .into_response();
                };

                let state = Arc::clone(&state);
                ws.on_upgrade(move |websocket| async move {
                    let _permit = permit;
                    WsApiClient::run(state, websocket).await;
                })
                .into_response()
            });

    warp::get().and(route).boxed()
}
