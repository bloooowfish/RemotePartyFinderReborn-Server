use std::{convert::Infallible, future::Future, net::SocketAddr, sync::Arc};
use serde::de::DeserializeOwned;
use warp::{
    filters::BoxedFilter,
    http::{HeaderMap, StatusCode},
    hyper::body::Bytes,
    Filter, Rejection, Reply,
};

use super::ingest_guard::{self, CapabilityScope, GuardContext, GuardError, IngestEndpoint};
use super::State;
use super::{contribute, fflogs_ingest};
use crate::listing::PartyFinderListing;
use crate::player::{UploadableCharacterIdentity, UploadablePlayer};

#[derive(Debug)]
struct PreAuthGuardRejection(GuardError);

impl warp::reject::Reject for PreAuthGuardRejection {}

fn recover_pre_auth_guard(
    rejection: Rejection,
) -> std::future::Ready<Result<warp::reply::Response, Rejection>> {
    if let Some(PreAuthGuardRejection(error)) = rejection.find::<PreAuthGuardRejection>() {
        std::future::ready(Ok(ingest_guard::guard_error_reply(*error)))
    } else {
        std::future::ready(Err(rejection))
    }
}

fn post_authorized_json_filter<T, F, Fut>(
    state: Arc<State>,
    endpoint: IngestEndpoint,
    path: &'static str,
    max_body_bytes: u64,
    rate_limit_only_authenticate: bool,
    header_capability_scope: Option<CapabilityScope>,
    handler: F,
) -> BoxedFilter<(warp::reply::Response,)>
where
    T: DeserializeOwned + Send + 'static,
    F: Fn(Arc<State>, HeaderMap, GuardContext, T) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<warp::reply::Response, Infallible>> + Send + 'static,
{
    warp::body::content_length_limit(max_body_bytes)
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and_then(move |headers: HeaderMap, remote_addr: Option<SocketAddr>| {
            let state = Arc::clone(&state);
            async move {
                let ctx = if rate_limit_only_authenticate {
                    ingest_guard::authenticate_request(
                        &state,
                        endpoint,
                        &headers,
                        remote_addr,
                        "POST",
                        path,
                    )
                    .await
                } else {
                    ingest_guard::authorize_request(
                        &state,
                        endpoint,
                        &headers,
                        remote_addr,
                        "POST",
                        path,
                    )
                    .await
                }
                .map_err(|error| warp::reject::custom(PreAuthGuardRejection(error)))?;

                if let Some(scope) = header_capability_scope {
                    if let Err(error) = ingest_guard::require_capability(
                        &state,
                        &headers,
                        &ctx.client_id,
                        scope,
                        None,
                    ) {
                        let mapped = match ingest_guard::record_capability_auth_failure(
                            &state,
                            endpoint,
                            &ctx,
                        )
                        .await
                        {
                            Ok(()) => error,
                            Err(rate_err) => rate_err,
                        };
                        return Err(warp::reject::custom(PreAuthGuardRejection(mapped)));
                    }
                }

                Ok((state, headers, ctx))
            }
        })
        .untuple_one()
        .and(warp::body::bytes())
        .and_then(
            move |state: Arc<State>, headers: HeaderMap, ctx: GuardContext, body: Bytes| {
                let handler = handler.clone();
                async move {
                    let parsed = match serde_json::from_slice::<T>(&body) {
                        Ok(parsed) => parsed,
                        Err(_) => {
                            return Ok::<_, Rejection>(
                                warp::reply::with_status(
                                    "invalid json payload",
                                    StatusCode::BAD_REQUEST,
                                )
                                .into_response(),
                            );
                        }
                    };
                    let response = handler(state, headers, ctx, parsed).await;
                    Ok::<_, Rejection>(response.unwrap_or_else(|never| match never {}))
                }
            },
        )
        .recover(recover_pre_auth_guard)
        .unify()
        .boxed()
}

pub fn router(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    contribute(Arc::clone(&state))
        .or(contribute_multiple(Arc::clone(&state)))
        .or(contribute_players(Arc::clone(&state)))
        .or(contribute_character_identity(Arc::clone(&state)))
        .or(contribute_detail_batch(Arc::clone(&state)))
        .or(contribute_detail(Arc::clone(&state)))
        .or(contribute_fflogs_jobs(Arc::clone(&state)))
        .or(contribute_fflogs_results(Arc::clone(&state)))
        .or(contribute_fflogs_leases_abandon(Arc::clone(&state)))
        .or(crate::api::internal_routes(Arc::clone(&state)))
        .or(crate::api::api(Arc::clone(&state)))
        .boxed()
}

fn contribute(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_contribute;
    let route = warp::path("contribute")
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::Contribute,
            "/contribute",
            max_body,
            false,
            None,
            |state, _headers, _ctx, listing: PartyFinderListing| {
                contribute::contribute_handler_with_guard(state, listing)
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_multiple(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_multiple;
    let route = warp::path("contribute")
        .and(warp::path("multiple"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributeMultiple,
            "/contribute/multiple",
            max_body,
            false,
            None,
            |state, _headers, ctx, listings: Vec<PartyFinderListing>| {
                contribute::contribute_multiple_handler_with_guard(state, ctx, listings)
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_players(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_players;
    let route = warp::path("contribute")
        .and(warp::path("players"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributePlayers,
            "/contribute/players",
            max_body,
            false,
            None,
            |state, _headers, ctx, players: Vec<UploadablePlayer>| {
                contribute::contribute_players_handler_with_guard(state, ctx, players)
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_character_identity(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_players;
    let route = warp::path("contribute")
        .and(warp::path("character-identity"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributePlayers,
            "/contribute/character-identity",
            max_body,
            false,
            None,
            |state, _headers, ctx, identities: Vec<UploadableCharacterIdentity>| {
                contribute::contribute_character_identity_handler_with_guard(state, ctx, identities)
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_detail(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_detail;
    let route = warp::path("contribute")
        .and(warp::path("detail"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributeDetail,
            "/contribute/detail",
            max_body,
            false,
            None,
            |state, headers, ctx, detail: contribute::UploadablePartyDetail| {
                contribute::contribute_detail_handler_with_guard(state, headers, ctx, detail)
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_detail_batch(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_detail_batch;
    let route = warp::path("contribute")
        .and(warp::path("detail"))
        .and(warp::path("batch"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributeDetail,
            "/contribute/detail/batch",
            max_body,
            true,
            None,
            |state, _headers, ctx, items: Vec<contribute::UploadablePartyDetailBatchItem>| {
                contribute::contribute_detail_batch_handler_with_guard(state, ctx, items)
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_fflogs_jobs(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("jobs"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and_then(move |headers, remote_addr| {
            fflogs_ingest::contribute_fflogs_jobs_handler(Arc::clone(&state), headers, remote_addr)
        });
    warp::get().and(route).boxed()
}

fn contribute_fflogs_results(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_fflogs_results;
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("results"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributeFflogsResults,
            "/contribute/fflogs/results",
            max_body,
            false,
            Some(CapabilityScope::FflogsResults),
            |state, _headers, ctx, results: Vec<fflogs_ingest::ParseResult>| {
                fflogs_ingest::contribute_fflogs_results_handler_with_guard(state, ctx, results)
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_fflogs_leases_abandon(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_fflogs_results;
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("leases"))
        .and(warp::path("abandon"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributeFflogsLeasesAbandon,
            "/contribute/fflogs/leases/abandon",
            max_body,
            false,
            Some(CapabilityScope::FflogsLeasesAbandon),
            |state, headers, ctx, leases: Vec<fflogs_ingest::AbandonFflogsLease>| {
                fflogs_ingest::contribute_fflogs_leases_abandon_handler_with_guard(
                    state, headers, ctx, leases,
                )
            },
        ));
    warp::post().and(route).boxed()
}

#[cfg(test)]
mod tests {
    use super::router;
    use crate::web::test_support::{
        sample_listing, signed_ingest_headers, state_with_signature_required,
    };
    use warp::http::StatusCode;

    #[tokio::test]
    async fn origin_router_does_not_serve_public_web_ui() {
        let route = router(state_with_signature_required().await);

        for path in [
            "/",
            "/listings",
            "/stats",
            "/stats/7days",
            "/assets/common.css",
        ] {
            let response = warp::test::request()
                .method("GET")
                .path(path)
                .reply(&route)
                .await;

            assert!(
                !response.status().is_success() && !response.status().is_redirection(),
                "origin server should not serve or redirect public web UI path {path}; got {}",
                response.status()
            );
        }
    }

    #[tokio::test]
    async fn contribute_routes_bind_request_bodies_before_authorization() {
        let route = router(state_with_signature_required().await);
        let listing = serde_json::to_string(&sample_listing()).unwrap();

        for (path, body) in [
            ("/contribute", listing),
            ("/contribute/multiple", "[]".to_string()),
            ("/contribute/players", "[]".to_string()),
            ("/contribute/character-identity", "[]".to_string()),
            (
                "/contribute/detail",
                r#"{"listing_id":1,"leader_content_id":2,"leader_name":"Leader","home_world":73,"member_content_ids":[2],"member_jobs":[37],"slot_flags":["0x0000000000000001"]}"#
                    .to_string(),
            ),
            (
                "/contribute/detail/batch",
                r#"[{"listing_id":1,"leader_content_id":2,"leader_name":"Leader","home_world":73,"member_content_ids":[2],"member_jobs":[37],"slot_flags":["0x0000000000000001"]}]"#
                    .to_string(),
            ),
        ] {
            let response = warp::test::request()
                .method("POST")
                .path(path)
                .header("content-type", "application/json")
                .body(body)
                .reply(&route)
                .await;

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "route {path} should reach authorization after JSON binding"
            );
        }
    }

    #[tokio::test]
    async fn signed_detail_batch_route_uses_batch_canonical_path() {
        let route = router(state_with_signature_required().await);
        let headers = signed_ingest_headers(
            "test-shared-secret",
            "route-client",
            "detail-batch-route",
            "POST",
            "/contribute/detail/batch",
        );
        let mut request = warp::test::request()
            .method("POST")
            .path("/contribute/detail/batch")
            .header("content-type", "application/json")
            .body("[]");
        for (name, value) in headers.iter() {
            request = request.header(name.as_str(), value.to_str().unwrap());
        }

        let response = request.reply(&route).await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn detail_batch_route_rejects_body_over_batch_limit() {
        let route = router(state_with_signature_required().await);
        let oversized_name = "x".repeat(20 * 1024);
        let body = format!(
            r#"[{{"listing_id":1,"leader_content_id":2,"leader_name":"{oversized_name}","home_world":73,"member_content_ids":[2],"member_jobs":[37],"slot_flags":["0x0000000000000001"]}}]"#
        );

        let response = warp::test::request()
            .method("POST")
            .path("/contribute/detail/batch")
            .header("content-type", "application/json")
            .body(body)
            .reply(&route)
            .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn fflogs_routes_bind_request_bodies_before_authorization() {
        let route = router(state_with_signature_required().await);

        let jobs_response = warp::test::request()
            .method("GET")
            .path("/contribute/fflogs/jobs")
            .reply(&route)
            .await;
        assert_eq!(
            jobs_response.status(),
            StatusCode::UNAUTHORIZED,
            "FFLogs jobs route should reach authorization"
        );

        for (path, body) in [
            (
                "/contribute/fflogs/results",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"partition":1,"encounters":{"1":95.0},"boss_percentages":{},"clear_counts":{"1":1},"is_hidden":false,"is_estimated":false,"matched_server":"Chocobo","lease_token":"lease"}]"#,
            ),
            (
                "/contribute/fflogs/leases/abandon",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"partition":1,"lease_token":"lease","reason":"test"}]"#,
            ),
        ] {
            let response = warp::test::request()
                .method("POST")
                .path(path)
                .header("content-type", "application/json")
                .body(body)
                .reply(&route)
                .await;

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "route {path} should reach authorization after JSON binding"
            );
        }
    }

    #[tokio::test]
    async fn contribute_and_fflogs_routes_reject_malformed_json_during_auth_before_deserialization() {
        let route = router(state_with_signature_required().await);

        for path in [
            "/contribute",
            "/contribute/multiple",
            "/contribute/players",
            "/contribute/character-identity",
            "/contribute/detail",
            "/contribute/detail/batch",
            "/contribute/fflogs/results",
            "/contribute/fflogs/leases/abandon",
        ] {
            let response = warp::test::request()
                .method("POST")
                .path(path)
                .header("content-type", "application/json")
                .body("{not valid json")
                .reply(&route)
                .await;

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "route {path} must reject unauthenticated requests before JSON deserialization"
            );
        }
    }
}
