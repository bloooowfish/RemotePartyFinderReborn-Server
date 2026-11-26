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
use super::{contribute, fflogs_ingest, tomestone_ingest};
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

fn optional_raw_query() -> impl Filter<Extract = (Option<String>,), Error = Infallible> + Copy {
    warp::query::raw()
        .map(Some)
        .or_else(|_| async { Ok::<_, Infallible>((None,)) })
}

fn join_path_and_query(path: &str, raw_query: Option<&str>) -> String {
    match raw_query {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        _ => path.to_string(),
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
        .and(optional_raw_query())
        .and_then(
            move |headers: HeaderMap,
                  remote_addr: Option<SocketAddr>,
                  raw_query: Option<String>| {
                let state = Arc::clone(&state);
                async move {
                    let pre = ingest_guard::precheck_post_json_request(
                        &state,
                        endpoint,
                        &headers,
                        remote_addr,
                        rate_limit_only_authenticate,
                    )
                    .await
                    .map_err(|error| warp::reject::custom(PreAuthGuardRejection(error)))?;

                    let path_and_query = join_path_and_query(path, raw_query.as_deref());
                    Ok::<_, Rejection>((state, headers, remote_addr, path_and_query, pre))
                }
            },
        )
        .untuple_one()
        .and(warp::body::bytes())
        .and_then(
            move |state: Arc<State>,
                  headers: HeaderMap,
                  remote_addr: Option<SocketAddr>,
                  path_and_query: String,
                  pre: ingest_guard::PreBodyAuthState,
                  body: Bytes| {
                let handler = handler.clone();
                async move {
                    let ctx = ingest_guard::finalize_post_json_auth(
                        &state,
                        endpoint,
                        &headers,
                        remote_addr,
                        &path_and_query,
                        &body,
                        pre,
                        header_capability_scope,
                    )
                    .await
                    .map_err(|error| warp::reject::custom(PreAuthGuardRejection(error)))?;

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
        .or(contribute_tomestone_jobs(Arc::clone(&state)))
        .or(contribute_tomestone_results(Arc::clone(&state)))
        .or(contribute_tomestone_leases_abandon(Arc::clone(&state)))
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
            |state, _headers, ctx, listing: PartyFinderListing| {
                contribute::contribute_handler_with_guard(state, ctx, listing)
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
            |state, _headers, ctx, listings: Vec<serde_json::Value>| {
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
        .and(optional_raw_query())
        .and_then(move |headers, remote_addr, raw_query: Option<String>| {
            let path_and_query = join_path_and_query("/contribute/fflogs/jobs", raw_query.as_deref());
            fflogs_ingest::contribute_fflogs_jobs_handler(
                Arc::clone(&state),
                headers,
                remote_addr,
                path_and_query,
            )
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

fn contribute_tomestone_jobs(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("tomestone"))
        .and(warp::path("jobs"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(optional_raw_query())
        .and_then(move |headers, remote_addr, raw_query: Option<String>| {
            let path_and_query =
                join_path_and_query("/contribute/tomestone/jobs", raw_query.as_deref());
            tomestone_ingest::contribute_tomestone_jobs_handler(
                Arc::clone(&state),
                headers,
                remote_addr,
                path_and_query,
            )
        });
    warp::get().and(route).boxed()
}

fn contribute_tomestone_results(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_tomestone_results;
    let route = warp::path("contribute")
        .and(warp::path("tomestone"))
        .and(warp::path("results"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributeTomestoneResults,
            "/contribute/tomestone/results",
            max_body,
            false,
            Some(CapabilityScope::TomestoneResults),
            |state, _headers, ctx, results: Vec<tomestone_ingest::TomestoneResult>| {
                tomestone_ingest::contribute_tomestone_results_handler_with_guard(
                    state, ctx, results,
                )
            },
        ));
    warp::post().and(route).boxed()
}

fn contribute_tomestone_leases_abandon(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let max_body = state.max_body_bytes_tomestone_results;
    let route = warp::path("contribute")
        .and(warp::path("tomestone"))
        .and(warp::path("leases"))
        .and(warp::path("abandon"))
        .and(warp::path::end())
        .and(post_authorized_json_filter(
            state,
            IngestEndpoint::ContributeTomestoneLeasesAbandon,
            "/contribute/tomestone/leases/abandon",
            max_body,
            false,
            Some(CapabilityScope::TomestoneLeasesAbandon),
            |state, headers, ctx, leases: Vec<tomestone_ingest::AbandonTomestoneLease>| {
                tomestone_ingest::contribute_tomestone_leases_abandon_handler_with_guard(
                    state, headers, ctx, leases,
                )
            },
        ));
    warp::post().and(route).boxed()
}

#[cfg(test)]
mod tests {
    use super::router;
    use crate::web::ingest_guard::{issue_capability_token, CapabilityClaims, CapabilityScope};
    use crate::web::test_support::{
        sample_listing, signed_ingest_headers_with_body, state_with_signature_required,
    };
    use chrono::Utc;
    use std::sync::Arc;
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
                r#"{"listing_id":1,"created_world":73,"leader_content_id":2,"leader_name":"Leader","home_world":73,"member_content_ids":[2],"member_jobs":[37],"slot_flags":["0x0000000000000001"]}"#
                    .to_string(),
            ),
            (
                "/contribute/detail/batch",
                r#"[{"listing_id":1,"created_world":73,"leader_content_id":2,"leader_name":"Leader","home_world":73,"member_content_ids":[2],"member_jobs":[37],"slot_flags":["0x0000000000000001"]}]"#
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
        let headers = signed_ingest_headers_with_body(
            "test-shared-secret",
            "route-client",
            "detail-batch-route",
            "POST",
            "/contribute/detail/batch",
            "test.local",
            b"[]",
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
            r#"[{{"listing_id":1,"created_world":73,"leader_content_id":2,"leader_name":"{oversized_name}","home_world":73,"member_content_ids":[2],"member_jobs":[37],"slot_flags":["0x0000000000000001"]}}]"#
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
    async fn fflogs_and_tomestone_routes_bind_request_bodies_before_authorization() {
        let route = router(state_with_signature_required().await);

        for jobs_path in ["/contribute/fflogs/jobs", "/contribute/tomestone/jobs"] {
            let jobs_response = warp::test::request()
                .method("GET")
                .path(jobs_path)
                .reply(&route)
                .await;
            assert_eq!(
                jobs_response.status(),
                StatusCode::UNAUTHORIZED,
                "jobs route {jobs_path} should reach authorization"
            );
        }

        for (path, body) in [
            (
                "/contribute/fflogs/results",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"partition":1,"encounters":{"1":95.0},"boss_percentages":{},"clear_counts":{"1":1},"is_hidden":false,"is_estimated":false,"matched_server":"Chocobo","lease_token":"lease"}]"#,
            ),
            (
                "/contribute/fflogs/leases/abandon",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"partition":1,"lease_token":"lease","reason":"test"}]"#,
            ),
            (
                "/contribute/tomestone/results",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"encounter_id":104,"encounters":{},"boss_percentages":{"104":52.5},"clear_counts":{},"is_hidden":false,"is_estimated":false,"matched_server":"Chocobo","lease_token":"lease"}]"#,
            ),
            (
                "/contribute/tomestone/leases/abandon",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"encounter_id":104,"lease_token":"lease","reason":"test"}]"#,
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
    async fn contribute_and_fflogs_and_tomestone_routes_reject_malformed_json_during_auth_before_deserialization() {
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
            "/contribute/tomestone/results",
            "/contribute/tomestone/leases/abandon",
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

    #[tokio::test]
    async fn detail_batch_route_enforces_pre_body_rate_limit_before_json_deserialization() {
        let mut state = state_with_signature_required().await;
        {
            let mutable = Arc::get_mut(&mut state).unwrap();
            mutable.ingest_security.rate_limits.detail_per_minute = 1;
        }
        let route = router(Arc::clone(&state));

        let bad_json = b"{invalid";
        let headers_1 = signed_ingest_headers_with_body(
            "test-shared-secret",
            "batch-rate-client",
            "pre-body-1",
            "POST",
            "/contribute/detail/batch",
            "test.local",
            bad_json,
        );
        let mut req_1 = warp::test::request()
            .method("POST")
            .path("/contribute/detail/batch")
            .header("content-type", "application/json")
            .body(&bad_json[..]);
        for (name, value) in headers_1.iter() {
            req_1 = req_1.header(name.as_str(), value.to_str().unwrap());
        }
        let resp_1 = req_1.reply(&route).await;
        assert_eq!(resp_1.status(), StatusCode::BAD_REQUEST);

        // Second request must be stopped pre-body with 429 before JSON parsing or body verification.
        let headers_2 = signed_ingest_headers_with_body(
            "test-shared-secret",
            "batch-rate-client",
            "pre-body-2",
            "POST",
            "/contribute/detail/batch",
            "test.local",
            bad_json,
        );
        let mut req_2 = warp::test::request()
            .method("POST")
            .path("/contribute/detail/batch")
            .header("content-type", "application/json")
            .body(&bad_json[..]);
        for (name, value) in headers_2.iter() {
            req_2 = req_2.header(name.as_str(), value.to_str().unwrap());
        }
        let resp_2 = req_2.reply(&route).await;
        assert_eq!(resp_2.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn get_jobs_route_includes_query_string_in_v2_canonical_signature() {
        let mut state = state_with_signature_required().await;
        {
            let mutable = Arc::get_mut(&mut state).unwrap();
            mutable.fflogs_jobs_limit = 0; // avoids Mongo query after auth in unit test
        }
        let route = router(Arc::clone(&state));
        let now = Utc::now().timestamp();
        let cap_token = issue_capability_token(
            &state.ingest_security.capability_secret,
            &CapabilityClaims::new(
                "fflogs-q-worker".to_string(),
                CapabilityScope::FflogsJobs,
                None,
                now,
                now + 300,
            ),
        )
        .unwrap();

        let wrong_query_headers = signed_ingest_headers_with_body(
            "test-shared-secret",
            "fflogs-q-worker",
            "q-nonce-1",
            "GET",
            "/contribute/fflogs/jobs?limit=99",
            "test.local",
            b"",
        );
        let mut bad_req = warp::test::request()
            .method("GET")
            .path("/contribute/fflogs/jobs?limit=1")
            .header("x-rpf-capability", &cap_token);
        for (name, value) in wrong_query_headers.iter() {
            bad_req = bad_req.header(name.as_str(), value.to_str().unwrap());
        }
        assert_eq!(bad_req.reply(&route).await.status(), StatusCode::UNAUTHORIZED);

        let valid_headers = signed_ingest_headers_with_body(
            "test-shared-secret",
            "fflogs-q-worker",
            "q-nonce-2",
            "GET",
            "/contribute/fflogs/jobs?limit=1",
            "test.local",
            b"",
        );
        let mut ok_req = warp::test::request()
            .method("GET")
            .path("/contribute/fflogs/jobs?limit=1")
            .header("x-rpf-capability", &cap_token);
        for (name, value) in valid_headers.iter() {
            ok_req = ok_req.header(name.as_str(), value.to_str().unwrap());
        }
        assert_eq!(ok_req.reply(&route).await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn contribute_multiple_route_isolates_unknown_category_and_rejects_oversized_headers() {
        let state = state_with_signature_required().await;
        let route = router(Arc::clone(&state));

        let mut bad_listing = serde_json::to_value(sample_listing()).unwrap();
        bad_listing["category"] = serde_json::json!(999_999_u32);
        let body = serde_json::to_vec(&vec![bad_listing]).unwrap();

        let headers = signed_ingest_headers_with_body(
            "test-shared-secret",
            "multi-client",
            "multi-nonce-1",
            "POST",
            "/contribute/multiple",
            "test.local",
            &body,
        );
        let mut req = warp::test::request()
            .method("POST")
            .path("/contribute/multiple")
            .header("content-type", "application/json")
            .body(&body[..]);
        for (name, value) in headers.iter() {
            req = req.header(name.as_str(), value.to_str().unwrap());
        }
        let resp = req.reply(&route).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(payload["requested"], 1);
        assert_eq!(payload["accepted"], 0);
        assert_eq!(payload["failed"], 1);

        let oversized_client = "x".repeat(129);
        let bad_resp = warp::test::request()
            .method("POST")
            .path("/contribute/multiple")
            .header("content-type", "application/json")
            .header("x-rpf-client-id", oversized_client)
            .body("[]")
            .reply(&route)
            .await;
        assert_eq!(bad_resp.status(), StatusCode::BAD_REQUEST);
    }
}
