use std::sync::Arc;
use warp::{filters::BoxedFilter, Filter, Reply};

use super::State;
use super::{contribute, fflogs_ingest};
use crate::listing::PartyFinderListing;
use crate::player::{UploadableCharacterIdentity, UploadablePlayer};

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
    let route = warp::path("contribute")
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_contribute,
        ))
        .and(warp::body::json())
        .and_then(move |headers, remote_addr, listing: PartyFinderListing| {
            contribute::contribute_handler(Arc::clone(&state), headers, remote_addr, listing)
        });
    warp::post().and(route).boxed()
}

fn contribute_multiple(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("multiple"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_multiple,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, listings: Vec<PartyFinderListing>| {
                contribute::contribute_multiple_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    listings,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_players(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("players"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_players,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, players: Vec<UploadablePlayer>| {
                contribute::contribute_players_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    players,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_character_identity(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("character-identity"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_players,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, identities: Vec<UploadableCharacterIdentity>| {
                contribute::contribute_character_identity_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    identities,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_detail(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("detail"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_detail,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, detail: contribute::UploadablePartyDetail| {
                contribute::contribute_detail_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    detail,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_detail_batch(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("detail"))
        .and(warp::path("batch"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_detail_batch,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, items: Vec<contribute::UploadablePartyDetailBatchItem>| {
                contribute::contribute_detail_batch_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    items,
                )
            },
        );
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
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("results"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_fflogs_results,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, results: Vec<fflogs_ingest::ParseResult>| {
                fflogs_ingest::contribute_fflogs_results_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    results,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_fflogs_leases_abandon(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("leases"))
        .and(warp::path("abandon"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_fflogs_results,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, leases: Vec<fflogs_ingest::AbandonFflogsLease>| {
                fflogs_ingest::contribute_fflogs_leases_abandon_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    leases,
                )
            },
        );
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
}
