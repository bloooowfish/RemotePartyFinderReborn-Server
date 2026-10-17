# Remote Party Finder Reborn - Server

A Rust origin backend server that aggregates Party Finder listings, integrates with FFLogs, and exposes API/ingest endpoints for the public web edge.

## Disclaimer

This is a fork of the [original Remote Party Finder project](https://github.com/zeroeightysix/remote-party-finder)'s server part by zeroeightysix.

**Note**: This project is a proof of concept created for personal interest. There are no plans for a public release.

## Key Features

- **Origin API**: Serves listing snapshots, stats JSON, WebSocket revision notifications, and plugin ingest endpoints.
- **FFLogs Integration**: Receive parse data (Best Perfs) from clients and cache for display party members' Best Perfs.
- **Real-time Updates**: Receives data from connected Dalamud plugins.

## Architecture

The server is built with:
- **Language**: Rust
- **Web Framework**: Warp
- **Database**: MongoDB

## Setup & Usage

### Prerequisites

- **Rust** (Stable toolchain)
- **MongoDB** (Running instance)

### Configuration

1.  Copy `config.example.toml` to `config.toml`.
2.  Edit `config.toml` with your environment details:

    ```toml
    [web]

    # Network
    host = "127.0.0.1:8000"

    # FFLogs job scheduling
    fflogs_jobs_limit = 20
    fflogs_hidden_cache_ttl_hours = 24
    fflogs_active_parse_refresh_minutes = 60
    parse_cache_ttl_hours = 24

    # Console monitor snapshot (seconds, 0 to disable; default off for low-noise console)
    monitor_snapshot_interval_seconds = 0

    # MongoDB write concurrency
    listing_upsert_concurrency = 16
    player_upsert_concurrency = 32

    # Request body size limits (bytes)
    max_body_bytes_contribute = 262144
    max_body_bytes_multiple = 1048576
    max_body_bytes_players = 524288
    max_body_bytes_detail = 131072
    max_body_bytes_detail_batch = 524288
    max_body_bytes_fflogs_results = 524288

    # Batch size limits (items per request)
    max_multiple_batch_size = 200
    max_players_batch_size = 100
    max_fflogs_results_batch_size = 100
    max_detail_batch_size = 16
    max_detail_member_count = 48

    # Per-client rate limits (requests per minute)
    ingest_rate_limit_contribute_per_minute = 120
    ingest_rate_limit_multiple_per_minute = 60
    ingest_rate_limit_players_per_minute = 120
    ingest_rate_limit_detail_per_minute = 120
    ingest_rate_limit_fflogs_jobs_per_minute = 30
    ingest_rate_limit_fflogs_results_per_minute = 60
    ingest_rate_limit_prefer_client_id = false

    # Signature/replay protection
    # Keep false for public plugin distribution; true is for controlled/private deployments.
    ingest_require_signature = false
    ingest_shared_secret = "rpf-reborn-public-ingest-v1"
    ingest_clock_skew_seconds = 300
    ingest_nonce_ttl_seconds = 300

    # Protected endpoint capabilities
    # Keep false during rollout, then enable after upgraded plugin builds are deployed.
    ingest_require_capabilities_for_protected_endpoints = false
    ingest_capability_secret = ""
    ingest_capability_session_ttl_seconds = 43200
    ingest_capability_detail_ttl_seconds = 900

    [mongo]
    url = "mongodb://localhost:27017/remote-party-finder"
    ```

For signature-enforced operation, start from `config.secure.example.toml` instead.
Set `ingest_shared_secret` to a strong random value and use the same value in the plugin's `IngestSharedSecret`.

Recommended mode for public plugin distribution is `ingest_require_signature = false`.
Shared-secret signatures are suitable for controlled/private deployments only; they are not strong client authentication for openly distributed binaries.
Protected endpoint capabilities are designed for a staged rollout: keep
`ingest_require_capabilities_for_protected_endpoints = false` until updated plugin builds are live, then enable it with a separate `ingest_capability_secret`.

### Split Web/Origin Deployment

The preferred deployment is:

- Koyeb: public web/edge app from `RemotePartyFinderReborn-Web`.
- Raspberry Pi: this origin server plus FFLogs/report workers.
- MongoDB: Mongo Atlas.

The origin server uses inline listings snapshots; do not deploy a separate
snapshot worker process.

Pi origin config:

```toml
[web]
host = "127.0.0.1:8000"
listings_snapshot_source = "inline"
listings_snapshot_document_id = "current"
listing_source_state_document_id = "current"
listings_revision_coalesce_millis = 250
listings_revision_webhook_url = "https://<koyeb-app>/internal/listings/revision"
listings_revision_webhook_shared_secret = "<shared-webhook-secret>"
snapshot_refresh_shared_secret = ""

[mongo]
url = "<mongo-atlas-connection-string>"
```

Deploy the Pi origin server with:

```powershell
.\scripts\deploy-origin-server-rpi-aarch64.ps1 -RemoteHost <host> -RemoteUser <user>
```

The deploy script preserves the Pi-side `config.toml`. It creates one from
`config.example.toml` only when no config exists.

### Recommended Presets

Tune these four values together based on server capacity and contributor count:

| Preset | `fflogs_jobs_limit` | `fflogs_hidden_cache_ttl_hours` | `listing_upsert_concurrency` | `player_upsert_concurrency` |
| --- | ---: | ---: | ---: | ---: |
| Low-load / single-node | 10 | 24 | 8 | 16 |
| Balanced (default) | 20 | 24 | 16 | 32 |
| High-throughput | 30 | 12 | 24 | 48 |

- Higher `fflogs_jobs_limit` increases refresh speed but raises FFLogs/API pressure.
- Lower `fflogs_hidden_cache_ttl_hours` re-checks hidden characters sooner but increases query volume.
- Lower `fflogs_active_parse_refresh_minutes` makes active visible listings eligible for FFLogs refresh sooner, but increases FFLogs job and API pressure.
- The default refresh window is 60 minutes from receiving fresh data. Cache hits and listing updates do not extend it: data received at 13:00 becomes eligible for another fetch at 14:00, on the next job poll. Cached results remain available while the refresh is pending.
- `parse_cache_ttl_hours` controls how long documents in `parses` remain after their last refresh.
- `monitor_snapshot_interval_seconds` controls periodic one-line `[MONITOR]` logs for Koyeb console observability (default `0` = off).
- Increase upsert concurrency only if MongoDB has headroom; otherwise keep defaults.
- `/contribute/detail/batch` accepts up to `max_detail_batch_size` detail payloads per request, bounded separately by `max_body_bytes_detail_batch`.
- `ingest_require_signature = true` enables HMAC+timestamp+nonce verification for `/contribute*` requests.
- `ingest_require_capabilities_for_protected_endpoints = true` requires server-issued `X-RPF-Capability` tokens for `/contribute/detail` and `/contribute/fflogs/*`; `/contribute/detail/batch` uses a per-item `capability_token` field.
- `ingest_capability_session_ttl_seconds` controls the FFLogs worker token lifetime; `ingest_capability_detail_ttl_seconds` controls per-listing detail upload lifetime.
- `ingest_rate_limit_*` limits are keyed by remote IP by default and return `429` with `Retry-After`. For a Koyeb/Web relay in front of the origin, set `ingest_require_signature = true` and `ingest_rate_limit_prefer_client_id = true` so verified plugin clients are limited independently instead of sharing the relay IP bucket.
- Restart the server after editing `config.toml`.

### Tomestone Progress Data

The server does not proxy official Tomestone API calls. Plugins own official API access, keep Tomestone API keys locally, and submit only additive phase progress data to the server.

Submitted progress is cached by the server and exposed in listing snapshots. Public web display should show only the farthest/current phase tag for Ultimate phase progress, not every completed phase.

Tomestone official API failures are plugin-local and do not block FFLogs result uploads. Tomestone web/Inertia fallback remains scoped to FFLogs-style percentile fallback data and is unrelated to official API progression enrichment.

### Legacy `account_id` Migration

`players.account_id` is stored as `String` by design. If legacy documents still contain numeric values, the server can now read both formats, but you should normalize old records once.

Run this in Mongo shell:

```javascript
db.players.updateMany(
  { account_id: { $type: ["int", "long", "double", "decimal"] } },
  [
    {
      $set: {
        account_id: { $toString: "$account_id" }
      }
    }
  ]
)
```

### Running the Server

```bash
# Debug run
cargo run

# Release build and run
cargo run --release
```

The server listens on `http://127.0.0.1:8000` by default.

### Validation

```bash
# Type check on stable
CARGO_INCREMENTAL=0 cargo +stable check

# Targeted regression tests used by recent listings/member fixes
CARGO_INCREMENTAL=0 cargo +stable test resolve_member_player
```

### Docker

`Dockerfile` uses a stable Rust builder image (`rust:bookworm`).

## License

No open-source license has been declared for this repository at this time.

Please read `LEGAL_NOTICE.md` for provenance and policy details.
