# Refactor Investigation Report - 2026-05-07

## Scope

- 조사 대상: `RemotePartyFinderReborn-Plugin`, `RemotePartyFinderReborn-Web`, `RemotePartyFinderReborn-Server`.
- 제외 대상: Worker 계통 프로젝트, `References`.
- 작업 브랜치: 세 프로젝트 모두 `refactor`.
- 조사 방식: 정적 코드 조사, `rg` 기반 전수 스캔, 파일별 핵심 경로 확인, 병렬 서브 에이전트 조사 결과 취합.
- 이 문서는 구현 계획 전 검토용 리포트다. 코드 수정은 이 문서 추가 외에 새로 수행하지 않았다.
- 라인 번호는 2026-05-07 현재 dirty working tree 기준이다.

## Current Worktree State

루트 `E:\FF14\Repos\MyRepos\RFP-Reborn` 자체는 Git 저장소가 아니며, 각 하위 프로젝트가 별도 Git 저장소다.

- `RemotePartyFinderReborn-Plugin`: `refactor`, clean.
- `RemotePartyFinderReborn-Web`: `refactor`, dirty.
- `RemotePartyFinderReborn-Server`: `refactor`, dirty.

현재 dirty 상태는 사용자가 지적하기 전 이미 적용된 변경이다. 사용자 지시에 따라 그대로 둔다.

### Already Applied Before Report Review

Web 변경 통계:

- `public/assets/listings.js`
- `public/assets/stats.js`
- `public/listings.html`
- `server.js`
- `test/listings-assets.test.js`
- `test/server.test.js`
- 6 files changed, 116 insertions, 10 deletions

내용 요약:

- `?v=`가 있는 `/assets/*`에만 immutable cache 적용.
- live listing revision refresh timer가 더 빠른 refresh 요청을 재스케줄할 수 있도록 조정.
- stats bar chart empty/zero-sum 방어.
- `listings.js?v=20` bump 및 관련 테스트 추가.

Server 변경 통계:

- `src/domain/listing/types.rs`
- `src/domain/listing/types/tests.rs`
- `src/domain/player/mod.rs`
- `src/domain/player/player.rs` 삭제
- `src/domain/player/types.rs` 추가
- `src/domain/stats/mod.rs`
- `src/domain/stats/stats.rs` 삭제
- `src/domain/stats/summary.rs` 추가
- `src/ffxiv.rs`
- `src/infra/mongo/identities.rs`
- `src/infra/mongo/players.rs`
- `src/listings_render.rs`
- `src/listings_snapshot/payload.rs`
- `src/web/background.rs`
- `src/web/fflogs_ingest.rs`
- `src/web/fflogs_ingest/tests.rs`
- `src/web/mod.rs`
- 15 files changed, 141 insertions, 671 deletions

내용 요약:

- Rust module-inception 정리: `player.rs -> types.rs`, `stats.rs -> summary.rs`.
- Clippy cleanup: casts, clone, range, `len`, `io-other-error` 등.
- `Language::from_codes`의 `q=` parsing 개선 및 테스트.
- `FFLogsJobAppendContext`, `ParseDisplay` helper 도입 등 작은 가독성 정리.

이미 수행한 검증:

- Plugin: `dotnet test RemotePartyFinderReborn.Tests\RemotePartyFinderReborn.Tests.csproj --configuration Debug --no-restore` 통과, 226 tests.
- Web: `npm test` 통과, 21 tests.
- Server: `cargo test` 통과, lib 161 + main 1.
- Server: `cargo fmt --check` 통과.
- Server: `cargo clippy --all-targets --all-features -- -D warnings` 통과.
- Playwright MCP: `/listings`, `/stats` 503 shell 렌더 확인. 원 origin unavailable에 따른 503/404 console error는 예상 상태였다.

## Coverage Of Investigation

정적 파일 범위:

- Plugin C# 파일: 40개.
- Web JS/HTML/CSS 파일: 22개.
- Server `src`, `generator`, `scripts`의 Rust/C#/JS 파일: 67개.

주요 스캔 축:

- Plugin: `Task.Run`, `ContinueWith`, `Dispose`, `HttpClient`, `CancellationTokenSource`, `Signature`, `Offset`, addon suppression, config `Save`.
- Web: static serving, relay, WebSocket revision flow, cache headers, `Accept-Language`, `localStorage`, `innerHTML`, listing render, stats render.
- Server: background loop, Mongo cursor handling, state coupling, FFLogs job allocation, listing rendering, generator, `high_end`, FFLogs mapping.

## Executive Summary

가장 먼저 다뤄야 할 축은 다음 네 가지다.

1. Web static path validation has a real boundary bug.
   `server.js`가 `startsWith(PUBLIC_DIR)` 문자열 검사만 사용해서 `public2` 같은 sibling path가 허용될 수 있다. Koyeb public edge에 해당하므로 단순 가독성 문제가 아니라 공개 surface 문제다.

2. Patch data pipeline is brittle and partially stale.
   `generator`, `generate-listing-data.js`, `translations.js`, `listing-data.js`, Web public asset 경로가 서로 어긋나 있다. 새 컨텐츠 패치 대응 시 “Rust generated data는 바뀌었지만 Web asset은 안 바뀜” 또는 “FFLogs mapping은 추가했지만 `high_end` gate 때문에 기능이 안 켜짐”이 쉽게 발생한다.

3. Plugin background lifecycle and FFLogs failure classification are coupled risks.
   여러 collector가 fire-and-forget task를 만들고 unload 때 join하지 않는다. 동시에 FFLogs transient failure가 `HadTransientFailure: false`로 귀결될 수 있어 backoff가 작동하지 않는다.

4. Server runtime hides operational failure as empty or partial data.
   stats task는 실패 시 tight loop 가능성이 있고, Mongo cursor/decode error는 여러 repository에서 silent drop 된다. FFLogs job endpoint도 DB 실패를 빈 작업 또는 skip으로 흡수한다.

## UX Invariance Addendum

사용자 추가 조건: UX는 변하지 않아야 한다. 속도 개선에 따른 체감 개선은 허용되지만, 문구, 레이아웃, 기본 필터, 표시 데이터, 언어 선택, 빈 상태, 에러 상태, 카운트 의미, listing 노출 범위가 바뀌는 변경은 사전 검토 대상이다.

2026-05-07 decision update:

- 우선 구현 범위는 UX에 영향이 없는 변경으로 제한한다.
- UX에 영향이 있는 변경은 추후 별도 목록으로 뽑아 검토한다.
- Web listings의 제품 목적은 오직 high-end duty 표시다. high-end toggle은 복구하지 않는다.
- 클라이언트에서 high-end 여부를 판단할 수 있는 신호는 적극 활용하되, 첫 단계에서는 public snapshot `high_end`, Web 필터, FFLogs job gate를 바꾸지 않는 telemetry/storage 경로로 적용한다.

### UX Impact Matrix

No intended UX change:

- `W1` static path boundary: 정상 asset URL의 응답과 화면은 유지되어야 한다. traversal/sibling path의 거절 동작만 달라진다.
- `W4`, `W5` asset version/cleanup: 동일 asset 내용과 참조 경로를 유지하면 화면 변화는 없다. stale asset이 사라지는 것은 배포 정확성 개선이다.
- `W6` safe storage wrapper: 정상 브라우저에서는 현재 저장/복원 동작을 유지해야 한다. storage 차단 환경에서만 graceful fallback이 생긴다.
- `W10`, `W12` render/data pipeline refactor: 출력 DOM, 정렬, 필터 결과, 차트 값이 byte-for-byte 또는 DOM snapshot 기준으로 같아야 한다. 허용되는 변화는 main-thread 시간 감소뿐이다.
- `P1`, `P3`, `P5`, `P6`, `P7`, `S1`, `S2`, `S5`, `S6`, `S7`, `S8`: 내부 lifecycle, 오류 처리, 구조화 refactor다. 사용자-visible 상태 문구나 API contract를 바꾸면 별도 승인 대상이다.
- high-end client telemetry-only phase: 새 신호를 업로드/로그/비교만 하고 Web 필터, snapshot `high_end`, FFLogs 표시, job allocation에 쓰지 않으면 UX 변화는 없다.

Visible behavior change, requires explicit review:

- `W2`: `Accept-Language` q-value를 정확히 반영하면 503/unavailable 페이지의 언어가 기존과 달라질 수 있다.
- `W3`: no-cookie 첫 방문 언어를 browser language로 바꾸면 첫 화면 언어가 달라진다.
- `W7`: countdown이 실제 시간에 따라 감소하면 같은 snapshot에서도 표시 문자열이 바뀐다.
- `W8`: high-end toggle을 복구하거나 high-end-only 정책을 명시적으로 바꾸면 listing 노출 범위가 달라진다.
- `W9`: data center count의 의미를 “전체”에서 “현재 필터 결과”로 바꾸면 숫자가 달라진다.
- `W11`: hardcoded English를 번역 key로 옮기면 표시 문구와 언어 혼합 상태가 달라질 수 있다.
- `W13`: empty/error state를 명확히 만들면 빈 화면, 실패 화면, chart preload 동작이 보인다.
- `C3`, `C4`: high-end 판정과 FFLogs mapping을 실제 표시/수집 gate에 반영하면 listing visibility, FFLogs badge/parse 표시, worker job 생성 범위가 달라진다.
- `C7`: stats duty/category decode를 고치면 stats 페이지의 일부 비정상 category label이 올바른 이름으로 바뀔 수 있다.

Planning rule:

- 먼저 “무변경 검증”을 구현하고 snapshot/DOM/API diff로 기존 UX 보존을 확인한다.
- UX-visible correctness fix는 별도 항목으로 분리하고, 적용 전후에 어떤 listing, badge, count, language, empty state가 바뀌는지 fixture로 제시한다.

## Client-Side High-End Duty Detection Research

`References/FFXIVClientStructs`와 `References/Dalamud`를 확인한 결과, 수동 duty table만으로 판정하는 방식은 피할 수 있다. 다만 “PF listing이 High-end Duty 탭에 올라왔는가”와 “해당 duty 자체를 RPF high-end target으로 볼 것인가”는 다른 질문이다.

### Available Client Signals

- Party Finder packet/listing 자체가 category를 갖는다.
  - `References/FFXIVClientStructs/FFXIVClientStructs/FFXIV/Client/Game/Network/PartyFinderPackets.cs:31-34`: `Category`, `Duty`, `DutyType`.
  - `References/FFXIVClientStructs/FFXIVClientStructs/FFXIV/Client/UI/Agent/AgentLookingForGroup.cs:148-155`: `DutyCategory.HighEndDuty = 1 << 6`.
  - `References/Dalamud/Dalamud/Game/Gui/PartyFinder/Types/PartyFinderListing.cs:93-108`: `IPartyFinderListing.Category`, `RawDuty`, `Duty`, `DutyType`.
- 현재 Plugin은 이미 이 값을 업로드한다.
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/UploadableListing.cs:20-22`, `45-47`: `Category = source.Category`, `Duty = source.RawDuty`, `DutyType = source.DutyType`.
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PartyFinder/PartyDetailCaptureRuntime.cs:559-560`: detail snapshot도 `LastViewedListing.Category`, `DutyId`를 잡는다.
- Dalamud/Lumina에서 `ContentFinderCondition` row를 읽을 수 있다.
  - `References/Dalamud/lib/Lumina.Excel/deps/EXDSchema/ContentFinderCondition.yml:118-141`: `ContentType`, `ContentUICategory`, `AllowUndersized`, `IsInDutyFinder`, `HighEndDuty`.
  - `References/Dalamud/Dalamud/Plugin/Services/IDataManager.cs:27-55`: plugin service로 Excel sheet 접근 가능.
  - 단, 현재 서버 generated data가 여러 최신 Extreme/Savage row에서 `HighEndDuty=false`를 내고 있으므로, `ContentFinderCondition.HighEndDuty` 단독 승격은 live client에서 재검증해야 한다.
- FFXIVClientStructs에는 native client helper도 있다.
  - `References/FFXIVClientStructs/FFXIVClientStructs/FFXIV/Client/Game/UI/ContentInterface.cs:208`: `IsHighEndDuty()`.
  - `References/FFXIVClientStructs/FFXIVClientStructs/FFXIV/Client/Game/UI/InstanceContent.cs:49-52`: `ContentUI.LoadByContentFinderConditionId(rowId)` 후 `GetContentInterface()`.
  - 이 경로는 client parity가 가장 강하지만 unsafe/interop-heavy이며 patch signature/vfunc 변화에 민감하다. FFXIVClientStructs/Dalamud가 감싸는 현재 dependency를 통해서만 써야 하며, `References`를 production dependency로 직접 참조하면 안 된다.
- `Duty`는 항상 CFC row id가 아니다.
  - `PartyFinderPackets.cs:32` 주석상 `DutyType`에 따라 `ContentFinderCondition` 또는 `LFGExtensionContent` row id가 된다.
  - CFC 기반 판정은 `DutyType == Normal`과 `RawDuty != 0`에서만 적용해야 한다.

### Recommended No-UX-Change Evaluation Path

1. Plugin에서 세 신호를 비교 가능한 metadata로 만든다.
   - `pf_category_high_end`: `listing.Category.HasFlag(DutyCategory.HighEndDuty)`.
   - `cfc_excel_high_end`: `DutyType.Normal`일 때 Lumina `ContentFinderCondition.HighEndDuty`.
   - `native_content_high_end`: 선택적 실험 경로. `ContentUI.LoadByContentFinderConditionId` + `IsHighEndDuty()`를 cache하고 debug/telemetry로만 사용한다.
2. Server는 첫 단계에서 이 metadata를 Web snapshot gate나 FFLogs job gate에 쓰지 않는다.
   - 기존 UX와 worker job 범위를 보존한다.
   - 수집된 live listing에서 `generated_high_end`, `pf_category_high_end`, `cfc_excel_high_end`, `native_content_high_end`가 어떻게 갈라지는지 비교한다.
3. promotion 후보는 별도 검토한다.
   - listing visibility와 FFLogs badge/job allocation에는 `pf_category_high_end`가 가장 직접적인 신호다.
   - static duty metadata와 patch checklist에는 CFC fields plus ContentType/name heuristic이 더 적합하다.
   - `ContentInterface.IsHighEndDuty()`는 Lumina와 결과가 다를 때만 fallback 또는 검증 oracle로 고려한다.

## Cross-Project And Patch Pipeline Findings

### C1. Web Asset Generation Path Is Stale

Severity: High

Evidence:

- `RemotePartyFinderReborn-Server/scripts/generate-listing-data.js:80` writes to `RemotePartyFinderReborn-Server/assets/listing-data.js`.
- `RemotePartyFinderReborn-Web/server.js:8` serves `RemotePartyFinderReborn-Web/public`.
- `RemotePartyFinderReborn-Web/public/listings.html:13-15` consumes `/assets/translations.js`, `/assets/listing-data.js?v=...`, `/assets/listings.js?v=...`.
- `RemotePartyFinderReborn-Server/scripts/listings-live-refresh.spec.mjs:145-148` still reads the old server-side asset path.

Impact:

- 패치 후 script를 실행해도 실제 배포되는 Web asset이 갱신되지 않을 수 있다.
- `listing-data.js?v=...` version bump를 빼먹으면 Koyeb 배포 후에도 사용자가 stale duty data를 볼 수 있다.
- Playwright smoke test도 현재 Web split 구조를 정확히 검증하지 못할 가능성이 있다.

Direction:

- `generate-listing-data.js` 출력 대상을 Web public asset으로 명시하거나, 별도 `--out` 인자를 추가한다.
- Web의 HTML asset version과 generated file hash를 한 곳에서 갱신하는 script로 묶는다.
- Playwright script의 asset path를 `RemotePartyFinderReborn-Web/public/assets` 기준으로 바꾼다.
- CI 또는 local test에 “generated listing-data contains expected new duty”와 “HTML references current version” 검사를 추가한다.

### C2. Translation Generation Can Remove Runtime Keys

Severity: High

Evidence:

- `RemotePartyFinderReborn-Server/generator/Program.cs:61-121`, `169-220` generate `translations.js` from hard-coded IDs and manual entries.
- `RemotePartyFinderReborn-Web/public/assets/translations.js:22` contains `service_unavailable`.
- `RemotePartyFinderReborn-Web/public/assets/listings.js:1056-1062` uses `service_unavailable`.
- `Program.cs` does not contain that key.

Impact:

- generator를 다시 돌리면 Web 503 UI가 참조하는 번역 key가 사라질 수 있다.
- Web i18n runtime과 generator의 source of truth가 이미 drift 상태다.

Direction:

- Web-owned runtime translation keys를 generator source에 추가하거나, generated translations와 Web manual translations를 분리한다.
- `translations.js` generation 후 key allowlist 검증을 추가한다.
- 503 UI 다국어 표시를 Playwright MCP로 확인하는 smoke check를 유지한다.

### C3. `high_end` Flag Is An Unreliable Gate For Current Content

Severity: High

Evidence:

- `RemotePartyFinderReborn-Server/generator/Program.cs:379` writes `high_end` directly from `ContentFinderCondition.HighEndDuty`.
- Runtime gate: `RemotePartyFinderReborn-Server/src/domain/listing/types.rs:213-222`.
- FFLogs render gate: `RemotePartyFinderReborn-Server/src/listings_render.rs:418-423`, `500-505`.
- FFLogs job gate: `RemotePartyFinderReborn-Server/src/web/fflogs_ingest.rs:978-986`.
- Current runtime gate ignores `listing.category`; even `DutyCategory::HighEndDuty` uploaded by the Plugin does not affect `Listing.high_end()`.
- Plugin already sends the PF category:
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/UploadableListing.cs:20`, `45`.
  - `References/Dalamud/Dalamud/Game/Gui/PartyFinder/Types/PartyFinderListing.cs:93`.
  - `References/Dalamud/lib/FFXIVClientStructs/FFXIVClientStructs/FFXIV/Client/UI/Agent/AgentLookingForGroup.cs:155`.
- Recent high-end-like examples with `high_end: false`:
  - `src/ffxiv/duties.rs:7550-7558`: AAC Light-heavyweight M1 Savage.
  - `src/ffxiv/duties.rs:7650-7658`: Everkeep Extreme.
  - `src/ffxiv/duties.rs:7860-7868`: AAC Cruiserweight M1 Savage.
  - `src/ffxiv/duties.rs:7960-7968`: Recollection Extreme.
  - `src/ffxiv/duties.rs:8080-8088`: The Windward Wilds Extreme.
  - `src/ffxiv/duties.rs:8390-8398`: Hell on Rails Extreme.
- Current true examples:
  - `src/ffxiv/duties.rs:8630-8638`: The Unmaking Extreme.
  - `src/ffxiv/duties.rs:8650-8658`: Shinryu's Domain Unreal.

Impact:

- FFLogs mapping을 추가해도 `listing.high_end()`가 false면 수집/표시 경로가 켜지지 않는다.
- Web `state.highEnd` 기본 true와 결합되면 non-high-end로 분류된 listing이 화면에서 사라질 수 있다.
- PF listing이 실제 High-end Duty category로 올라왔더라도 서버가 generated duty metadata만 보므로 client-side category 신호가 버려진다.

Direction:

- 먼저 Plugin에서 `pf_category_high_end`, `cfc_excel_high_end`, optional `native_content_high_end`를 telemetry/debug metadata로 비교한다. 이 단계는 Web/FFLogs gate에 쓰지 않아 UX를 바꾸지 않는다.
- “PF listing category high-end”와 “RPF high-end target duty”를 분리한다.
- listing visibility/FFLogs job gate 후보는 `DutyCategory::HighEndDuty`를 우선 검토한다.
- static duty metadata 후보는 `ContentFinderCondition.HighEndDuty`, `ContentType`, `ContentUICategory`, duty name/difficulty heuristic, 필요 시 `ContentInterface.IsHighEndDuty()` 비교 결과를 함께 본다.
- Extreme, Savage, Ultimate, Chaotic, Unreal 여부와 FFLogs support 여부를 명시하는 generated or curated table을 둔다. 단, 실제 Web 필터 적용 전에는 영향 listing 목록을 제시하고 승인받아야 한다.
- 매 패치 검증에 “expected high-end duties all pass `listing.high_end()` or override gate” 테스트를 추가한다.

### C4. FFLogs Mapping Is Manual And Not Exhaustively Tested

Severity: High

Evidence:

- Mapping body: `RemotePartyFinderReborn-Server/src/infra/fflogs/mapping.rs:30-39`.
- Zone table: `mapping.rs:112-121`.
- Ultimate detector: `mapping.rs:142-145`.
- TODO holes remain:
  - `mapping.rs:69-77`: Dawntrail 7.2 Cruiserweight Savage.
  - `mapping.rs:80-88`: Dawntrail 7.0 Light-heavyweight Savage.
- Tests only cover selected duties:
  - `mapping.rs:177-210`: Enuo Extreme and Cloud of Darkness Chaotic.

Impact:

- 새 Extreme/Savage/Ultimate/Chaotic가 추가될 때 `DUTY_TO_FFLOGS`, `FFLOGS_ZONES`, `is_ultimate_encounter` 중 하나만 누락되어도 기능 일부가 조용히 빠진다.
- Missing mapping은 UI/worker 입장에서 “FFLogs unsupported”처럼 보인다.

Direction:

- `expected_fflogs_supported_duties` test fixture를 만들고 현재 expansion의 target duty를 전부 검사한다.
- Mapping data를 Rust code에 직접 박기보다 JSON/TOML or generated data로 분리한다.
- Ultimate-specific phase progress는 zone/encounter table과 함께 테스트한다.

### C5. `generate-listing-data.js` Parses Rust By Regex

Severity: Medium-High

Evidence:

- Regex definitions: `RemotePartyFinderReborn-Server/scripts/generate-listing-data.js:6-8`.
- Duty parse: `generate-listing-data.js:28-45`.
- Auto-translate parse: `generate-listing-data.js:58-68`.
- Source order: `generate-listing-data.js:72-73` reads generated `duties.rs`, then `src/ffxiv.rs`.
- Backend lookup uses generated first, old fallback: `RemotePartyFinderReborn-Server/src/ffxiv.rs:135-145`.

Impact:

- `LocalisedText` field order, Rust formatting, raw strings, generated header changes can break Web asset generation.
- If generated and old manual data share an ID later, Web and backend priority can diverge.

Direction:

- Have generator emit JSON directly for Web assets.
- If regex parser remains, add parser fixture tests using representative generated and manual blocks.
- Add duplicate ID check across generated and old data.

### C6. Generated Data Boundary Is Not Marked

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Server/generator/Program.cs:230-239` `DefaultHeader` emits only `use` lines, no generated-file warning.
- `RemotePartyFinderReborn-Server/src/ffxiv.rs:267-469` contains manual old fallback data in the same broad domain.

Impact:

- Future maintainers may patch generated files manually.
- Manual old fallback and generated data boundary is hidden.

Direction:

- Add generated-file header with generator command and “do not edit manually”.
- Move old fallback data to a clearly named module/file and test no ID collisions.

### C7. `DutyCategory::from_u32` Does Not Match Serialized Values

Severity: Medium-High

Evidence:

- `RemotePartyFinderReborn-Server/src/domain/listing/types.rs:267-284`: serialized enum values use `1 << n`, for example `HighEndDuty = 64`, `Fate = 512`.
- `RemotePartyFinderReborn-Server/src/domain/listing/types.rs:288-304`: `from_u32` maps `32 => HighEndDuty`, `256 => Fate`, etc. It is shifted one step lower than the enum values.
- `RemotePartyFinderReborn-Server/src/api/tests.rs:306`: API serialization expects `DutyCategory::HighEndDuty as u32`, i.e. 64.
- `RemotePartyFinderReborn-Server/src/domain/stats/summary.rs:105-109`: stats duty name decoding uses `DutyCategory::from_u32(self.info.1)`.
- `RemotePartyFinderReborn-Server/src/domain/stats/summary.rs:187-193`: stats grouping stores raw `$listing.category`.

Impact:

- Stats category/name decoding can misclassify non-normal duty categories.
- `DutyType::Normal` happens to ignore category in `ffxiv::duty_name`, so many high-end normal duties may still display correctly, but `Other`, roulette-adjacent, Gold Saucer, Fate, Treasure Hunt, Hunt, Deep Dungeon, Field Operation, Variant/Criterion category names can drift.
- Fixing this may visibly change stats labels, so it is a correctness fix with UX impact.

Direction:

- Replace manual `from_u32` mapping with `num_enum`/`TryFromPrimitive` or a match using the enum constants themselves.
- Add tests for every `DutyCategory` serialized value round-tripping through `from_u32`.
- Add a stats fixture covering at least High-end, Fate, Treasure Hunt, Deep Dungeon, and Variant/Criterion categories.

## Plugin Findings

### P1. Background Tasks Are Fire-And-Forget And Not Joined

Severity: High

Evidence:

- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Gatherer.cs:71-75`
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PlayerCollector.cs:57`, `123-124`
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PartyDetailCollector.cs:169-173`
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/FFLogsCollector.cs:342-348`, `400`
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/ContentIdEnrichment/IdentityUploadPump.cs:82`

Impact:

- Plugin unload 시 in-flight task가 남을 수 있다.
- `FFLogsCollector.Dispose()`가 cancellation 후 HTTP clients를 바로 dispose한다.
- `PlayerCollector.Dispose()`도 `_httpClient`를 dispose하지만 upload task는 await하지 않는다.
- Fault observation이 약해서 background exception이 실제 상태와 UI에 반영되지 않을 수 있다.

Direction:

- Shared `BackgroundTaskRunner` or pump abstraction 도입.
- `CancellationTokenSource`, stored `Task`, stop/join path, exception logging을 표준화한다.
- `volatile bool` start gate는 `Interlocked` or `SemaphoreSlim`로 대체한다.

### P2. FFLogs Transient Failures Are Classified As No Data

Severity: High

Evidence:

- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/FFLogsClient.cs:121`: OAuth token request has no worker cancellation token.
- `FFLogsClient.cs:227`: broad exception handling in query path.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/FFLogs/FFLogsBatchProcessor.cs:257-260`: `HadTransientFailure: false`.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/FFLogs/FFLogsBackgroundWorker.cs:97`, `127`, `137`: worker policy depends on `HadTransientFailure`.

Impact:

- Network failures, API hiccups, or cancellation can look like empty result/no work.
- Intended backoff path often does not run.
- Plugin unload can wait on token request timeout rather than cancel promptly.

Direction:

- Replace nullable/no-data returns with typed result: `Success`, `RateLimited`, `TransientFailure`, `PermanentFailure`, `Canceled`.
- Rethrow or explicitly propagate `OperationCanceledException`.
- Make `EnsureTokenAsync(CancellationToken)` and classify auth failures.

### P3. `HttpClient` Ownership Is Inconsistent

Severity: Medium-High

Evidence:

- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Gatherer.cs:24` creates a client, no dispose in `Dispose`.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PartyDetailCollector.cs:24` creates a client, no dispose in `Dispose`.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PlayerCollector.cs:29`, `44-46` disposes while uploads may run.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/FFLogsCollector.cs:312`, `338-348` owns multiple disposables.

Impact:

- Resource leak on some collectors.
- Dispose race on others.
- Timeout/cancellation policy differs by caller.

Direction:

- Centralize ingest HTTP client ownership.
- Dispose after task runner has stopped.
- Prefer per-operation cancellation over `HttpClient.Timeout` for controlled unload behavior.

### P4. Config UI Persistence Is Both Too Eager And Incomplete

Severity: Medium-High

Evidence:

- Frequent saves on input/slider frames:
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Ui/SettingsTabRenderer.cs:48`, `55`, `62`, `121`, `143`, `306-403`
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Ui/DebugTabRenderer.cs:44-89`
- Endpoint mutations without obvious `Save()`:
  - `SettingsTabRenderer.cs:190`, `200`, `231`
- `ContinueWith` UI mutation:
  - `SettingsTabRenderer.cs:222`, `227`

Impact:

- Text editing and slider dragging can write config repeatedly, including secrets.
- Endpoint add/delete/enable can mutate runtime state without persistence.
- `ContinueWith` runs on thread-pool and may mutate UI state after dispose.

Direction:

- Add debounced config save scheduler or apply-on-deactivate model.
- Route all persistent mutations through one helper.
- Replace delayed `ContinueWith` with expiry timestamp checked during `Draw()`.

### P5. Mutable Endpoint State Is Shared Across Threads

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Configuration.cs:19` mutable `UploadUrl`.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Gatherer.cs:139` upload worker reads/mutates endpoint state.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Ui/SettingsTabRenderer.cs:259` UI resets/mutates endpoint state.

Impact:

- Immutable list wrapper does not make inner `UploadUrl` objects safe.
- UI and background upload paths can race over failure counters, capability tokens, and enabled state.

Direction:

- Separate persistent endpoint config from transient circuit/capability state.
- Use endpoint state service with locks or immutable snapshots plus atomic update methods.

### P6. Patch-Sensitive Interop Is Hardcoded In Runtime Classes

Severity: Medium

Evidence:

- CharaCard packet offset and signatures:
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/ContentIdEnrichment/CharaCardResolverRuntime.cs:158`, `162-174`
- Party detail signatures and addon ID:
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PartyFinder/PartyDetailCaptureRuntime.cs:33-39`
- Player offsets:
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PlayerCollector.cs:18-19`
- Availability check always permissive:
  - `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/CharaCardResolver.cs:327`

Impact:

- Game patch signature drift appears at runtime and may disable broad features.
- Required vs optional hooks are not obvious.

Direction:

- Introduce `DalamudInteropVersionProfile` or feature-specific adapters.
- Startup diagnostics should report hook availability per feature.
- Fail narrowly by feature rather than broad resolver failure where possible.

### P7. Large Responsibility Classes Remain

Severity: Medium

Examples:

- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/CharaCardResolver.cs`: resolver state, suppression, identity upload, game UI suppression, world resolution.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/PartyFinder/PartyDetailCaptureRuntime.cs`: hooks, request cycles, scanner suppression, snapshot building.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/FFLogsClient.cs`: OAuth, GraphQL query construction, rate-limit handling, Tomestone enrichment adjacent concerns.
- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/Ui/SettingsTabRenderer.cs`: multiple settings sections plus validation/persistence behavior.

Direction:

- Split by behavior boundary rather than file length:
  - interop/hook adapter
  - state machine
  - upload transport
  - retry policy
  - DTO/query mapping
  - UI section renderer

### P8. Older GraphQL Query Path Does Not Use Escaping

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Plugin/RemotePartyFinderReborn/FFLogsClient.cs:248`

Impact:

- One older `FetchCharacterParsesBatchAsync` path inserts character/server/region directly into GraphQL.
- Newer methods use `EscapeGraphQlString`, so this is probably dead or legacy code, but risky if reused.

Direction:

- Remove unused path or route all GraphQL construction through a single variables/escaping helper.

## Web Findings

### W1. Static File Boundary Check Can Admit Sibling Directories

Severity: Critical

Evidence:

- `RemotePartyFinderReborn-Web/server.js:75-77`: `decodeURIComponent`, `path.join`, `path.normalize`, then string `startsWith(PUBLIC_DIR)`.
- `RemotePartyFinderReborn-Web/server.js:91-111`: `serveStatic` serves the computed path.

Impact:

- `PUBLIC_DIR` like `...\public` and sibling `...\public2` share the same string prefix.
- A path like `/../public2/secret.txt` can normalize outside public and still pass `startsWith(PUBLIC_DIR)`.
- Actual exploitability depends on sibling files, but public edge should not depend on directory layout.

Direction:

- Use `path.relative(PUBLIC_DIR, filePath)` and reject paths starting with `..` or absolute paths.
- Catch malformed `decodeURIComponent`.
- Add tests for `../public2`, encoded `%2e%2e`, Windows separator variants, and non-existing targets.

### W2. Locale Negotiation Is Incorrect And Cookie Parsing Can Throw

Severity: High

Evidence:

- `RemotePartyFinderReborn-Web/server.js:26-37`

Impact:

- `Accept-Language` q-values are ignored.
- `en-US,en;q=0.9,ja;q=0.1` can select Japanese because of simple `includes`.
- Malformed encoded `lang` cookie can throw during unavailable response selection.

Direction:

- Implement a small `Accept-Language` parser with q sorting and supported language filtering.
- Guard `decodeURIComponent`.
- Add server tests for q-values, order, malformed cookies, unsupported language fallback.

### W3. Normal Page Initial Language Ignores Browser Language

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/common.js:14-27`
- `RemotePartyFinderReborn-Web/public/assets/listings.js:369-379`
- `RemotePartyFinderReborn-Web/public/listings.html:34`
- `RemotePartyFinderReborn-Web/public/stats.html:32`

Impact:

- First visit defaults to English unless cookie exists.
- Server-side `Accept-Language` only affects unavailable response, not normal UI bootstrap.

Direction:

- Server can inject supported initial language or client can use `navigator.languages`.
- Add browser-level tests for first-load language with no cookie.

### W4. Asset Versioning And Cache Validators Are Incomplete

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Web/server.js:81-109`
- `RemotePartyFinderReborn-Web/public/listings.html:8-15`
- `RemotePartyFinderReborn-Web/public/stats.html:8-14`

Impact:

- `pico.css`, `minireset.css`, `translations.js`, `d3.js`, `stats-edge.js`, `stats.css` are unversioned or unevenly versioned.
- `no-cache` without ETag/Last-Modified causes repeated transfer or weak revalidation behavior.
- `d3.js` is about 277 KB and loaded before stats fetch outcome is known.

Direction:

- Apply consistent hashed query or filename versioning to all static assets.
- Add ETag/Last-Modified for non-versioned assets.
- Update tests to scan all HTML asset references, not just selected listing assets.

### W5. Duplicate And Dead Assets Exist

Severity: Low-Medium

Evidence:

- `public/assets/pico.css` and `public/assets/pico.min.css` are duplicates by reported file length/hash.
- `public/assets/d3.js` and `public/assets/d3.v7.min.js` are duplicates.
- `public/assets/list.min.js` appears unreferenced.
- Some assets contain source map comments but map files are absent.

Impact:

- Repository noise and potential 404 source map chatter.
- Unclear source of truth for vendor assets.

Direction:

- Keep one canonical file per vendor asset.
- Add static allowlist test for HTML-referenced assets.
- Remove or provide matching source maps intentionally.

### W6. `localStorage` Is Unguarded And Key Is Too Generic

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/listings.js:335-366`
- `RemotePartyFinderReborn-Web/public/assets/common.js:29-38`

Impact:

- Browser privacy mode, blocked storage, or quota errors can break initialization or hide persistence.
- Key `state` is too broad and can collide with future pages.

Direction:

- Wrap storage access in safe helpers.
- Namespace keys, for example `rpf:listings:state`.
- Add unit tests with throwing `localStorage`.

### W7. Expiration Countdown Uses A Fixed Relative Value

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/listings.js:507-515`
- `RemotePartyFinderReborn-Web/public/assets/listings.js:760-763`

Impact:

- `time_left_seconds` is rendered into `data-expires-in`, then reused unchanged.
- Without snapshot refresh, a listing can keep showing “5 minutes” rather than decreasing.

Direction:

- Store absolute expiry timestamp derived from fetch/render time.
- `updateTimeDisplays()` should compute remaining seconds from current time.
- Add fake-clock test.

### W8. High-End Filter State Has No UI Control

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/listings.js:8-24`, `786-788`, `822-824`
- `RemotePartyFinderReborn-Web/public/listings.html:240`
- `RemotePartyFinderReborn-Web/public/assets/listings.css:1266-1279`

Impact:

- `state.highEnd` defaults true and filter path always applies, but the UI toggle appears absent.
- Non-high-end listings can be invisible with no user control.
- This interacts badly with server `high_end` misclassification.

Direction:

- Product behavior is now decided: always high-end-only.
- Do not restore a user-facing toggle.
- Any cleanup should remove dead toggle state/CSS only if it keeps current visible output unchanged.
- Be careful with stale `localStorage` values such as `state.highEnd=false`; forcing high-end-only may be the desired product behavior, but it can still change listing visibility for those users and should be listed as UX-impacting before implementation.

### W9. Filter Counts Do Not Reflect Active Filters

Severity: Low-Medium

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/listings.js:252-277`
- `RemotePartyFinderReborn-Web/public/assets/listings.js:782-801`

Impact:

- Data center counts are based on broader snapshot/high-end set, not current search/content filters.
- Users can see misleading count numbers after narrowing filters.

Direction:

- Decide whether counts mean global availability or currently filtered availability.
- Encode that expectation in tests.

### W10. Listings Render Path Is Monolithic And Costly

Severity: Medium-High

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/listings.js:713`: `renderListingCard`.
- `listings.js:996-1009`: page render clears and rebuilds cards with `innerHTML`.
- `listings.js:1012-1016`: filter/render path.
- `listings.js:1188-1197`: search debounce.
- `RemotePartyFinderReborn-Web/public/assets/listing-data.js:1`: large generated data, around 790 KB.

Impact:

- Every filter/search/live refresh can rescan snapshot and replace 50 cards.
- `applyTranslations()` and time update run again after render.
- Larger snapshots will increase main-thread cost.

Direction:

- Split pure filtering, pagination, card view model, and DOM patching.
- Cache normalized listing search fields.
- Consider lazy-loading listing-data or narrowing generated data.
- Add large fixture performance budget test for filter and refresh.

### W11. Hardcoded English Text Remains In Rendered UI

Severity: Medium

Evidence examples:

- `RemotePartyFinderReborn-Web/public/assets/listings.js:532-545`
- `listings.js:570-610`
- `listings.js:664-669`
- `listings.js:746-766`
- `listings.js:933-938`
- `RemotePartyFinderReborn-Web/public/listings.html:48-70`, `78-81`, `332-349`

Impact:

- Switching language can produce mixed English/Japanese/German/French UI.
- Some ARIA/title/header strings are not covered by translation table.

Direction:

- Move render-time labels into translation keys.
- Add DOM test with `state.lang='ja'` or browser-level i18n smoke.

### W12. Stats Pipeline Uses DOM As Data Source

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/stats-edge.js:25-28`, `57-67`
- `RemotePartyFinderReborn-Web/public/assets/stats.js:88-105`, `315-349`

Impact:

- `stats-edge` fetches JSON, renders table HTML, then `stats.js` reads table DOM back into chart data.
- Entity handling can diverge, for example `A&B` vs `A&amp;B`.
- Testing pure data transforms is harder.

Direction:

- Keep JSON as source of truth and pass structured data to chart builders.
- Render table and chart from the same normalized model.
- Add special-character fixture tests.

### W13. Stats Empty/Error States Are Thin

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Web/public/assets/stats-edge.js:47-67`
- `RemotePartyFinderReborn-Web/public/assets/stats.js:141-245`, `247-306`
- Current added test only guards `makeBarPlot([])`.

Impact:

- Fetch failure and valid empty dataset are not clearly distinguished.
- D3 loads even when stats fetch fails.
- Treemap empty behavior lacks regression coverage.

Direction:

- Add empty dataset UI state.
- Load D3/chart code after confirming chartable data, or accept preload intentionally and document.
- Export/test transform helpers for bar and treemap.

## Server Findings

### S1. Background Tasks Can Retry Too Aggressively On Failure

Severity: High

Evidence:

- `RemotePartyFinderReborn-Server/src/web/background.rs:38-51`: stats task `continue`s before the 12-hour sleep on error.
- `RemotePartyFinderReborn-Server/src/web/background.rs:108-126`, `164-171`: revision publisher/materialized revision paths can re-notify quickly after increment failure.

Impact:

- Mongo/stat outage can turn into tight retry loop.
- Logs and DB pressure can spike during an outage.

Direction:

- Shared background task policy with bounded exponential backoff and jitter.
- Ensure every loop has a failure sleep path.
- Add tests for outage cadence, not just success behavior.

### S2. Mongo Cursor And Decode Errors Are Often Silently Dropped

Severity: High

Evidence:

- `RemotePartyFinderReborn-Server/src/infra/mongo/listings.rs:106-111`
- `RemotePartyFinderReborn-Server/src/infra/mongo/parse_cache.rs:63-65`, `87-89`
- `RemotePartyFinderReborn-Server/src/infra/mongo/report_parse.rs:30-32`
- `RemotePartyFinderReborn-Server/src/infra/mongo/players.rs:236-242`, `280-282`
- `RemotePartyFinderReborn-Server/src/web/mod.rs:736-741`: duplicate cleanup path also skips read/deserialize errors.

Impact:

- Partial data can be returned as success.
- Listing, parse cache, report summary, active player, or duplicate cleanup corruption can be hidden.
- Startup index migration/dedupe should fail fast rather than silently skip.

Direction:

- Use `TryStreamExt::try_collect` for critical reads.
- For non-critical enrichment, return partial-result type with skipped/error counters and warn-level logs.
- Startup maintenance operations should fail startup on cursor/decode errors.

### S3. FFLogs Job Allocation Is N+1 And Hides DB Failures

Severity: High

Evidence:

- `RemotePartyFinderReborn-Server/src/web/fflogs_ingest.rs:957-1061`.
- Top-level listing read failure returns empty jobs: `fflogs_ingest.rs:958-960`.
- Per-listing cache/player failures `continue`: `fflogs_ingest.rs:1003-1010`, `1056-1061`.

Impact:

- Workers cannot distinguish “no work” from “server could not inspect work”.
- Active PF load can cause repeated per-listing DB calls:
  - zone cache lookup
  - final failure lookup
  - player lookup
- Job starvation can be quiet.

Direction:

- Split allocation into phases:
  - collect mapped high-end listings
  - collect unique `(zone_key, content_id)` candidates
  - batch cache/failure/player reads
  - allocate jobs from normalized candidate set
- Return 503 or structured degraded response for top-level dependency failure.
- Add metrics/log dimensions for skipped allocation.

### S4. FFLogs Leases Are In-Memory Only

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Server/src/web/fflogs_ingest.rs:1094-1137`
- State fields include lease maps in `RemotePartyFinderReborn-Server/src/web/mod.rs:57-115`.

Impact:

- Duplicate dispatch is prevented only within one process.
- Multi-process or multi-instance deployment can duplicate jobs.

Direction:

- If single-process Pi origin is intentional, document it near config/deployment.
- If multi-instance is possible, move leases to Mongo with TTL and unique key semantics.

### S5. `State` Is A Broad Public Service Locator

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Server/src/web/mod.rs:57-115`
- Many tests manually construct `State`, for example `src/web/test_support.rs:20`.

Impact:

- Handlers/background tasks depend on unrelated fields.
- Tests require large fixture construction.
- Refactors are harder because state ownership is unclear.

Direction:

- Split into narrower structs:
  - `DbCollections`
  - `ListingRevisionState`
  - `SnapshotState`
  - `IngestSecurityState`
  - `FflogsLeaseState`
  - `RuntimeMetrics`
- Pass narrow dependencies into handlers and background tasks.

### S6. Listing Render Enrichment Is Too Concentrated

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Server/src/listings_render.rs:361-667`
- Player lookup: `listings_render.rs:396`
- Report summary fallback suppression: `listings_render.rs:477-485`

Impact:

- One function owns listing load, sorting, player lookup, parse cache lookup, report summary lookup, fallback identity resolution, display resolution, and metrics.
- Per-zone report summary errors can degrade to no fallback summaries.

Direction:

- Introduce `ListingEnrichmentContext` from batched inputs.
- Split pure member rendering and parse display resolution.
- Report lookup should account for per-zone errors instead of `.unwrap_or_default()`.

### S7. Contributor Ingest And Mongo Upsert Reporting Duplicate Logic

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Server/src/web/contribute.rs:215-347`, `350-470`
- `RemotePartyFinderReborn-Server/src/infra/mongo/players.rs:39-221`
- `RemotePartyFinderReborn-Server/src/infra/mongo/identities.rs:32-303`

Impact:

- Status mapping, logging, response construction, validation counters, retry loops, and report shapes can drift.

Direction:

- Extract response/report builders for player/identity upsert.
- Keep domain-specific merge logic separate from generic batch execution/reporting.

### S8. FFLogs Zone `partition` Is Stored But Not In Cache Key

Severity: Medium

Evidence:

- `RemotePartyFinderReborn-Server/src/infra/fflogs/cache.rs:9-16`
- `RemotePartyFinderReborn-Server/src/infra/fflogs/mapping.rs:111-120`
- Current cache key only uses zone and difficulty via `make_zone_cache_key`.

Impact:

- If FFLogs partitions become meaningful for same zone/difficulty, cache entries can collide or stale data can be reused.

Direction:

- Either remove `partition` if intentionally unused, or introduce typed `FflogsZoneKey` including partition with compatibility migration.

## Test Gaps

### Plugin

- In-flight disposal/lifecycle tests for `Gatherer`, `PlayerCollector`, `PartyDetailCollector`, `FFLogsCollector`, `IdentityUploadPump`.
- Cancellation propagation through FFLogs OAuth, GraphQL, ingest upload, Tomestone calls, detail upload, identity upload.
- FFLogs transient failures setting `HadTransientFailure` and triggering worker backoff.
- High-end signal comparison test/diagnostic around `IPartyFinderListing.Category`, `RawDuty`, `DutyType`, and optional CFC lookup.
- Config persistence tests for endpoint add/delete/enable and debounced text/slider saves.
- Interop diagnostics tests where possible, plus manual in-game patch checklist for signatures/offsets.

### Web

- Static path traversal and sibling directory rejection.
- `Accept-Language` q-values, malformed cookie, unsupported fallback.
- No-cookie first-load language with browser language via Playwright MCP.
- All HTML asset references have cache strategy/version/validator coverage.
- `localStorage` throwing or quota failure.
- Expiration countdown with fake clock.
- High-end-only behavior decision encoded as test.
- Data center counts under active filters.
- Large snapshot filter/render performance budget.
- Stats special-character label, empty dataset, treemap empty behavior.
- Playwright MCP smoke for `/listings`, `/stats`, 503 shell, i18n, cache-busted assets.

### Server

- Mongo cursor item error and BSON decode failure handling.
- Background task failure backoff cadence.
- FFLogs job allocation integration with multi-listing and dependency failures.
- Exhaustive current high-end duty to FFLogs mapping expectations.
- No-UX-change comparison fixture for generated `high_end` vs uploaded PF category high-end metadata before changing gates.
- `DutyCategory::from_u32` round-trip coverage for every serialized enum value.
- Generated vs old duty/roulette duplicate ID test.
- `generate-listing-data` output path and Web asset inclusion test.
- `translations.js` required Web runtime key presence.

## Prioritization Matrix

### Must Review Before Planning

1. Web `server.js` static path boundary check.
2. Patch pipeline output paths: `listing-data.js`, `translations.js`, Playwright script asset path.
3. `high_end` source of truth and client-side signal comparison policy for Extreme/Savage/Ultimate/Chaotic.
4. Plugin FFLogs transient failure classification and background task lifecycle.
5. Server background failure backoff and Mongo cursor error policy.
6. `DutyCategory::from_u32` mismatch impact on stats labels.

### Good First Implementation Batch

These are narrow enough to implement with tests before larger refactors.

- Web path boundary fix plus tests.
- `Accept-Language` parser and malformed cookie guard.
- Safe storage wrapper for Web `localStorage`.
- Server stats task failure sleep/backoff.
- Server cursor error policy for one critical repository, likely `get_current_listings`.
- Server `DutyCategory::from_u32` round-trip fix with stats fixture, if visible stats label changes are accepted.
- Plugin `FFLogsBatchProcessor` transient status propagation test first, then typed client result.
- Generator output path explicit option or script fix for Web public asset.
- Translation required-key test.

### Larger Refactor Batches

- Plugin background runner and HTTP client ownership standardization.
- Server FFLogs job allocator phase split and batching.
- Server `State` split.
- Listings render enrichment context split.
- Web listings module decomposition and large-snapshot render optimization.
- Stats JSON-to-model-to-table/chart restructure.
- FFLogs mapping data source and exhaustive patch checklist automation.

## Open Questions For Review

1. Should current accidental Web/Server changes stay as the baseline for the refactor branch, or should they be split into a preliminary cleanup commit before new work?
2. Resolved: the product is intentionally high-end-only in Web listings; do not restore the high-end toggle.
3. Should RPF define its own high-end target table independent of Lumina `HighEndDuty`, or promote uploaded PF `Category.HighEndDuty` for live listing gates?
4. What is the official patch generation procedure for Web assets after the server/web split?
5. Is server deployment guaranteed single-process on Pi, or should FFLogs leases be made multi-instance-safe?
6. For static assets, do we prefer query-string versions, hashed filenames, or validator headers?
7. Should generator output be treated as a release artifact copied into Web, or should Web generate/consume it from Server source at build time?
8. For high-end client-side detection, do we accept a telemetry-only plugin change first, or do we require in-game Playwright-equivalent/manual capture evidence before even adding upload fields?

## Suggested Next Step

Before implementation planning, decide the answers to the open questions above. Then produce a staged plan with explicit test gates:

1. Stabilization guardrails: Web path safety, locale parsing, server backoff, patch asset path checks.
2. Patch pipeline correctness: generated data, high-end client signal comparison, override policy, FFLogs mapping coverage.
3. Runtime lifecycle correctness: Plugin task runner, cancellation, HTTP ownership, FFLogs transient classification.
4. Server reliability: cursor error policy, FFLogs allocator batching, listing render enrichment split.
5. Web maintainability/performance: listings module split, stats data model split, asset cleanup.
