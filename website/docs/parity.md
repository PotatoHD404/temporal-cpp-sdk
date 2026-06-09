---
title: Capabilities & parity
description: An honest, itemized matrix of what works vs. the official Temporal SDKs.
---

# Capabilities & parity

**This SDK is not at parity with the official Temporal SDKs, and won't be for a long time.** The
official Go/Java/Python/TS/.NET SDKs are years of work by teams; Temporal's Go engine alone is tens
of thousands of lines, and the full surface (schedules, Nexus, versioning, the test framework,
security, observability, the operator/cloud client…) is enormous.

What this project *is*: a genuinely working, **fully-tested core** of the Temporal programming model —
enough to write and run real workflows that orchestrate activities, react to signals/queries/updates,
compose child workflows and selectors, and continue-as-new — on a native C++ engine with a sticky
cache. This page is the honest accounting.

**Legend:** ✅ implemented & tested · 🟡 partial · ❌ not implemented

## Client

| Capability | Status | Notes |
|---|---|---|
| Connect (insecure) | ✅ | |
| Start workflow | ✅ | id, task queue, timeouts, retry policy |
| Await result | ✅ | follows continue-as-new chains |
| Signal / Query / Update | ✅ | synchronous query & update |
| Cancel / Terminate | ✅ | |
| Get handle to existing run | ✅ | |
| Signal-with-start | ✅ | `Client::SignalWithStartWorkflow` |
| List / count / describe workflows | ✅ | `Describe`, `ListWorkflows`, `CountWorkflows` (visibility query) |
| Reset workflow | ✅ | `Client::ResetWorkflow` (ResetWorkflowExecution); e2e-verified |
| Batch operations | ✅ | `StartBatchTerminate`/`StartBatchCancel` + `Describe`/`List`; e2e-verified |
| Schedules client | ✅ | create / describe / delete / update / list / trigger / pause / unpause (interval spec) |
| Operator service | 🟡 | search-attribute add/list/remove + cluster info/list ✅ e2e; remote-cluster + namespace admin ❌ |
| Cloud service | ❌ | cloud proto not vendored |

## Worker

| Capability | Status | Notes |
|---|---|---|
| Register workflows / activities | ✅ | plain `R(Context&, Args...)` functions |
| Poller threads, start/stop/run | ✅ | |
| Sticky cache (resident workflows) | ✅ | incremental-history continuations |
| Bounded cache LRU / eviction tuning | ✅ | `max_cached_workflows` (LRU eviction) |
| Concurrent-execution caps | ✅ | `max_concurrent_activity/workflow_task_executions` enforced by a gate; e2e-verified |
| Rate limiting (per-second) | ✅ | per-worker activity-per-second throttle (`max_activities_per_second`, token bucket); e2e-verified |
| Graceful drain | ✅ | `graceful_shutdown_timeout`; Stop() drains in-flight tasks; e2e-verified |
| Poller autoscaling | 🟡 | conservative idle-park within the fixed poller bounds; not true elasticity |
| Worker Build-ID compatibility (v0.1) | ✅ | `Get/Update/PromoteWorkerBuildIdCompatibility`; e2e-verified |
| Worker versioning rules / deployments | 🟡 | assignment rules (`InsertWorkerAssignmentRule`/`GetWorkerVersioningRules`) ✅ e2e; redirect rules + deployments ❌ |
| Session workers | 🟡 | host-unique session-queue routing + cap; no session lifecycle (create/complete/pin) |

## Workflow authoring

| Capability | Status | Notes |
|---|---|---|
| Execute activity (typed, parallel) | ✅ | |
| Timers (`Sleep` / `NewTimer`) | ✅ | |
| Signals (channels, buffered/ordered) | ✅ | |
| Queries (`SetQueryHandler`) | ✅ | live-state, read-only |
| Updates (`SetUpdateHandler`) | ✅ | accept + complete on the live path |
| Update validators | ✅ | read-only validator; rejection is ephemeral (no history entry) |
| Selectors | ✅ | future cases + signal-channel receive cases (`AddReceive`) |
| Child workflows | ✅ | basic + cancellation; no parent-close-policy / signal-child |
| Continue-as-new | ✅ | |
| Observe cancellation (`IsCancelled`) | ✅ | |
| Cancellation scopes / propagation | ✅ | `AwaitCancellation` + timer / activity / child-workflow `Future::Cancel` |
| `GetVersion` / patching | ✅ | marker-based; `kDefaultVersion` on pre-version history |
| SideEffect / MutableSideEffect | ✅ | both marker-based; `MutableSideEffect` is id-keyed and records only when the value changes |
| Local activities | ✅ | `ExecuteLocalActivity` runs registered activities inline (marker record/replay) with inline retry; e2e-verified |
| External-workflow signal/cancel | ✅ | `CancelExternalWorkflow` + `SignalExternalWorkflow` |
| Search attributes / memo / upsert | ✅ | memo ✅; start-time + workflow `UpsertSearchAttributes` ✅ (`sa::` typed helpers) |
| Header / context propagation | ✅ | start headers readable in the workflow + auto-propagated to activities |

## Activities

| Capability | Status | Notes |
|---|---|---|
| Typed execution | ✅ | |
| Server-driven retries (`RetryPolicy`) | ✅ | |
| Application errors (retryable / not) | ✅ | |
| Heartbeating | ✅ | `Context::IsCancelled` observes the server's cancel; throttling ❌ |
| Async (manual) completion | ✅ | `Context::SetWillCompleteAsync` + `Client::CompleteActivity`/`FailActivity` (by task token) |
| Activity-side cancellation | ✅ | workflow `Future::Cancel` → `RequestCancelActivityTask`; activity sees it via `Context::IsCancelled` |

## Data & serialization

| Capability | Status | Notes |
|---|---|---|
| JSON / nil / bytes converters | ✅ | nlohmann-json default stack |
| Custom converters | ✅ | |
| Proto / ProtoJSON converters | ✅ | binary protobuf + proto-json (`WithProtoJson`), both directions; unit-tested |
| Payload codecs (encryption/compression) | 🟡 | `PayloadCodec` interface + chain applied to every payload + base64 reference codec; no bundled encryption/compression codec |
| Custom failure converter | 🟡 | `FailureConverter` interface + `DefaultFailureConverter` + `DataConverter` hook (application-failure round-trips losslessly); not yet wired into the live error path |
| Large-payload / external storage | 🟡 | `PayloadStorage` interface + in-memory reference impl; no real external store (S3/GCS) bundled |

## Determinism & safety

| Capability | Status | Notes |
|---|---|---|
| Stackful-coroutine dispatcher | ✅ | |
| Sticky cache + incremental history | ✅ | |
| Non-determinism detection | ✅ | replayed commands matched to history in order; `WorkflowPanicPolicy` (block/fail) |
| Replay re-application of updates | ❌ | matters only after a cache eviction |
| History pagination | ✅ | workflow-task / query / export paths assemble paged histories via `next_page_token` |
| Deadlock detection / panic policies | ❌ | |

## Security, observability, ecosystem

| Capability | Status | Notes |
|---|---|---|
| TLS / mTLS / API-key auth | 🟡 | implemented (`ClientOptions::tls` + `api_key`, SslCredentials + per-call auth); **e2e-unverified locally** — no TLS Temporal server in the harness |
| Interceptors (client + worker) | 🟡 | chain framework (workflow/activity/client inbound+outbound) + base no-ops, unit-tested; not yet wired into live call paths |
| Metrics | 🟡 | `MetricsHandler` (counter/gauge/timer): task counters + execution-latency timers + in-flight gauge + poll success/timeout counters; e2e-verified; not the full Go metric set |
| Tracing / OpenTelemetry | 🟡 | `Tracer`/`Span` interface + `TracingInterceptor` (inject/extract via headers), unit-tested; no OTel exporter bundled; not yet wired |
| Structured logging | ✅ | pluggable `log::Logger` |
| Test framework (time-skip, replayer) | 🟡 | replayer ✅ (`Worker::ReplayWorkflowHistory`); time-skip ❌ |
| Schedules | ✅ | full client lifecycle (create/describe/delete/update/list/trigger/pause); calendar/cron specs ❌ |
| Nexus operations | ❌ | |

## Roadmap {#roadmap}

Rough priority order (see the repo's `docs/ROADMAP.md` for detail):

1. **Determinism hardening** — ✅ non-determinism detection + bounded sticky-cache LRU
   (`max_cached_workflows`) + heartbeat cancel-detection (`activity::Context::IsCancelled`);
   remaining: history pagination, heartbeat throttling.
2. **Workflow feature surface** — ✅ `SideEffect` + `GetVersion` + update validators + cancellation
   (timer, activity, child-workflow, `AwaitCancellation`) + selector channel cases; remaining:
   MutableSideEffect, local activities.
3. **Production concerns** — TLS/mTLS + API-key auth, interceptors, metrics & tracing,
   proto/protoJSON converters + payload codecs, worker tuning.
4. **Breadth** — ✅ replay/test framework + ✅ schedules (full client lifecycle); remaining: Nexus,
   worker versioning, calendar/cron schedule specs, the broader client surface.

If a capability you need is in the ❌ column, it genuinely isn't there yet — please don't assume
otherwise from the working core.
