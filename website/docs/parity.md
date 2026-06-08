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
| Reset / batch operations | ❌ | |
| Schedules client | 🟡 | create / describe / delete (interval + start-workflow); update/list/trigger/pause ❌ |
| Operator & Cloud services | ❌ | |

## Worker

| Capability | Status | Notes |
|---|---|---|
| Register workflows / activities | ✅ | plain `R(Context&, Args...)` functions |
| Poller threads, start/stop/run | ✅ | |
| Sticky cache (resident workflows) | ✅ | incremental-history continuations |
| Bounded cache LRU / eviction tuning | ✅ | `max_cached_workflows` (LRU eviction) |
| Concurrency / rate limiting (wired) | 🟡 | options exist; not all enforced |
| Poller autoscaling, graceful drain | ❌ | |
| Worker versioning / Build IDs / deployments | ❌ | |
| Session workers | ❌ | |

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
| SideEffect / MutableSideEffect | 🟡 | `SideEffect` ✅ (marker record/replay); MutableSideEffect ❌ |
| Local activities | ❌ | |
| External-workflow signal/cancel | ✅ | `CancelExternalWorkflow` + `SignalExternalWorkflow` |
| Search attributes / memo / upsert | 🟡 | memo ✅ (start + `Describe`); search attributes / upsert ❌ |
| Header / context propagation | ❌ | |

## Activities

| Capability | Status | Notes |
|---|---|---|
| Typed execution | ✅ | |
| Server-driven retries (`RetryPolicy`) | ✅ | |
| Application errors (retryable / not) | ✅ | |
| Heartbeating | ✅ | `Context::IsCancelled` observes the server's cancel; throttling ❌ |
| Async (manual) completion | ❌ | |
| Activity-side cancellation | ✅ | workflow `Future::Cancel` → `RequestCancelActivityTask`; activity sees it via `Context::IsCancelled` |

## Data & serialization

| Capability | Status | Notes |
|---|---|---|
| JSON / nil / bytes converters | ✅ | nlohmann-json default stack |
| Custom converters | ✅ | |
| Proto / ProtoJSON converters | ❌ | |
| Payload codecs (encryption/compression) | ❌ | |
| Custom failure converter | ❌ | |
| Large-payload / external storage | ❌ | |

## Determinism & safety

| Capability | Status | Notes |
|---|---|---|
| Stackful-coroutine dispatcher | ✅ | |
| Sticky cache + incremental history | ✅ | |
| Non-determinism detection | ✅ | replayed commands matched to history in order; `WorkflowPanicPolicy` (block/fail) |
| Replay re-application of updates | ❌ | matters only after a cache eviction |
| History pagination | ❌ | long histories not paged |
| Deadlock detection / panic policies | ❌ | |

## Security, observability, ecosystem

| Capability | Status | Notes |
|---|---|---|
| TLS / mTLS / API-key auth | ❌ | insecure only |
| Interceptors (client + worker) | ❌ | |
| Metrics | ❌ | |
| Tracing / OpenTelemetry | ❌ | |
| Structured logging | ✅ | pluggable `log::Logger` |
| Test framework (time-skip, replayer) | 🟡 | replayer ✅ (`Worker::ReplayWorkflowHistory`); time-skip ❌ |
| Schedules | 🟡 | create / describe / delete via the client |
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
4. **Breadth** — ✅ replay/test framework + 🟡 schedules (create/describe/delete); remaining: fuller
   schedules (update/list/trigger/pause), Nexus, worker versioning, search attributes, the broader
   client surface.

If a capability you need is in the ❌ column, it genuinely isn't there yet — please don't assume
otherwise from the working core.
