---
title: Architecture
description: How the native engine executes workflows — coroutine dispatcher and sticky cache.
---

# Architecture

`temporal-cpp-sdk` is a **native** SDK: it speaks gRPC to the Temporal frontend directly and implements
the determinism-critical workflow replay engine itself, mirroring the Go SDK's structure. There is no
Rust `sdk-core`.

## Layers

```
include/temporal/        Public API (proto-free)
  client/  worker/  workflow/  activity/  converter/  common/  log/
src/
  client/  worker/        Thin public-facing implementations
  converter/  log/        Data conversion, structured logging
  internal/               The engine (never in public headers):
    grpc_client           WorkflowService stub wrapper
    coroutine             Stackful thread-fiber coroutine
    workflow_task_handler Replay engine, sticky cache, the runner
    activity_task_handler Activity execution + heartbeating
    worker_impl           Poller threads + lifecycle
    proto_util            Payload/Failure/Duration <-> protobuf, ids
```

A hard rule: **public headers never include protobuf** — the gRPC/proto surface lives entirely under
`src/internal/`.

## The determinism problem

A workflow must be **deterministic and replayable**: its event history is the source of truth, and
the SDK must reconstruct the workflow's exact in-memory state by replaying that history. Every
orchestration call is intercepted, turned into a *command* for the server, and later matched back
against the resulting history events. This is the hard, native part of any SDK.

## Stackful coroutine dispatcher

The workflow body runs on a **stackful coroutine** — a cooperative thread-fiber driven by a strict
turn-based handshake, so the dispatcher and the workflow thread never run concurrently. (It's the C++
analogue of the goroutine-based dispatcher the Go SDK uses.)

Awaiting an unresolved future or signal **yields** the coroutine — preserving the workflow's entire
stack and locals — rather than unwinding it. This is what lets **queries** read live state and
**selectors** choose among live futures. Teardown (cache eviction or completion) unwinds the
suspended frame cleanly.

## Sticky cache

A running workflow's coroutine is kept resident between tasks, keyed by run id. The worker advertises
a unique **sticky task queue** in every response and polls it with a second poller, so the server
delivers continuation tasks carrying only the **incremental** history:

- **Continuation** (the task continues the cached state): apply only the new events to the live
  futures / signal state, then resume the coroutine — **no full re-replay**.
- **Replay** (first task, or the history doesn't continue the cache): rebuild from full history, run
  to the current suspension point, and cache the runner.
- **Sticky-cache miss** (incremental history but nothing cached): reply `RESET_STICKY_TASK_QUEUE` so
  the server resends full history.

The cache is a bounded LRU: `WorkerOptions::max_cached_workflows` caps the resident set (`0` =
unbounded), and once full the least-recently-used runner is evicted — its next task replays from full
history. `Worker::cache_hits()` / `replays()` expose the continuation-vs-replay split.

## Activations → commands

Each task feeds history-derived state to the running workflow, which emits commands
(`ScheduleActivityTask`, `StartTimer`, `StartChildWorkflowExecution`, `CompleteWorkflowExecution`,
`ContinueAsNewWorkflowExecution`, …). Updates flow as protocol messages
(`Acceptance` / `Response`). This is the same activation→command model the core-based SDKs use, here
computed natively in C++.

## Replay re-application of updates {#replay-re-application-of-updates}

On a sticky **continuation** the engine just applies the new events to live futures and resumes, so
update-mutated state is already in memory. A from-scratch **replay** (cold start, sticky-cache
eviction, or `Worker::ReplayWorkflowHistory`) is harder: the prescan resolves every history-known
future up front, so a single naive resume would race the workflow body to completion *before* any
accepted update handler re-runs — and the body would never observe the state an update set.

To reconstruct the live interleaving, the prescan splits history at the **first accepted update**.
Everything before it is fast-forwarded by the initial resume as usual. Everything *after* it — every
future-resolution and delivered signal — is **deferred** into a `post_update_timeline` instead of
being pre-applied. The engine then replays that timeline in **history order**, with the accepted
updates spliced back in at their recorded positions: each update re-runs its handler, each deferred
event resolves a future or delivers a signal, and a resume after each step lets the body advance to
its next park. State an update mutated — a flag the body `Await`s on, a counter a later signal
completes — is rebuilt exactly as it was live.

Because emitted commands are guarded to fire once, re-running a handler or re-resolving a future
never re-schedules work; it only advances the body and the produced-command order the determinism
check compares against. **Markers / side-effects are *not* deferred** — they are consumed in call
order, independent of event timing. For the common update-free workflow the timeline is empty, so
replay stays the single-resume fast-forward, unchanged.

## Deadlock abort

Workflow code must never block. The dispatcher resumes each coroutine under an optional per-resume
bound (`WorkerOptions::deadlock_detection_timeout`): the resume is `ResumeFor(timeout)`, and a task
that fails to yield within it flags the runner as deadlocked. The handler then **aborts** — it emits
the `temporal_workflow_task_deadlock` metric, logs an error, drops the run from the cache, and fails
the workflow *task* so the server reschedules it.

The runaway coroutine cannot be safely interrupted, so it is **abandoned**: it keeps running on its
own detached thread, and its runner is moved into a process-lifetime leak list so that thread never
dereferences freed memory. The poller thread returns immediately and keeps serving other workflows.
This is the same trade-off the Go SDK makes (a deadlocked workflow goroutine is leaked). See
[Running in production](/production#deadlock-detection) for the operational view.

## What's deliberately not here (yet)

Determinism detection, workflow versioning (`GetVersion`/patching), side effects
(`SideEffect`/`MutableSideEffect`), and a bounded sticky-cache LRU (`max_cached_workflows`) — once
listed here as future work — are now implemented (see [Sticky cache](#sticky-cache) and the
[parity matrix](/parity)). The remaining engine-level gaps (TLS, full metric/trace emission, worker
versioning routing, payload codecs, …) are tracked in the [parity matrix](/parity#roadmap). The
repository's [`docs/ARCHITECTURE.md`](https://github.com/) contains the same design at file/symbol
granularity.
