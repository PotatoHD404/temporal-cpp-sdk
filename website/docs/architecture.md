---
title: Architecture
description: How the native engine executes workflows — coroutine dispatcher and sticky cache.
---

# Architecture

`temporal-cpp` is a **native** SDK: it speaks gRPC to the Temporal frontend directly and implements
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

`Worker::cache_hits()` / `replays()` expose the split.

## Activations → commands

Each task feeds history-derived state to the running workflow, which emits commands
(`ScheduleActivityTask`, `StartTimer`, `StartChildWorkflowExecution`, `CompleteWorkflowExecution`,
`ContinueAsNewWorkflowExecution`, …). Updates flow as protocol messages
(`Acceptance` / `Response`). This is the same activation→command model the core-based SDKs use, here
computed natively in C++.

## What's deliberately not here (yet)

- **Non-determinism detection** — emitted commands are not yet compared against replayed history.
- **Workflow versioning** (`GetVersion`/patching) and **side effects**.
- **A bounded sticky-cache LRU** — the cache currently evicts only on completion.

These are the determinism-hardening items on the [roadmap](/parity#roadmap). The repository's
[`docs/ARCHITECTURE.md`](https://github.com/) contains the same design at file/symbol granularity.
