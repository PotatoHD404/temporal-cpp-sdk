---
title: Architecture
description: How the native engine executes workflows — coroutine dispatcher, sticky cache, and the replay engine.
---

# Architecture

`temporal-cpp-sdk` is a **native** C++ Temporal SDK: it speaks gRPC to the Temporal frontend
directly and implements the determinism-critical workflow replay engine itself, mirroring the
structure of the official [Go SDK](https://github.com/temporalio/sdk-go). It does **not** embed the
Rust [`sdk-core`](https://github.com/temporalio/sdk-core) that the Python/TypeScript/.NET/Ruby SDKs
build on.

## Layered structure (mirrors the Go SDK)

```
include/temporal/            Public API (proto-free)
  client/  worker/  workflow/  activity/  converter/
  common/  interceptor/  log/  testing/
src/
  client/  worker/                 Thin public-facing implementations
  converter/  log/                 Data conversion, structured logging
  internal/                        The engine (never installed, never in public headers):
    grpc_client          WorkflowService stub wrapper
    coroutine            Stackful thread-fiber coroutine
    workflow_task_handler  Replay engine + sticky cache + WorkflowRunner (the crux)
    activity_task_handler  Activity execution + heartbeating
    nexus_task_handler   Nexus operation execution
    worker_impl          Poller threads + lifecycle
    determinism          Non-determinism detection (command-vs-history matching)
    lru_cache            Bounded sticky-cache LRU
    proto_util           Payload/Failure/Duration <-> protobuf, ids
```

The public/`internal` split matches Go's `client`/`worker`/`workflow` packages over its large
`internal/` engine. A hard rule: **public headers never include protobuf** — the gRPC/proto surface
stays entirely under `src/internal/`.

## The determinism problem

A Temporal workflow must be **deterministic and replayable**: its history of events (activity
scheduled/completed, timer started/fired, …) is the source of truth, and the SDK must be able to
reconstruct the workflow's exact in-memory state by replaying that history. Every orchestration call
a workflow makes is intercepted, turned into a *command* for the server, and later matched back
against the resulting history events. Getting this right is the hard, native part of any SDK.

## The workflow replay engine

The engine lives in `src/internal/workflow_task_handler.cpp`. A cold start (or a sticky-cache miss)
runs the **replay** path: the server sends the full event history with the workflow task, and we
re-execute the workflow function from the beginning against it.

**1. History prescan** (`ScanHistory`). One pass over the events builds:

- the workflow input (from `WorkflowExecutionStarted`);
- per-activity outcomes (`scheduled`, and `completed`+result / `failed`+failure / `timed_out`),
  correlating completion events to their schedule via the `scheduled_event_id` they carry;
- per-timer, child-workflow, and Nexus-operation outcomes (`started`/`fired`, initiated/completed);
- delivered signals, the cancel-requested flag, and recorded markers (side-effects, versions).

**2. Deterministic operation ids.** `WorkflowRunner` assigns each `ExecuteActivity` call the next
integer sequence number as its `activity_id` (timers, child workflows, and Nexus operations likewise,
each with their own prefix). Because the workflow is deterministic, the *same* call on a later replay
gets the *same* id, so it lines up with the history event from the previous attempt.

**3. Execution.** The user workflow function runs with a `workflow::Context` backed by the runner:

- `ExecuteActivity(seq)`: if `activity_id == seq` is already in history → resolve the returned
  `Future` from the recorded outcome and emit **no** command (replay). Otherwise → emit a
  `ScheduleActivityTask` command and return a *pending* future (first execution).
- `Future::Get()`: if the future is ready, return the decoded value (or throw `ActivityError`). If it
  is pending, **yield** the coroutine to park the workflow (see below).
- The function returning normally yields a `CompleteWorkflowExecution` command; an
  `ApplicationError`/`std::exception` yields `FailWorkflowExecution`; a `ContinueAsNew` request yields
  `ContinueAsNewWorkflowExecution`; parking yields only the scheduling commands collected so far.

**4. Respond.** All emitted commands (schedules/timers first, then any terminal command) go back via
`RespondWorkflowTaskCompleted`.

This is exactly the **activation → commands** model the core-based SDKs use, with the history-derived
state playing the role of the activation and the emitted commands the completion — here computed
natively in C++ instead of by `sdk-core`.

### Worked example (`GreetWorkflow`)

| Task | History seen | Runner does | Commands sent |
|------|--------------|-------------|---------------|
| 1 | `WorkflowExecutionStarted` | `ExecuteActivity("0")` not in history → schedule; `Get()` pending → **park** | `ScheduleActivityTask(id=0)` |
| — | *(activity worker runs `ComposeGreeting`, server records `ActivityTaskCompleted`)* | | |
| 2 | …`ActivityTaskScheduled(0)`, `ActivityTaskCompleted`→"Hello, Temporal!" | `ExecuteActivity("0")` resolved → `Get()` returns; function returns | `CompleteWorkflowExecution(result)` |

## Suspension model: stackful coroutine dispatcher

The workflow body runs on a **stackful coroutine** (`src/internal/coroutine.h`) — a cooperative
thread-fiber driven by a strict turn-based handshake (mutex + condition variable) so the dispatcher
and the workflow thread never run concurrently; the state they share therefore needs no further
locking. This is the C++ analogue of the goroutine-based dispatcher the Go SDK uses.

Awaiting an unresolved future or signal **yields** the coroutine — preserving the workflow's entire
stack and locals — rather than unwinding it. On a resume the workflow runs to its next suspension
point; its live state then stays available for queries to read and for selectors to choose among.
Teardown (cache eviction or completion) throws `CoroutineAbort` into the suspended frame to unwind it
cleanly.

:::note
`CoroutineAbort` is deliberately **not** derived from `std::exception`, so a workflow's
`catch (const std::exception&)` cannot swallow it. User code must never `catch (...)` in a way that
discards it.
:::

## Sticky cache

The handler keeps each running workflow's coroutine alive between tasks, keyed by run id
(`src/internal/workflow_task_handler.cpp`). The worker advertises a unique **sticky task queue** in
every `RespondWorkflowTaskCompleted` and polls it with a second poller, so the server delivers
continuation tasks carrying only the **incremental** history:

- **Continuation** (the task's first event id is `cached.last_event_id + 1`): apply just the new
  events to the live futures/signal state (`ApplyEvents`) and `Resume` the cached coroutine — **no
  full re-replay**. Activity completions correlate to their future via a persistent
  `scheduled_event_id → activity_id` map.
- **Replay** (first task, or the history doesn't continue the cache): rebuild from full history via
  the prescan path, run to the current suspension point, and cache the runner.
- **Sticky-cache miss** (incremental history but nothing cached, e.g. after eviction): respond
  `RESET_STICKY_TASK_QUEUE` so the server resends full history on the normal queue.

Because `Block`/`Park` are written as re-checking loops, the same coroutine code serves both paths.

The cache is a **bounded LRU** (`src/internal/lru_cache.h`): `WorkerOptions::max_cached_workflows`
caps the resident set (`0` = unbounded), and once full the least-recently-used runner is evicted — its
next task takes the `RESET_STICKY_TASK_QUEUE` path and replays from full history. Completed workflows
are evicted immediately. `Worker::cache_hits()` / `Worker::replays()` expose how many tasks took the
continuation vs. replay path.

## Activations → commands

Each task feeds history-derived state to the running workflow, which emits commands
(`ScheduleActivityTask`, `StartTimer`, `StartChildWorkflowExecution`, `ScheduleNexusOperation`,
`UpsertWorkflowSearchAttributes`, `RecordMarker`, `CompleteWorkflowExecution`,
`ContinueAsNewWorkflowExecution`, …). Updates flow as protocol messages (`Acceptance` / `Response`)
rather than commands. This is the same activation→command model the core-based SDKs use, here computed
natively in C++.

### Non-determinism detection

Unlike the original engine, the replay path now **validates** itself. `src/internal/determinism.h`
captures the ordered stream of commands the workflow *produces* during a replay and the ordered stream
of command-generating events history *recorded*, then matches them pairwise
(`MatchReplayCommands` / `CommandMatchesEvent`, mirroring the Go SDK's `matchReplayWithHistory` /
`isCommandMatchEvent`). History is authoritative: activities and child workflows are keyed by id +
type name; timers, markers, and Nexus operations by id; terminal commands match on kind alone. The
first divergence raises a `[TMPRL1100]` non-determinism error and fails the workflow task rather than
corrupting state. The workflow may still emit *additional* trailing commands — that is real forward
progress past the replayed history.

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

Because emitted commands are guarded to fire once, re-running a handler or re-resolving a future never
re-schedules work; it only advances the body and the produced-command order the determinism check
compares against. **Markers / side-effects are *not* deferred** — they are consumed in call order,
independent of event timing. For the common update-free workflow the timeline is empty, so replay
stays the single-resume fast-forward, unchanged.

## Deadlock abort

Workflow code must never block. The dispatcher resumes each coroutine under an optional per-resume
bound (`WorkerOptions::deadlock_detection_timeout`): the resume is `Coroutine::ResumeFor(timeout)`,
and a task that fails to yield within it flags the runner as deadlocked. The handler then **aborts** —
it emits the `temporal_workflow_task_deadlock` metric, logs an error, drops the run from the cache,
and fails the workflow *task* (cause `WorkflowDeadlock`) so the server reschedules it.

The runaway coroutine cannot be safely interrupted, so it is **abandoned**: it keeps running on its
own detached thread, and its runner is moved into a process-lifetime leak list (`g_leaked_runners`)
so that thread never dereferences freed memory. The poller thread returns immediately and keeps
serving other workflows. This is the same trade-off the Go SDK makes (a deadlocked workflow goroutine
is leaked). See [Running in production](/production#deadlock-detection) for the operational view.

## Signals and cancellation

Both arrive as history events (`WorkflowExecutionSignaled`, `WorkflowExecutionCancelRequested`), so
they fit the replay model:

- **Signals** — `ScanHistory` collects each `WorkflowExecutionSignaled` into a per-name ordered list.
  `ctx.GetSignalChannel<T>(name)` returns a `ReceiveChannel` whose `Receive()` consumes the next
  signal via a deterministic per-name cursor (reset every replay) and decodes it to `T`; if none
  remain it parks (yields the coroutine) until the next event. Because replay is deterministic, the
  Nth `Receive()` always returns the Nth signal — buffering and ordering match Temporal semantics.
  `ReceiveAsync()` is the non-blocking variant.
- **Cancellation** — `WorkflowExecutionCancelRequested` sets a flag exposed as `ctx.IsCancelled()`,
  and `ctx.AwaitCancellation()` returns a future you can race in a `Selector` (the canonical
  "work OR cancel" pattern). The workflow chooses how to react (finish, clean up); the cancel request
  schedules a workflow task, so a workflow parked on a signal `Receive()` re-runs and can observe the
  flag. `ctx.CancelExternalWorkflow` / `ctx.SignalExternalWorkflow` reach other executions by id. See
  [Cancellation](/cancellation) for the full model.

## Queries, updates, and selectors

These rely on the coroutine keeping live workflow state across a suspension:

- **Queries** — `ctx.SetQueryHandler(name, fn)` registers a read-only handler (re-registered each
  replay). When a query arrives, the workflow is replayed to its current suspension point and the
  handler is invoked against the live, parked state, answering via `RespondQueryTaskCompleted` (or
  `query_results` for queries attached to a workflow task). The client side is
  `WorkflowHandle::Query<R>(type, args...)`.
- **Updates** — `ctx.SetUpdateHandler(name, fn)` registers a handler that may mutate state; a
  three-argument overload adds a read-only **validator** that runs first and, by throwing, rejects an
  update before acceptance (nothing is written to history and the handler never runs). An update
  arrives as a protocol message in the workflow task; the handler runs against live state and the
  worker replies with `Acceptance` + `Response` protocol messages (matching the Go SDK's bodies, incl.
  `accepted_request_sequencing_event_id`). The client side is `WorkflowHandle::Update<R>(name,
  args...)`. Updates apply on the live (sticky) path, and on a from-scratch replay they are
  re-applied in history order (see [Replay re-application of
  updates](#replay-re-application-of-updates)).
- **Selectors** — `workflow::Selector` waits on multiple futures and proceeds when any is ready (the
  canonical "activity OR timeout" pattern), running the matching case's handler; `Select()` parks via
  the coroutine when nothing is ready. An optional default makes it non-blocking.

## Side effects and versioning

The marker mechanism the core SDKs expose is implemented natively (`workflow::Context`, recorded via
`RecordMarker` commands and matched on replay):

- `ctx.SideEffect<R>(fn)` runs `fn` once, records its result to history, and returns the recorded
  value on every later replay.
- `ctx.MutableSideEffect(id, fn[, equals])` is the keyed variant that only writes a new marker when
  the value changes.
- `ctx.GetVersion(change_id, min, max)` records a version number the first time it runs so a workflow
  can branch on code changes safely across replays (returning `kDefaultVersion` for histories that
  predate the call). See [Versioning](/versioning) for the patching workflow.
- `ctx.ExecuteLocalActivity<R>(opts, type, args...)` runs a registered activity inline in the workflow
  worker, recording its result as a marker (retries happen inline per the retry policy).

## Child workflows

`ctx.ExecuteChildWorkflow<R>(opts, type, args...)` emits a `StartChildWorkflowExecution` command and
returns a `Future` that resolves on `ChildWorkflowExecutionCompleted`/`Failed`, correlated directly by
the (parent-assigned, deterministic) child `workflow_id`, with a configurable `ParentClosePolicy`. It
is wired through replay, sticky continuations, the determinism check, and the runner exactly like
activities — the child runs as an independent workflow.

## Activities

`activity_task_handler.cpp` is straightforward: poll → decode input → look up the registered function
by type → run it in real time → `RespondActivityTaskCompleted`, or `RespondActivityTaskFailed` on an
`ApplicationError`/exception. Long-running activities call `RecordActivityTaskHeartbeat` (which also
surfaces server-side cancellation). Activities have no determinism constraints. See
[Timeouts & retries](/timeouts-and-retries) for the policy knobs.

## Nexus operations

Nexus is implemented as a first-class operation type. On the **caller** side
`ctx.ExecuteNexusOperation<R>(endpoint, service, operation, input, schedule_to_close)` emits a
`ScheduleNexusOperation` command — like an activity, but a Nexus operation's input and result are each
a **single** value (one `Payload`) — and returns a `Future` resolved by the
`NexusOperationCompleted`/`Failed` history event; it flows through replay, sticky continuations, and
the determinism check like every other command. On the **handler** side
`Worker::RegisterNexusOperation(service, operation, fn)` registers an `R Fn(Arg)` handler;
`src/internal/nexus_task_handler.cpp` polls the Nexus task queue, decodes the single input Payload,
runs the handler, and replies with a synchronous `StartOperationResponse`.

:::note
Only **synchronous** start-operation requests are handled today — asynchronous (long-running) Nexus
operations and cancel requests are failed back with `NOT_IMPLEMENTED`. See the
[parity matrix](/parity) for the current Nexus surface.
:::

## Client

- `StartWorkflow` → `StartWorkflowExecution` (args encoded to payloads; a UUID workflow id and request
  id are generated when unset).
- `WorkflowHandle::Result<R>()` long-polls `GetWorkflowExecutionHistory` filtered to the close event,
  then decodes `WorkflowExecutionCompleted.result`, or throws `WorkflowFailedError` for
  failed/timed-out/terminated/canceled/continued executions.

## Data conversion

`Payload` is `{ map<string,bytes> metadata; bytes data }`, mirroring `temporal.api.common.v1.Payload`.
The default `DataConverter` chains converters by the `encoding` metadata key: `binary/null`,
`binary/plain`, and `json/plain` (the catch-all, via nlohmann-json). Values cross the API as any
nlohmann-serializable type. A `PayloadCodec` chain (bundled base64 and gzip codecs; encryption is
bring-your-own, as in the Go SDK) can sit between the converter and the wire. See
[Data conversion](/data-conversion).

## Concurrency model

`WorkerImpl` runs one or more workflow-, activity-, and Nexus-poller threads. Each long poll uses a
short idle deadline so `Stop()` returns promptly while still receiving available tasks immediately.
Within the engine, a workflow's body runs on its own coroutine thread but never concurrently with the
dispatcher (the turn-based handshake serializes them), so the shared per-workflow state needs no
internal locking.

## gRPC / protobuf build

The `temporalio/api` protos are vendored (`third_party/api`) and compiled at configure time
(`cmake/TemporalProto.cmake`). A single `-I third_party/api` import root resolves every import
(`temporal/…`, vendored `google/…`, `nexusannotations/…`); the well-known `google/protobuf/*` types
are taken from the system libprotobuf rather than regenerated, to avoid duplicate symbols. The
time-skipping `testservice` protos are compiled from a sibling import root for the test framework.

## Mapping to the Go SDK

| Go SDK | temporal-cpp-sdk |
|--------|------------------|
| `client.Dial` / `client.Client` | `temporal::client::Client::Connect` |
| `worker.New` / `RegisterWorkflow` / `RegisterActivity` / `RegisterNexusOperation` | `temporal::worker::Worker` |
| `workflow.Context`, `workflow.ExecuteActivity`, `Future.Get` | `temporal::workflow::Context`, `ExecuteActivity`, `Future::Get` |
| `workflow.GetVersion` / `SideEffect` / `MutableSideEffect` | `Context::GetVersion` / `SideEffect` / `MutableSideEffect` |
| `workflow.ExecuteNexusOperation` | `Context::ExecuteNexusOperation` |
| `activity.GetInfo` / `RecordHeartbeat` | `temporal::activity::Context::GetInfo` / heartbeat |
| `converter.DataConverter` | `temporal::DataConverter` |
| `internal/` engine (`internal_event_handlers.go`, `internal_command_state_machine.go`, …) | `src/internal/workflow_task_handler.cpp` + `determinism.h` |

## What's deliberately not here (yet)

The original roadmap items — non-determinism detection, workflow versioning (`GetVersion`/patching),
side effects (`SideEffect`/`MutableSideEffect`), the bounded sticky-cache LRU, Nexus operations, child
workflows, replay re-application of updates, and deadlock *abort* — are **all implemented now** (see
the sections above and the [parity matrix](/parity)).

What remains is depth, not the itemized roadmap. Honest caveats live in the
[parity matrix](/parity#roadmap), for example:

- **TLS / mTLS / API-key auth** is implemented and unit-verified, but not yet run against a real
  TLS-enabled Temporal server.
- **Nexus** handles only synchronous start-operation requests — async operations and cancel are not
  implemented.
- **Sessions** support host pinning + slot caps, but not heartbeat-held long-lived sessions.
- Parts of the **operator / worker-versioning** surface (remote-cluster federation, some routing) are
  implemented but not fully exercised end-to-end.

If a capability shows a caveat or 🟡/❌ in the [parity matrix](/parity), it genuinely isn't fully
there yet. The repository's `docs/ARCHITECTURE.md` (in the source tree) covers the same design at
file/symbol granularity.
