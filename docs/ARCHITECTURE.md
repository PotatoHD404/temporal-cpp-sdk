# Architecture

`temporal-cpp` is a **native** C++ Temporal SDK: it speaks gRPC to the Temporal frontend
directly and implements the determinism-critical workflow replay engine itself, mirroring the
structure of the official [Go SDK](https://github.com/temporalio/sdk-go). It does **not** embed
the Rust [`sdk-core`](https://github.com/temporalio/sdk-core) that the Python/TypeScript/.NET/Ruby
SDKs build on.

## Layered structure (mirrors the Go SDK)

```
include/temporal/            Public API (proto-free)
  client/    worker/    workflow/    activity/    converter/    common/    log/
src/
  client/    worker/                 Thin public-facing implementations
  converter/ log/                    Data conversion, structured logging
  internal/                          The engine (never installed, never in public headers):
    grpc_client          WorkflowService stub wrapper
    workflow_task_handler  Replay engine + WorkflowRunner (the crux)
    activity_task_handler  Activity execution
    worker_impl          Poller threads + lifecycle
    proto_util           Payload/Failure/Duration <-> protobuf, ids
```

The public/`internal` split matches Go's `client`/`worker`/`workflow` packages over its large
`internal/` engine. A hard rule: **public headers never include protobuf** — the gRPC/proto
surface stays entirely under `src/internal/`.

## The determinism problem

A Temporal workflow must be **deterministic and replayable**: its history of events (activity
scheduled/completed, timer started/fired, …) is the source of truth, and the SDK must be able to
reconstruct the workflow's exact in-memory state by replaying that history. Every orchestration
call a workflow makes is intercepted, turned into a *command* for the server, and later matched
back against the resulting history events. Getting this right is the hard, native part of any SDK.

## The workflow replay engine

The engine lives in `src/internal/workflow_task_handler.cpp`. It runs **non-sticky**: the server
sends the full event history with each workflow task, and we re-execute the workflow function from
the beginning against it.

**1. History prescan** (`ScanHistory`). One pass over the events builds:
- the workflow input (from `WorkflowExecutionStarted`);
- per-activity outcomes (`scheduled`, and `completed`+result / `failed`+failure / `timed_out`),
  correlating completion events to their schedule via the `scheduled_event_id` they carry;
- per-timer outcomes (`started`, `fired`).

**2. Deterministic operation ids.** `WorkflowRunner` assigns each `ExecuteActivity` call the next
integer sequence number as its `activity_id` (timers likewise, prefixed `t`). Because the workflow
is deterministic, the *same* call on a later replay gets the *same* id, so it lines up with the
history event from the previous attempt.

**3. Execution.** The user workflow function runs with a `workflow::Context` backed by the runner:
- `ExecuteActivity(seq)`: if `activity_id == seq` is already in history → resolve the returned
  `Future` from the recorded outcome and emit **no** command (replay). Otherwise → emit a
  `ScheduleActivityTask` command and return a *pending* future (first execution).
- `Future::Get()`: if the future is ready, return the decoded value (or throw `ActivityError`).
  If it is pending, throw `internal::WorkflowBlocked` to **park** the workflow.
- The function returning normally yields a `CompleteWorkflowExecution` command; an
  `ApplicationError`/`std::exception` yields `FailWorkflowExecution`; `WorkflowBlocked` yields only
  the scheduling commands collected so far.

**4. Respond.** All emitted commands (schedules/timers first, then any terminal command) go back via
`RespondWorkflowTaskCompleted`.

This is exactly the **activation → commands** model the core-based SDKs use, with the
history-derived state playing the role of the activation and the emitted commands the completion —
here computed natively in C++ instead of by `sdk-core`.

### Worked example (`GreetWorkflow`)

| Task | History seen | Runner does | Commands sent |
|------|--------------|-------------|---------------|
| 1 | `WorkflowExecutionStarted` | `ExecuteActivity("0")` not in history → schedule; `Get()` pending → **park** | `ScheduleActivityTask(id=0)` |
| — | *(activity worker runs `ComposeGreeting`, server records `ActivityTaskCompleted`)* | | |
| 2 | …`ActivityTaskScheduled(0)`, `ActivityTaskCompleted`→"Hello, Temporal!" | `ExecuteActivity("0")` resolved → `Get()` returns; function returns | `CompleteWorkflowExecution(result)` |

## Suspension model: block-by-exception

The Go SDK parks blocked workflows as suspended goroutines kept in an in-memory cache. This slice
instead **throws `WorkflowBlocked`** at the first unresolved `Get()` and re-executes the workflow
from scratch on the next task. Chosen because it is the simplest *correct* model for sequential and
parallel-await workflows: no fibers/stackful coroutines, no suspended-frame teardown hazards, and it
maps cleanly onto non-sticky replay.

Consequences and current limits:
- **User workflow code must not `catch (...)`** in a way that swallows `WorkflowBlocked` (it is
  deliberately *not* derived from `std::exception`, so `catch (const std::exception&)` is safe).
- **No mid-execution concurrency** yet — selectors, signals/queries/updates delivered partway
  through a run, and cancellation are not modeled (see [ROADMAP](ROADMAP.md)). All `ExecuteActivity`
  calls issued before the first blocking `Get()` *are* scheduled together, so fan-out/await of
  several activities works.
- **Re-execution is O(history) per task.** Fine for short workflows; a sticky in-process cache +
  stackful-coroutine dispatcher (closer to Go's `coroutineState`) is the roadmap upgrade for
  efficiency and full concurrency.
- **No non-determinism detection** yet (comparing emitted commands against history). User code is
  trusted to be deterministic, as in every Temporal SDK.

## Activities

`activity_task_handler.cpp` is straightforward and fully correct: poll → decode input → look up the
registered function by type → run it in real time → `RespondActivityTaskCompleted`, or
`RespondActivityTaskFailed` on an `ApplicationError`/exception. Activities have no determinism
constraints.

## Client

- `StartWorkflow` → `StartWorkflowExecution` (args encoded to payloads; a UUID workflow id and
  request id are generated when unset).
- `WorkflowHandle::Result<R>()` long-polls `GetWorkflowExecutionHistory` filtered to the close
  event, then decodes `WorkflowExecutionCompleted.result`, or throws `WorkflowFailedError` for
  failed/timed-out/terminated/canceled/continued executions.

## Data conversion

`Payload` is `{ map<string,bytes> metadata; bytes data }`, mirroring `temporal.api.common.v1.Payload`.
The default `DataConverter` chains converters by the `encoding` metadata key: `binary/null`,
`binary/plain`, and `json/plain` (the catch-all, via nlohmann-json). Values cross the API as any
nlohmann-serializable type.

## Concurrency

`WorkerImpl` runs one or more workflow- and activity-poller threads. Each long poll uses a short
idle deadline so `Stop()` returns promptly while still receiving available tasks immediately. In the
block-by-exception model each workflow task is handled synchronously on its poller thread (no
separate fiber).

## gRPC / protobuf build

The `temporalio/api` protos are vendored (`third_party/api`) and compiled at configure time
(`cmake/TemporalProto.cmake`). A single `-I third_party/api` import root resolves every import
(`temporal/…`, vendored `google/…`, `nexusannotations/…`); the well-known `google/protobuf/*` types
are taken from the system libprotobuf rather than regenerated, to avoid duplicate symbols.

## Mapping to the Go SDK

| Go SDK | temporal-cpp |
|--------|--------------|
| `client.Dial` / `client.Client` | `temporal::client::Client::Connect` |
| `worker.New` / `RegisterWorkflow` / `RegisterActivity` | `temporal::worker::Worker` |
| `workflow.Context`, `workflow.ExecuteActivity`, `Future.Get` | `temporal::workflow::Context`, `ExecuteActivity`, `Future::Get` |
| `activity.GetInfo` | `temporal::activity::Context::GetInfo` |
| `converter.DataConverter` | `temporal::DataConverter` |
| `internal/` engine (`internal_event_handlers.go`, `internal_command_state_machine.go`, …) | `src/internal/workflow_task_handler.cpp` |
