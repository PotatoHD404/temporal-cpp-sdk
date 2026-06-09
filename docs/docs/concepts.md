---
title: Core concepts
description: Workflows, activities, tasks, determinism, and how the engine executes them.
---

# Core concepts

If you're new to Temporal, here are the ideas you need. If you've used another Temporal SDK, this
maps one-to-one — only the C++ surface differs. This page is the conceptual *what & why* reference:
each concept gets a short explanation, the C++ symbol it lowers to, and a link to the how-to guide
that shows it in full. Code here is kept to the minimum needed to name the type.

:::note
Every concept below mirrors an official Temporal concept. The SDK's job is to make the
*determinism-critical* part work natively in C++; the vocabulary (workflow, activity, task queue,
signal…) is identical to the Go/Java/TypeScript SDKs.
:::

## The big picture

A **Temporal Client** talks to the Temporal **server** (the "cluster") to start and interact with
workflows. The server never runs your code — it durably stores each workflow's **Event History** and
hands out **tasks**. Your **Worker** process polls a **Task Queue**, runs the workflow and activity
code, and reports back. The server records the outcome as new events. That loop — *server keeps
history, worker makes progress, server records the result* — is the whole model.

```
 Client  ──start/signal/query──▶  Server (history + task queues)  ◀──poll/respond──  Worker
                                          │                                            │
                                          └────────────── runs your ───────────────────┘
                                                       workflows & activities
```

The rest of this page expands each box.

## Workflows

A **workflow** is durable, fault-tolerant orchestration code. You write it as an ordinary function:

```cpp
R MyWorkflow(temporal::workflow::Context& ctx, Args... args);
```

The catch: workflow code must be **deterministic**. Temporal persists the *history* of everything
that happened (activity scheduled, timer fired, signal received…) and re-runs your function against
that history to reconstruct state after a crash or when resuming on another worker. So a workflow
must not call `rand()`, read the clock directly, do I/O, or otherwise depend on anything not in its
history. All orchestration goes through the `workflow::Context`, which records it.

You register a workflow with `Worker::RegisterWorkflow(name, fn)`; a client starts it with
`Client::StartWorkflow(options, name, args...)`. Each running instance is a **Workflow Execution**,
identified by a **Workflow Id** (yours, in `StartWorkflowOptions::id`) plus a server-assigned **Run
Id**. Both are available inside the workflow via `ctx.GetInfo()` (a `WorkflowInfo`).

→ See [Workflows](/workflows/overview).

## Activities

An **activity** is a plain unit of work — a function that may do anything (network calls, disk,
CPU). Activities are **not** replayed; they run once (with retries) in real time:

```cpp
R MyActivity(temporal::activity::Context& ctx, Args... args);
```

A workflow schedules an activity with `ctx.ExecuteActivity<R>(opts, name, args...)` and gets a
`Future<R>` for its result. The server guarantees **at-least-once** execution: it handles retries,
timeouts, and redelivery, so an activity may run more than once and should be idempotent where it
matters. An activity normally completes when the function returns, but it can also defer completion
(`ctx.defer_completion()`) and be finished out-of-band later via `Client::CompleteActivity` /
`FailActivity` — useful when the result depends on an external callback.

Long-running activities report progress and liveness with `activity::Context::RecordHeartbeat(...)`,
which also surfaces server-requested cancellation (`ctx.IsCancelled()`).

→ See [Workflows](/workflows/overview) and [Timeouts & retries](/timeouts-and-retries).

## Workers, tasks & task queues

A **worker** is your process; it polls **task queues** for work. There are two kinds of task:

- **Workflow tasks** advance a workflow: the worker runs/​resumes the workflow code and replies with
  *commands* (schedule an activity, start a timer, complete the workflow…).
- **Activity tasks** execute an activity function and reply with the result.

A **task queue** is just a named queue the server uses to route work to workers. There is no setup —
naming a queue in `Client::StartWorkflow` (`StartWorkflowOptions::task_queue`) and in the `Worker`
constructor is all it takes; any worker polling that name is eligible to pick up the task. By default
a workflow's activities and child workflows inherit its task queue; override per call with
`ActivityOptions::task_queue` / `ChildWorkflowOptions::task_queue` to route work to a different pool
of workers.

You register your workflow and activity functions with a `Worker` and call `Start()` (non-blocking)
or `Run()` (blocks until SIGINT / `Stop()`). The string name passed to `RegisterWorkflow` /
`RegisterActivity` is the same name used to start the workflow or schedule the activity. To make
those names impossible to misspell, bind one to its function with a typed handle —
`TEMPORAL_ACTIVITY(fn)` plus `worker.Register(fn_activity)` and
`ctx.ExecuteActivity(opts, fn_activity, args...)` — and the name, argument types, and result type are
all checked at compile time.

→ See [Client & worker](/client-and-worker).

## The Temporal Client

A `temporal::client::Client` is a connection to the Temporal frontend service (cheap to copy — it
shares one gRPC channel). It's the entry point for everything *outside* a workflow:

- start a workflow (`StartWorkflow`) or signal-with-start (`SignalWithStartWorkflow`);
- get a handle to an existing one (`GetHandle`) and then `Signal`, `Query`, `Update`, `Cancel`,
  `Terminate`, or block on its `Result<R>()`;
- run visibility queries, manage schedules, search attributes, Nexus endpoints, and batch operations
  (covered below).

`Client::Connect(ClientOptions{...})` opens the connection. A `WorkflowHandle` is the per-execution
view; the `Client` is the per-cluster view.

→ See [Client & worker](/client-and-worker).

## Event History & Commands

The server keeps an append-only **Event History** per workflow execution — the durable source of
truth. The worker never mutates history directly. Instead, when it runs a workflow task it emits
**Commands** (the intent: *schedule this activity*, *start this timer*, *complete the workflow*), and
the server validates them and records the corresponding **Events** (*ActivityTaskScheduled*,
*TimerStarted*, *WorkflowExecutionCompleted*, …). Later events (*ActivityTaskCompleted*,
*TimerFired*, *WorkflowExecutionSignaled*) record what happened next and drive the workflow's next
task.

In this SDK you never write commands by hand — calling `ctx.ExecuteActivity`, `ctx.NewTimer`,
`ctx.ExecuteChildWorkflow`, or returning from the workflow *is* what emits them. You can fetch a
workflow's raw history as Temporal JSON with `WorkflowHandle::FetchHistoryJson()` (handy for replay
testing).

→ See [Architecture](/architecture).

## Determinism & replay

Because history is the source of truth, the engine reconstructs a workflow's in-memory state by
**replaying** its code against recorded history: it re-runs your function, and each orchestration
call returns the value already recorded in history instead of doing the work again. For this to
produce the same commands every time, **workflow code must be deterministic** — same history in, same
commands out. Anything non-deterministic (wall-clock time, RNG, I/O, map iteration order, reading
external state) must be routed through the `Context` so it's recorded:

| Instead of…              | Use…                                          |
| ------------------------ | --------------------------------------------- |
| `std::this_thread::sleep`| `ctx.Sleep(d)` / `ctx.NewTimer(d)`            |
| `rand()`, a fresh UUID   | `ctx.SideEffect<R>(fn)`                        |
| reading the clock        | `ctx.SideEffect<R>(fn)` (capture the time once)|
| direct network/disk I/O  | an **activity**                               |

To make replay cheap, the engine keeps a running workflow's coroutine resident in a **sticky cache**
between tasks; only on a cache miss (first task, or after eviction) does it replay from full history.
See *How the engine runs your workflow* below.

If a worker ever produces commands that **diverge** from recorded history (e.g. you changed workflow
code incompatibly), that's a **non-determinism error** — see [Determinism safety](#determinism-safety-panic-policy--deadlock-detection)
and [Versioning](#versioning) for how to handle it safely.

→ See [Architecture](/architecture) and [Versioning](/versioning).

## Timers

A **timer** is the durable, replay-safe way for a workflow to wait. `ctx.NewTimer(duration)` returns
a `Future<void>` that resolves when it fires; `ctx.Sleep(duration)` is the shorthand
(`NewTimer(...).Get()`):

```cpp
ctx.Sleep(std::chrono::hours(24));            // durable — survives worker restarts
auto t = ctx.NewTimer(std::chrono::minutes(5));  // race it against other work in a Selector
```

A timer is recorded in history, so a workflow can "sleep for a month" across crashes, deploys, and
worker moves. Cancel a pending timer with `Future::Cancel()`. Never use a real OS sleep in workflow
code — it isn't durable and breaks determinism.

## Signals, queries & updates

A running workflow can exchange messages with the outside world without ending:

- A **signal** delivers data *into* a workflow asynchronously (`ctx.GetSignalChannel<T>(name)` on the
  workflow side, returning a `ReceiveChannel<T>`; `handle.Signal(name, value)` from a client). It's
  recorded in history and durable. `SignalWithStartWorkflow` delivers a signal and starts the
  workflow if it isn't running yet, atomically.
- A **query** reads workflow state synchronously and must **not** mutate it or schedule work
  (`ctx.SetQueryHandler(name, fn)`; `handle.Query<R>(name, args...)`). Queries run against live
  cached state and never appear in history.
- An **update** is a synchronous call that *can* mutate state and **is** recorded
  (`ctx.SetUpdateHandler(name, fn)`; `handle.Update<R>(name, args...)`). It may have a read-only
  **validator** (the three-argument `SetUpdateHandler` overload) that rejects the call before it's
  accepted — nothing is written to history and the handler never runs.

As with activities, each of these can be referenced by a typed handle — `SignalRef<T>`,
`QueryRef<R>`, `UpdateRef<R>` — so the payload and result types are checked instead of restated as a
string plus an explicit template argument.

→ See [Messaging](/workflows/messaging).

## Child workflows

A workflow can start another workflow as a **child**, giving it its own history, retries, and
identity while keeping a parent/child link:

```cpp
auto fut = ctx.ExecuteChildWorkflow<R>(temporal::ChildWorkflowOptions{...}, "ChildType", args...);
R result = fut.Get();
```

Reach for a child workflow (rather than an activity) when a sub-task is itself long-running,
orchestrates its own activities, or deserves a separate history for size or separation-of-concerns
reasons. `ChildWorkflowOptions::parent_close_policy` (`Terminate` / `Abandon` / `RequestCancel`)
decides what happens to a still-running child when the parent closes.

→ See [Composition](/workflows/composition).

## Continue-As-New

History can't grow forever — a workflow that loops indefinitely (a long-lived "entity" or a polling
loop) would accumulate unbounded events. **Continue-As-New** atomically ends the current run and
starts a fresh one with the same Workflow Id and a brand-new, empty history, carrying forward only
the state you pass as the new input:

```cpp
ctx.ContinueAsNew("MyWorkflow", carriedOverState);  // [[noreturn]] — never returns
```

It's the standard pattern for periodic or never-ending workflows. Drain pending signals first; the
call is terminal, so anything after it is unreachable.

## Retry policies & timeouts

Activities (and workflows, where supported) retry automatically on failure under a `RetryPolicy`
(`initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`,
`non_retryable_error_types`). Throwing `ApplicationError` with `non_retryable = true` (or a matching
type) stops retries immediately. Timeouts bound each phase of an activity —
`schedule_to_close_timeout`, `schedule_to_start_timeout`, `start_to_close_timeout`,
`heartbeat_timeout` on `ActivityOptions` — and the workflow itself
(`execution_timeout` / `run_timeout` / `task_timeout` on `StartWorkflowOptions`).

→ See [Timeouts & retries](/timeouts-and-retries).

## Schedules

A **Schedule** runs a workflow on a recurring spec without an always-on driver. Create one with
`Client::CreateSchedule(schedule_id, ScheduleOptions{...})`, where `ScheduleOptions` takes an
`interval` and/or `cron_expressions` plus the `workflow_type` / `task_queue` to start. Schedules can
be described, updated, triggered on demand, paused/unpaused, listed, and deleted from the `Client`.

→ See [Schedules](/schedules).

## Visibility & search attributes

**Visibility** is the ability to *list and count* workflow executions by their properties.
`Client::ListWorkflows(query)` and `Client::CountWorkflows(query)` take a Temporal list filter (e.g.
`"WorkflowType = 'Foo' AND ExecutionStatus = 'Running'"`) and page through matches. The filter can
reference **search attributes** — indexed key/value fields attached to an execution.

Set search attributes at start time (`StartWorkflowOptions::search_attributes`) or from inside a
running workflow (`ctx.UpsertSearchAttributes(...)`). Build typed values with the `temporal::sa::`
helpers (`sa::Keyword`, `sa::Text`, `sa::Int`, `sa::Double`, `sa::Bool`, `sa::KeywordList`). Custom
attributes must be registered on the namespace first — manage them with `Client::AddSearchAttributes`
/ `ListSearchAttributes` / `RemoveSearchAttributes`. Non-indexed metadata that you only want back on
`Describe` (not searchable) goes in `memo` instead.

→ See [Client & worker](/client-and-worker).

## Namespaces

A **namespace** is a unit of isolation on the cluster: workflows, task queues, and search attributes
in one namespace are invisible to another. A client connects to exactly one namespace, set via
`ClientOptions::ns` (default `"default"`); every call it makes — start, signal, list, schedule —
operates within that namespace. Use separate namespaces to isolate teams, environments
(dev/staging/prod), or tenants. Namespace administration (e.g. `Client::DeleteNamespace`) is also on
the `Client`.

## Nexus

**Nexus** is Temporal's way to call across team or namespace boundaries with a clean, decoupled API.
A worker registers an operation handler (`worker.RegisterNexusOperation(service, operation, fn)`), an
endpoint names the target task queue (`client.CreateNexusEndpoint(name, task_queue)`), and a workflow
invokes it with `ctx.ExecuteNexusOperation<R, Arg>(endpoint, service, operation, input).Get()`.
Unlike an activity, a Nexus operation takes a **single** input value and returns a **single** value
(one `Payload` each). Endpoints are created, listed, and described from the `Client`
(`CreateNexusEndpoint` / `GetNexusEndpoint` / `ListNexusEndpoints`).

→ See [Advanced](/advanced).

## Versioning

When you change already-deployed workflow code, the change must not break in-flight executions that
replay against histories recorded by the old code. `ctx.GetVersion(change_id, min, max)` records a
version marker the first time it runs and returns it on every replay, so the workflow can branch on
old-vs-new code safely (returning `kDefaultVersion`, `-1`, for histories recorded before the call
existed). Worker Build-Id and Worker Deployment versioning APIs on the `Client` route new executions
to new worker builds at the fleet level.

→ See [Versioning](/versioning).

## Data conversion

Every value crossing the boundary — workflow args/results, activity args/results, signal/query/update
payloads, memos, search attributes — is serialized to a `Payload` (opaque bytes plus metadata; the
`encoding` key, e.g. `json/plain`, selects the decoder) by a **DataConverter**. The default converter
encodes as JSON; you can supply your own (`ClientOptions::data_converter`) to change serialization or
to add a **codec** layer for compression or end-to-end encryption of payload bytes. The typed APIs
(`ExecuteActivity<R>`, `Query<R>`, `Signal(name, value)`, …) call the converter for you, so app code
deals in C++ types, not payloads.

→ See [Data conversion](/data-conversion).

## Futures and blocking

Orchestration calls return a `Future<R>`:

```cpp
auto future = ctx.ExecuteActivity<std::string>(opts, "Act", arg);
std::string result = future.Get();   // blocks the workflow until the activity completes
```

`Future::Get()` reads like a normal blocking call, but under the hood it **parks** the workflow (the
worker responds to the server and waits for the next task) and resumes when the result is in history.

If you prefer C++20 coroutines, a workflow can return `workflow::workflow_task<R>` and `co_await` the
future instead — `co_await ctx.ExecuteActivity<R>(...)` is exactly equivalent to `.Get()` (same
commands, same replay). To wait on several cases at once — say a signal *or* a timeout — use a
`workflow::Selector`, which proceeds when the first of its future/channel cases becomes ready
(`AddFuture` / `AddReceive` / `AddDefault` / `Select`).

## Testing & replay

Two facilities let you test workflows without a production server:

- **Replay** — feed a recorded history (`Worker::ReplayWorkflowHistory`) back through your workflow
  code with no server at all. If the code's commands diverge from the recording, replay throws —
  catching a non-deterministic change before it reaches production.
- **Time-skipping** — `testing::TestWorkflowEnvironment` connects to the `temporal-test-server`,
  which fast-forwards through pending timers, so a workflow that sleeps for "days" finishes in
  milliseconds.

→ See [Testing](/testing).

## How the engine runs your workflow

This SDK implements the determinism-critical part natively. In short:

1. The workflow body runs on a **stackful coroutine** (a cooperative thread-fiber). Awaiting an
   unresolved future *yields* the coroutine, preserving its entire stack and local variables.
2. A **sticky cache** keeps a running workflow's coroutine resident between tasks. Continuation
   tasks carry only the *new* history events, which the engine applies to the live futures before
   resuming — no full re-replay. `WorkerOptions::max_cached_workflows` bounds the cache (LRU
   eviction); `Worker::cache_hits()` / `replays()` expose the hit/miss counts.
3. On a cache miss (first task, or after eviction), the engine replays from full history to rebuild
   state, then caches the workflow.

Because the coroutine keeps state alive across suspensions, **queries** and **selectors** can read
live workflow state — the same reason the official SDKs keep workflows resident.

## Determinism safety, panic policy & deadlock detection

The determinism-safety features are implemented too:

- **Non-determinism detection** — replayed commands that diverge from recorded history fail the
  workflow task per the worker's `WorkflowPanicPolicy` (`BlockWorkflow`, the safe default, fails just
  the *task* so a corrected build can recover; `FailWorkflow` fails the *execution*).
- **Versioning** (`ctx.GetVersion`) for safely rolling out workflow code changes (see
  [Versioning](#versioning)).
- **Side effects** (`ctx.SideEffect` / `ctx.MutableSideEffect`) for recording a one-off
  non-deterministic value into history.
- **Deadlock detection** — `WorkerOptions::deadlock_detection_timeout` bounds how long a single
  workflow-task resume may run; a workflow that blocks on a real OS call or loops forever is reported
  and its task aborted, while the worker keeps serving other workflows.

Accepted **updates** are also re-applied at their recorded interleaving on a full replay.

→ See [Production](/production).

---

See [Architecture](/architecture) for the full design and [Capabilities & parity](/parity) for the
current status of each feature versus the official SDKs.
