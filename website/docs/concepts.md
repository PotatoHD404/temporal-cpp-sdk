---
title: Core concepts
description: Workflows, activities, tasks, determinism, and how the engine executes them.
---

# Core concepts

If you're new to Temporal, here are the ideas you need. If you've used another Temporal SDK, this
maps one-to-one — only the C++ surface differs.

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

## Activities

An **activity** is a plain unit of work — a function that may do anything (network calls, disk,
CPU). Activities are **not** replayed; they run once (with retries) in real time:

```cpp
R MyActivity(temporal::activity::Context& ctx, Args... args);
```

A workflow schedules an activity and gets a `Future` for its result. The server handles retries,
timeouts, and at-least-once delivery. An activity normally completes when the function returns, but
it can also defer completion (`ctx.defer_completion()`) and be finished out-of-band later via
`Client::CompleteActivity` / `FailActivity` — useful when the result depends on an external callback.

## Tasks & the worker

A **worker** polls **task queues** for work. There are two kinds of task:

- **Workflow tasks** advance a workflow: the worker runs/​resumes the workflow code and replies with
  *commands* (schedule an activity, start a timer, complete the workflow…).
- **Activity tasks** execute an activity function and reply with the result.

You register your workflow and activity functions with a `Worker` and call `Start()`. The string
name passed to `RegisterWorkflow` / `RegisterActivity` is the same name used to start the workflow or
schedule the activity. To make those names impossible to misspell, bind one to its function with a
typed handle — `TEMPORAL_ACTIVITY(fn)` plus `worker.Register(fn_activity)` and
`ctx.ExecuteActivity(opts, fn_activity, args...)` — and the name, argument types, and result type are
all checked at compile time.

## Signals, queries & updates

A running workflow can exchange messages with the outside world without ending:

- A **signal** delivers data *into* a workflow asynchronously (`ctx.GetSignalChannel<T>(name)` on the
  workflow side; `handle.Signal(name, value)` from a client). It's recorded in history and durable.
- A **query** reads workflow state synchronously and must not mutate it or schedule work.
- An **update** is a synchronous call that *can* mutate state and is recorded; it may have a
  read-only validator that rejects the call before it's accepted.

As with activities, each of these can be referenced by a typed handle — `SignalRef<T>`,
`QueryRef<R>`, `UpdateRef<R>` — so the payload and result types are checked instead of restated as a
string plus an explicit template argument.

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
`workflow::Selector`, which proceeds when the first of its future/channel cases becomes ready.

## Nexus

**Nexus** is Temporal's way to call across team or namespace boundaries. A worker registers an
operation handler (`worker.RegisterNexusOperation(service, operation, fn)`), an endpoint names the
target task queue (`client.CreateNexusEndpoint(name, task_queue)`), and a workflow invokes it with
`ctx.ExecuteNexusOperation<R, Arg>(endpoint, service, operation, input).Get()`. Unlike an activity, a
Nexus operation takes a single input value and returns a single value.

## Testing & replay

Two facilities let you test workflows without a production server:

- **Replay** — feed a recorded history (`Worker::ReplayWorkflowHistory`) back through your workflow
  code with no server at all. If the code's commands diverge from the recording, replay throws —
  catching a non-deterministic change before it reaches production.
- **Time-skipping** — `testing::TestWorkflowEnvironment` connects to the `temporal-test-server`,
  which fast-forwards through pending timers, so a workflow that sleeps for "days" finishes in
  milliseconds.

## How the engine runs your workflow

This SDK implements the determinism-critical part natively. In short:

1. The workflow body runs on a **stackful coroutine** (a cooperative thread-fiber). Awaiting an
   unresolved future *yields* the coroutine, preserving its entire stack and local variables.
2. A **sticky cache** keeps a running workflow's coroutine resident between tasks. Continuation
   tasks carry only the *new* history events, which the engine applies to the live futures before
   resuming — no full re-replay.
3. On a cache miss (first task, or after eviction), the engine replays from full history to rebuild
   state, then caches the workflow.

Because the coroutine keeps state alive across suspensions, **queries** and **selectors** can read
live workflow state — the same reason the official SDKs keep workflows resident.

The determinism-safety features are implemented too: **non-determinism detection** (replayed commands
that diverge from recorded history fail the workflow task per the worker's `WorkflowPanicPolicy`),
**versioning** (`ctx.GetVersion`) for safely rolling out workflow code changes, and **side effects**
(`ctx.SideEffect` / `ctx.MutableSideEffect`) for recording a one-off non-deterministic value into
history. Accepted **updates** are also re-applied at their recorded interleaving on a full replay.

See [Architecture](/architecture) for the full design and [Capabilities & parity](/parity) for the
current status of each feature versus the official SDKs.
