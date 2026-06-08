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
timeouts, and at-least-once delivery.

## Tasks & the worker

A **worker** polls **task queues** for work. There are two kinds of task:

- **Workflow tasks** advance a workflow: the worker runs/​resumes the workflow code and replies with
  *commands* (schedule an activity, start a timer, complete the workflow…).
- **Activity tasks** execute an activity function and reply with the result.

You register your workflow and activity functions with a `Worker` and call `Start()`.

## Futures and blocking

Orchestration calls return a `Future<R>`:

```cpp
auto future = ctx.ExecuteActivity<std::string>(opts, "Act", arg);
std::string result = future.Get();   // blocks the workflow until the activity completes
```

`Future::Get()` reads like a normal blocking call, but under the hood it **parks** the workflow (the
worker responds to the server and waits for the next task) and resumes when the result is in history.

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

See [Architecture](/architecture) for the full design, and [Capabilities & parity](/parity) for the
determinism features that are *not* yet implemented (non-determinism detection, versioning, side
effects).
