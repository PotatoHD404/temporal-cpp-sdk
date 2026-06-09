---
title: Introduction
description: A native C++ SDK for Temporal — what it is, what works, and what doesn't.
slug: /
---

# temporal-cpp-sdk

**A native C++ SDK for [Temporal](https://temporal.io).** It talks to the Temporal frontend over
gRPC and runs the workflow replay engine itself — modeled on the official
[Go SDK](https://github.com/temporalio/sdk-go), with no Rust `sdk-core` dependency.

```cpp
#include <temporal/temporal.h>

std::string Greet(temporal::workflow::Context& ctx, std::string name) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);
  return ctx.ExecuteActivity<std::string>(o, "ComposeGreeting", name).Get();
}
```

:::warning Experimental — not affiliated with Temporal
This is a community project and **not** an official Temporal SDK. It implements a substantial,
fully-tested **core** of the Temporal programming model, but it is **not at parity** with the
official SDKs. See [Capabilities & parity](/parity) for an honest, itemized matrix of what works and
what doesn't.
:::

## Why it exists

There is no official C++ Temporal SDK. Temporal's SDKs come in two flavors:

- **Native** (Go, Java) — implement the gRPC client *and* the determinism-critical workflow
  state-machine / history-replay engine in the host language.
- **Core-based** (Python, TypeScript, .NET, Ruby) — delegate that engine to the Rust
  [`sdk-core`](https://github.com/temporalio/sdk-core).

`temporal-cpp-sdk` takes the **native** route, reproducing the Go SDK's ergonomics in idiomatic C++20
over its own engine (a stackful-coroutine dispatcher with a sticky in-memory cache).

## What works today

- **Client** — connect, start workflows, await results (following continue-as-new), signal, query,
  update, cancel, terminate; schedules (interval + cron), batch operations, visibility queries,
  search-attribute management, and async activity completion (`CompleteActivity`/`FailActivity`).
- **Worker** — register plain `R(Context&, Args...)` functions (or typed handles via
  `TEMPORAL_ACTIVITY`), poller threads, a sticky cache, and history replay
  (`ReplayWorkflowHistory`).
- **Activities** — typed execution, server-driven retries with custom `RetryPolicy`, throttled
  heartbeating, local activities, and async completion.
- **Timers** — `NewTimer` / `Sleep` (and `chrono` literals like `10s` / `24h`).
- **Workflow messaging** — signals, queries, updates (with validators), all also addressable through
  typed `SignalRef`/`QueryRef`/`UpdateRef` handles.
- **Determinism helpers** — `SideEffect` / `MutableSideEffect`, `GetVersion` (patching),
  `UpsertSearchAttributes`.
- **Composition** — selectors ("activity OR timeout"), child workflows (with
  `ParentClosePolicy` + signal-to-child), continue-as-new, observe-only cancellation, and Nexus
  operations.
- **Engine** — coroutine dispatcher keeping live workflow state across suspensions; sticky cache so
  continuation tasks apply only incremental history (no full re-replay).
- **Testing** — a time-skipping `TestWorkflowEnvironment` and `Worker::ReplayWorkflowHistory` for
  unit-testing workflows against recorded history.
- **Tested** — 86 unit tests + an end-to-end integration suite against a real Temporal dev server,
  plus CI.

## What's missing

Full parity is still a multi-year effort. Remaining gaps are mostly at the edges: bundled
metrics/tracing exporters (the `MetricsHandler` sink and tracing hooks are there, but no OTel
exporter ships), payload-codec breadth beyond the bundled base64/gzip, worker-side version pinning,
and parts of the operator/cloud client surface that need infra a single dev server can't provide.
The [parity matrix](/parity) lists it all, with caveats.

## Next steps

- [Getting started](/getting-started) — install, build, and run your first workflow.
- [Core concepts](/concepts) — workflows, activities, determinism, and the engine model.
- [Writing workflows](/workflows/overview) — the full authoring API.
- [Capabilities & parity](/parity) — the honest matrix.
