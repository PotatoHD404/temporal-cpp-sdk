---
title: Introduction
description: A native C++ SDK for Temporal — what it is, what works, and what doesn't.
slug: /
---

# temporal-cpp

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

`temporal-cpp` takes the **native** route, reproducing the Go SDK's ergonomics in idiomatic C++20
over its own engine (a stackful-coroutine dispatcher with a sticky in-memory cache).

## What works today

- **Client** — connect, start workflows, await results (following continue-as-new), signal, query,
  update, cancel, terminate.
- **Worker** — register plain `R(Context&, Args...)` functions, poller threads, a sticky cache.
- **Activities** — typed execution, server-driven retries with custom `RetryPolicy`, heartbeating.
- **Timers** — `NewTimer` / `Sleep`.
- **Workflow messaging** — signals, queries, updates.
- **Composition** — selectors ("activity OR timeout"), child workflows, continue-as-new,
  observe-only cancellation.
- **Engine** — coroutine dispatcher keeping live workflow state across suspensions; sticky cache so
  continuation tasks apply only incremental history (no full re-replay).
- **Tested** — 18 unit tests + 17 end-to-end integration tests against a real Temporal dev server,
  plus CI.

## What's missing

A lot — full parity is a multi-year effort. Notably absent: workflow versioning/patching, side
effects, the test/replay framework, schedules, Nexus, worker versioning, interceptors, TLS/mTLS &
API-key auth, metrics/tracing, search attributes & memo, local activities, payload codecs, and much
of the operator/cloud client surface. The [parity matrix](/parity) lists it all.

## Next steps

- [Getting started](/getting-started) — install, build, and run your first workflow.
- [Core concepts](/concepts) — workflows, activities, determinism, and the engine model.
- [Writing workflows](/workflows/overview) — the full authoring API.
- [Capabilities & parity](/parity) — the honest matrix.
