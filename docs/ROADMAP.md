# Roadmap

This SDK currently implements a working vertical slice. Below is what works today and the phased
path toward [Go SDK](https://github.com/temporalio/sdk-go) parity. Items are roughly ordered by
priority/dependency.

## Working today

- Client: connect (insecure), `StartWorkflow`, `WorkflowHandle::Result<R>()`, `Signal`, `Cancel`,
  `Terminate`.
- Worker: register workflows/activities (plain `R(Context&, Args...)` functions), poller threads,
  `Start`/`Run`/`Stop`.
- Workflows: sequential and parallel-await `ExecuteActivity<R>(...)`, timers (`NewTimer`/`Sleep`),
  typed args/results, failure propagation, activity retries (custom `RetryPolicy` honored).
- Workflows: signals (`GetSignalChannel<T>().Receive()` / `ReceiveAsync`), cancellation
  (`IsCancelled()`), queries (`SetQueryHandler` + `WorkflowHandle::Query<R>`), and selectors
  (`workflow::Selector`, the "activity OR timeout" pattern).
- Activities: typed execution, application-error failures.
- Data conversion: Nil / ByteSlice / JSON (nlohmann).
- Engine: non-sticky history replay, deterministic command/event correlation, and a stackful
  **coroutine dispatcher** that keeps live workflow state across suspensions.
- Tested: 18 unit tests + 12 end-to-end integration tests (timer, single + parallel activities,
  activity-failure propagation, RetryPolicy fail-fast, terminate, signal delivery + ordering,
  observed cancellation, live-state query, selector activity-vs-timeout both directions) — run
  against a dev server via `TEMPORAL_INTEGRATION=1`, and in CI.

> Coverage caveat: integration tests prove the paths listed above. Replay is exercised for short
> workflows only (non-sticky; the coroutine is torn down per task), and everything below is **not**
> implemented.

## Phase 1 — robustness & determinism hardening

- **Stickiness + in-process workflow cache** keyed by run id: keep the coroutine alive across tasks
  on a sticky task queue + incremental history. The dispatcher already exists; this is the
  efficiency upgrade (avoids re-running from full history and re-creating a thread per task).
- **Non-determinism detection**: compare emitted commands against replayed history; surface
  mismatches per `WorkflowPanicPolicy`.
- History **pagination** (`next_page_token`) for long histories.
- Heartbeating (`activity::Context::RecordHeartbeat`) wired to `RecordActivityTaskHeartbeat`.

## Phase 2 — workflow feature surface

- **Updates** (`SetUpdateHandler`).
- **Child workflows** (`ExecuteChildWorkflow`).
- Richer **cancellation scopes** (current cancellation is observe-only via `IsCancelled()`) and
  selector **channel cases** (the current `Selector` supports future cases).
- **SideEffect / MutableSideEffect**, **`GetVersion`** versioning.
- **ContinueAsNew**.
- **Local activities**.

## Phase 3 — production concerns

- **TLS / mTLS** and **API-key** auth in `ClientOptions`.
- **Interceptors** (inbound/outbound, client + worker), header/context propagation.
- **Metrics** handler interface; OpenTelemetry-friendly hooks.
- **Proto / ProtoJSON** payload converters; payload codecs (encryption/compression).
- Worker tuning: concurrency limits, poller autoscaling, graceful drain.

## Phase 4 — breadth

- **Schedules** client API.
- **Nexus** operations.
- **Worker versioning** / deployments.
- **Replay/test framework** (deterministic replay of recorded histories in unit tests).
- Schema-driven typed workflow/activity stubs; richer error type mapping.

## Build / packaging

- `install()` rules + a CMake package config (`find_package(temporal-cpp)`), pkg-config.
- vcpkg/Conan packaging; Linux CI matrix (Clang/GCC) in addition to macOS.
- Optional: build protobuf/gRPC via `FetchContent` for hermetic builds.
