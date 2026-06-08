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
  typed args/results, failure propagation, server-driven activity retries.
- Activities: typed execution, application-error failures.
- Data conversion: Nil / ByteSlice / JSON (nlohmann).
- Engine: non-sticky history replay, deterministic command/event correlation, block-by-exception
  suspension.

## Phase 1 — robustness & determinism hardening

- **Stickiness + in-process workflow cache** keyed by run id; respond on a sticky task queue and
  process incremental history.
- **Stackful-coroutine dispatcher** (mirroring Go's `coroutineState`) to replace block-by-exception:
  enables true mid-execution concurrency and removes the `catch(...)` caveat.
- **Non-determinism detection**: compare emitted commands against replayed history; surface
  mismatches per `WorkflowPanicPolicy`.
- History **pagination** (`next_page_token`) for long histories.
- Heartbeating (`activity::Context::RecordHeartbeat`) wired to `RecordActivityTaskHeartbeat`.

## Phase 2 — workflow feature surface

- **Signals** (`GetSignalChannel`), **Queries** (`SetQueryHandler`), **Updates** (`SetUpdateHandler`).
- **Child workflows** (`ExecuteChildWorkflow`).
- **Selectors** (`workflow.Selector` equivalent) and **cancellation scopes**.
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
