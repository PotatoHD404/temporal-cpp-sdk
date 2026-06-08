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
  (`IsCancelled()`), queries (`SetQueryHandler` + `WorkflowHandle::Query<R>`), selectors
  (`workflow::Selector`, "activity OR timeout"), **updates** (`SetUpdateHandler` +
  `WorkflowHandle::Update<R>`), **child workflows** (`ExecuteChildWorkflow<R>`), and
  **continue-as-new** (`ctx.ContinueAsNew`, client follows the run chain).
- Activities: typed execution, application-error failures, **heartbeating**
  (`activity::Context::RecordHeartbeat`).
- Data conversion: Nil / ByteSlice / JSON (nlohmann).
- Engine: a stackful **coroutine dispatcher** (live workflow state across suspensions) with a
  **sticky cache** — running workflows stay in memory and continuation tasks apply only incremental
  history (no full re-replay), falling back to full-history replay on a cache miss.
- Tested: 18 unit tests + 17 end-to-end integration tests (timer, single + parallel activities,
  activity-failure propagation, RetryPolicy fail-fast, terminate, signal delivery + ordering,
  observed cancellation, live-state query, selector activity-vs-timeout both directions, sticky-cache
  continuations, child workflow, state-accumulating update, activity heartbeating, continue-as-new
  chaining) — run against a dev server via `TEMPORAL_INTEGRATION=1`, and in CI.

> Coverage caveat: integration tests prove the paths listed above. Update **validators** and
> **replay re-application** of updates after a cache eviction are not yet implemented (the sticky
> cache keeps a running workflow resident, so updates apply on the live path); a bounded cache LRU
> and non-determinism detection are also pending, and everything below is **not** implemented.

## Phase 1 — robustness & determinism hardening

- **Bounded sticky-cache LRU** ✅ — `WorkerOptions::max_cached_workflows` caps the resident workflow
  count; the least-recently-used run is evicted (`src/internal/lru_cache.h`), and its next task
  triggers a from-scratch replay. 0 means unbounded.
- **Non-determinism detection** ✅: on a full-history replay the workflow's emitted commands are
  matched in order against the command events history recorded (`src/internal/determinism.h`);
  divergences surface per `WorkflowPanicPolicy` (BlockWorkflow default fails the task so a fixed
  worker recovers; FailWorkflow fails the run).
- History **pagination** (`next_page_token`) for long histories.
- **Heartbeat throttling** + acting on the server's `cancel_requested` heartbeat response (the call
  is wired; throttling/cancel-detection are not).

## Phase 2 — workflow feature surface

- **Update validators** ✅ — `SetUpdateHandler(name, handler, validator)`; the read-only validator
  runs before acceptance, and a throw rejects the update with an ephemeral `Rejection` protocol
  message (no command, so nothing is written to history). **Replay re-application** of accepted
  updates after a cache eviction remains.
- **Cancellation** ✅ (mostly): timer cancellation via `Future::Cancel()`; workflow reacts to its
  own cancel via `ctx.AwaitCancellation()` (a Selector case); **activity cancellation** via
  `Future::Cancel()` → `RequestCancelActivityTask`, observed activity-side through
  `activity::Context::IsCancelled()` (heartbeat). Remaining: child-workflow cancellation +
  parent-close-policy.
- **Selector channel cases** ✅ — `Selector::AddReceive` waits on a signal channel ("signal OR
  timeout"); a non-consuming `HasSignal` peek backs the ready check.
- **SideEffect** ✅ + **`GetVersion`** versioning ✅ — marker record/replay (RecordMarker command;
  SideEffect keyed by call order, Version by change id). MutableSideEffect remaining.
- **Local activities**.

## Phase 3 — production concerns

- **TLS / mTLS** and **API-key** auth in `ClientOptions`.
- **Interceptors** (inbound/outbound, client + worker), header/context propagation.
- **Metrics** handler interface; OpenTelemetry-friendly hooks.
- **Proto / ProtoJSON** payload converters; payload codecs (encryption/compression).
- Worker tuning: concurrency limits, poller autoscaling, graceful drain.

## Phase 4 — breadth

- **Schedules** client API 🟡 — `Client::CreateSchedule`/`DescribeSchedule`/`DeleteSchedule`
  (interval spec + start-workflow action). Remaining: update/list/trigger/pause, calendar/cron specs.
- **Nexus** operations.
- **Worker versioning** / deployments.
- **Replay/test framework** ✅ — `Worker::ReplayWorkflowHistory(json)` replays a recorded history
  offline against the registered workflow code and throws on non-determinism;
  `WorkflowHandle::FetchHistoryJson()` exports a real history (Temporal JSON). A time-skipping test
  environment remains.
- Schema-driven typed workflow/activity stubs; richer error type mapping.

## Build / packaging

- `install()` rules + a CMake package config ✅ — `find_package(temporal-cpp CONFIG)` exports
  `temporal::sdk`/`temporal::proto`; verified locally by a standalone downstream consumer
  (`tests/packaging/`). pkg-config + re-adding the consumer build as a Conan-toolchain CI step remain.
- **Conan** packaging ✅ — `conanfile.py` (CMakeToolchain + CMakeDeps, `build_tests`/`build_examples`
  options); both CI jobs build through it, verified green (gRPC 1.67.1 / protobuf 5.27.0). vcpkg port remaining.
- **CI matrix** ✅ — macOS (arm64) and Linux both build via Conan, for identical reproducible toolchain
  deps everywhere (Debian/Ubuntu ship no CMake config packages for protobuf/gRPC, so Conan supplies
  them). macOS runs unit + integration, Linux runs unit. Windows/MSVC is compiler-flag-clean (warning
  flags gated, tools resolved from imported targets) but not yet CI-covered.
- Optional: build protobuf/gRPC via `FetchContent` for hermetic builds.
