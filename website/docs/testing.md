---
title: Testing
description: How the SDK is tested, and how to run the suites.
---

# Testing

The SDK ships two test suites: fast **unit tests** (no server) and **integration tests** that run
real workflows against a Temporal dev server.

## Unit tests

18 GoogleTest cases cover the data converter, the coroutine primitive (including
teardown-unwinds-a-suspended-stack), and the workflow runtime seam (futures, signal channels,
selectors, cancellation) against a fake environment. They need no server:

```bash
ctest --test-dir build -LE integration
```

## Integration tests

17 end-to-end cases run against a real dev server and are gated behind `TEMPORAL_INTEGRATION=1`, so
the default run stays server-free:

```bash
temporal server start-dev &
TEMPORAL_INTEGRATION=1 ctest --test-dir build -L integration
```

They exercise the real engine end-to-end:

- timer, single + parallel activities, activity-failure propagation, `RetryPolicy` fail-fast
- terminate, signal delivery + ordering, observed cancellation
- live-state query, state-accumulating update
- selector (activity wins / timeout wins), child workflow
- sticky-cache continuations, activity heartbeating, continue-as-new chaining
- offline history replay (`ReplayWorkflowHistory`), update state re-applied on a from-scratch replay
- deadlock abort (an overrunning task is failed + retried while the worker keeps serving other work)
- time-skipping fast-forward (gated on `TEMPORAL_TEST_SERVER`; needs the `temporal-test-server` binary)

## Time-skipping test environment

`temporal::testing::TestWorkflowEnvironment` connects to Temporal's **time-skipping test server**
and fast-forwards through pending timers, so timer-heavy logic is tested without wall-clock waits — a
workflow that sleeps for "days" finishes in milliseconds and the server clock jumps ahead the same
"days".

:::warning
This needs the **`temporal-test-server` binary**, a separate process — *not* the dev server, *not*
the production frontend, and *not bundled* with this SDK. Start it yourself first. The time-skipping
RPCs (`GetCurrentTime` / `Sleep` / `LockTimeSkipping` / `UnlockTimeSkipping`) are served only by that
binary; calling them against any other server throws `RpcError` (`UNIMPLEMENTED`).
:::

```bash
# Start the time-skipping test server on a port of your choosing:
temporal-test-server 7244
```

The test server starts with time skipping **locked** (counter = 1). Constructing the environment
connects to it and **unlocks** time skipping (counter → 0), so idle pending timers auto-skip:

```cpp
#include <temporal/testing/test_workflow_environment.h>
#include <temporal/worker/worker.h>

// Connects to the already-running test server and unlocks time skipping.
temporal::testing::TestWorkflowEnvironment env("localhost:7244");

// Build a Worker (and start workflows) on the environment's client:
temporal::worker::Worker worker(env.client(), "tq");
worker.RegisterWorkflow("Sleeper", Sleeper);   // body does ctx.Sleep(24h)
worker.Start();

const auto before = env.CurrentTime();
auto h = env.client().StartWorkflow(opts, "Sleeper");
h.Result<std::string>();                        // returns in ms — the 24h timer is skipped
const auto skipped = env.CurrentTime() - before;  // ~24h on the (skipped) server clock
worker.Stop();
```

`env.client()` is the client bound to the test server — start workflows on it or build a `Worker`
with `temporal::worker::Worker(env.client(), task_queue)`. The environment also exposes:

| Method | What it does |
|---|---|
| `env.client()` | The `client::Client` bound to the test server. |
| `env.CurrentTime()` | The current (possibly skipped-ahead) server clock as a `system_clock::time_point`. |
| `env.Sleep(d)` | Block until the server clock advances by `d` (fast-forwarded while time skipping is unlocked). |
| `env.LockTimeSkipping()` / `env.UnlockTimeSkipping()` | Pause / resume automatic skipping. The lock is **counted** — balance every lock with an unlock; time skips only while the counter is `0`. |

The integration suite's `TimeSkippingFastForwardsTimers` case exercises this end-to-end. It is gated
on `TEMPORAL_TEST_SERVER=host:port` and skipped when that variable is unset, so the normal dev-server
run is unaffected:

```bash
temporal-test-server 7244 &
TEMPORAL_TEST_SERVER=localhost:7244 TEMPORAL_INTEGRATION=1 \
  ctest --test-dir build -L integration -R TimeSkipping
```

## Replaying recorded histories

`Worker::ReplayWorkflowHistory(history_json)` replays a recorded workflow history against the
worker's registered workflow code and detects non-determinism — entirely **offline, with no server
contact (no RPCs)**. It throws (`std::runtime_error`) if the workflow's replayed commands diverge
from the recorded history, which is exactly how you catch a non-deterministic code change in a unit
test before it breaks a running workflow in production:

```cpp
// 1. Export a representative history (committed as a test fixture):
//    temporal workflow show -o json > fixtures/order-workflow.json
//    or, from a live WorkflowHandle:
std::string history_json = handle.FetchHistoryJson();

// 2. Replay it against the current workflow code:
temporal::worker::Worker replayer(client, "replay-task-queue");
replayer.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
replayer.ReplayWorkflowHistory(history_json);  // throws on divergence
```

See [Running in production](/production#catching-breakage-in-ci-with-replay-testing) for using this
as a CI gate.

### Updates are re-applied on a from-scratch replay

When a workflow that accepted **updates** is replayed from scratch — a cold start, a sticky-cache
eviction, or `ReplayWorkflowHistory` — the accepted update handlers **re-run at their recorded
interleaving**. Events that follow the first update (a future resolution, a delivered signal) are
deferred and replayed in history order with the updates spliced back in at their original positions,
so any state an update mutated (a flag the body `Await`s on, a counter a later signal completes) is
faithfully reconstructed. Markers / side-effects are *not* deferred — they are consumed in call
order. The `UpdateStateReappliedOnFullReplay` integration case proves a workflow whose updates summed
to `12` rebuilds that `12` after a cold-worker replay (it would be `0` if the updates were dropped).
See [Architecture](/architecture#replay-re-application-of-updates) for the mechanism.

## CI

`.github/workflows/ci.yml` runs both suites on macOS: it installs dependencies, builds, runs the
unit tests, then stands up a dev server and runs the integration suite.

## A note on flakiness

Some Temporal interactions are *eventually* consistent (e.g. a query issued immediately after a
signal). Tests that read-after-write poll until the expected state is visible, rather than asserting
once — the right pattern for eventually-consistent reads.

## What's not here

The **time-skipping environment** (above) connects to the external `temporal-test-server` binary
rather than running an in-process mock server — there is no bundled, fully in-memory test server. The
deterministic **replayer** (`Worker::ReplayWorkflowHistory`, above) is implemented and offline. See
the [parity matrix](/parity) for the remaining testing-surface gaps.
