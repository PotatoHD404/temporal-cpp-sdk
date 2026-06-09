---
title: Running in production
description: Worker tuning, graceful lifecycle, sticky-cache observability, non-determinism safety, and an honest list of what is not yet ready for production use.
---

# Running in production

This page covers the operational concerns you need to address before putting a `temporal-cpp-sdk` worker
in a production environment: how to tune `WorkerOptions`, how to observe the sticky cache, how to
protect against non-deterministic code changes, and what to do about the capabilities that are not yet
implemented.

:::warning
Several production-critical features — TLS/mTLS, authentication, metrics, distributed tracing, and
worker versioning — are **not yet implemented**. Review the [Not yet for production](#not-yet-for-production)
section and the full [parity matrix](/parity) before deploying.
:::

---

## Worker options

Pass a `temporal::WorkerOptions` struct as the third argument to the `Worker` constructor:

```cpp
#include <temporal/common/options.h>
#include <temporal/worker/worker.h>

temporal::WorkerOptions opts;
opts.max_concurrent_workflow_tasks = 8;
opts.max_concurrent_activities     = 100;
opts.workflow_task_pollers         = 2;
opts.activity_task_pollers         = 8;
opts.panic_policy                  = temporal::WorkflowPanicPolicy::BlockWorkflow;
opts.max_cached_workflows          = 500;

temporal::worker::Worker worker(client, "my-task-queue", opts);
```

### Field reference

| Field | Default | What it controls |
|---|---|---|
| `max_concurrent_workflow_tasks` | `0` (library default) | Maximum number of workflow tasks that may be executing concurrently on this worker. Each in-flight workflow task occupies a coroutine stack. |
| `max_concurrent_activities` | `0` (library default) | Maximum number of activity executions that may run concurrently on this worker. |
| `workflow_task_pollers` | `1` | Number of goroutine-analog poller threads dedicated to the normal (non-sticky) workflow task queue. |
| `activity_task_pollers` | `1` | Number of poller threads dedicated to the activity task queue. |
| `panic_policy` | `BlockWorkflow` | How to react when replayed commands diverge from recorded history. See [Non-determinism safety](#non-determinism-safety). |
| `max_cached_workflows` | `0` (unbounded) | Maximum number of workflow coroutines held resident in the sticky cache simultaneously. `0` means no limit. When the limit is reached, the least-recently-used workflow is evicted. See [Sticky cache](#sticky-cache). |

### Sizing guidance

**Workflow task pollers** (`workflow_task_pollers`): the server delivers workflow tasks
one-at-a-time per poll, so two to four pollers is usually enough even under high throughput. A single
poller is often sufficient during initial deployment.

**Activity task pollers** (`activity_task_pollers`): I/O-bound activities benefit from more pollers.
A starting point is `max(1, std::thread::hardware_concurrency() / 2)`, capped by whatever makes
sense for your downstream dependencies.

**Concurrent activities** (`max_concurrent_activities`): the hard cap on simultaneous activity
executions. Tune this against your database connection pool size, external rate limits, or memory
budget. `0` means the library applies its own default; set an explicit value once you know your
resource envelope.

**Concurrent workflow tasks** (`max_concurrent_workflow_tasks`): each in-flight workflow task holds a
coroutine stack in memory. For cache-hit continuations the cost is small; for replays (cold starts or
evictions) it includes re-running the full workflow history. Start with a value close to
`max_cached_workflows` and tune up if you see the sticky queue backing up.

:::note
The concurrency and rate-limiting options are present and accepted but **not all are fully enforced**
by the current implementation (see the [parity matrix](/parity)). Treat them as reservations
that the library honours on a best-effort basis until enforcement is complete.
:::

---

## Sticky cache {#sticky-cache}

The worker keeps a running workflow's stackful coroutine resident between tasks, keyed by run id.
When the server delivers a continuation task the worker applies only the incremental history and
resumes the live coroutine — no full replay needed. See [Architecture](/architecture) for the
design detail.

### Bounding the cache

By default `max_cached_workflows = 0` means the cache is unbounded and workflows are evicted only
when they complete. In production you almost certainly want a finite bound:

```cpp
opts.max_cached_workflows = 500;  // keep at most 500 resident coroutines
```

When the cache is full and a new workflow task arrives for an uncached run, the **least-recently-used**
entry is evicted. The evicted workflow's coroutine stack is cleaned up; its next task will trigger a
full replay to rebuild state. Choose a value that fits comfortably within the worker's memory budget.
A resident coroutine typically occupies a few hundred kilobytes of stack plus whatever heap the
workflow body allocates.

:::note
`max_cached_workflows = 0` (unbounded) is safe for development and low-volume deployments where the
set of active runs is small. For anything with sustained concurrency, set a bound — unbounded caches
can exhaust process memory if many workflow runs are simultaneously open.
:::

### Observing cache efficiency

The `Worker` exposes two counters you can sample and expose to your monitoring system:

```cpp
// After Start(), poll these on a periodic thread or before Stop():
long hits    = worker.cache_hits();   // tasks served from the live coroutine (no replay)
long misses  = worker.replays();      // tasks that required a full-history replay

double hit_rate = (double)hits / std::max(1L, hits + misses);
```

A healthy worker in steady state will have a high hit rate (> 95 %). A sustained low hit rate
indicates the cache is too small for the number of concurrently-open runs, the worker is restarting
frequently, or the task queue is being served by a pool of workers with no affinity (each worker
replaying tasks that another cached).

---

## Non-determinism safety {#non-determinism-safety}

A workflow is deterministic: replaying its full history must produce exactly the same sequence of
orchestration commands. If you deploy a code change that reorders, removes, or adds an activity or
timer relative to existing open executions, the replay engine detects the divergence.

`WorkflowPanicPolicy` governs what happens when a mismatch is detected:

```cpp
// Default — recommended for production:
opts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;

// Terminal — use only when a stuck workflow is worse than a lost one:
opts.panic_policy = temporal::WorkflowPanicPolicy::FailWorkflow;
```

- **`BlockWorkflow`** (default): fail the workflow *task*. The server will retry it, so deploying a
  corrected worker build can recover the workflow without data loss. This is the safe choice and
  mirrors the Go/Java SDKs' `BlockWorkflow` behavior.
- **`FailWorkflow`**: fail the workflow *execution* outright. Terminal — use only when a stuck
  workflow is worse than a lost one.

See [Non-determinism detection](/advanced#non-determinism-detection) in the advanced guide for the
full detection model.

### Catching breakage in CI with replay testing

The most reliable way to prevent a non-deterministic deployment is to replay real workflow histories
against your changed code before it ships. The `Worker` class provides
`ReplayWorkflowHistory` for exactly this purpose — it runs entirely offline, with no server required:

```cpp
// 1. Export a representative history from production or a staging run:
//    temporal workflow show -o json > fixtures/order-workflow.json
//    or:
std::string history_json = handle.FetchHistoryJson();  // from a WorkflowHandle

// 2. In a unit test, replay it against the current workflow code:
temporal::worker::Worker replayer(client, "replay-task-queue");
replayer.RegisterWorkflow("OrderWorkflow", OrderWorkflow);

// Throws std::runtime_error if the workflow's replayed commands diverge
// from the recorded history.
replayer.ReplayWorkflowHistory(history_json);
```

Add a handful of representative histories as committed test fixtures and replay them in your CI
pipeline. Any incompatible edit — a reordered activity, a removed timer, a new branch inserted before
an existing one — fails the test instead of silently breaking a running workflow in production.

See [Replay testing](/advanced#replay-testing) and [Testing](/testing) for the full treatment.

---

## Graceful worker lifecycle

`Worker` offers three lifecycle methods:

```cpp
worker.Start();  // spawn poller threads — returns immediately
// ...
worker.Stop();   // signal pollers to stop and join all threads
```

```cpp
worker.Run();    // Start() + block until SIGINT/SIGTERM, then Stop()
```

### When to use each

**`Run()`** is the right entry point for a dedicated worker process. It handles `SIGINT` and
`SIGTERM` for you and blocks until the process is asked to terminate:

```cpp
int main() {
    auto client = temporal::client::Client::Connect(client_opts);

    temporal::worker::Worker worker(client, "orders", worker_opts);
    worker.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
    worker.RegisterActivity("ChargeCard", ChargeCard);
    worker.RegisterActivity("SendEmail", SendEmail);

    worker.Run();   // blocks; clean shutdown on SIGINT / SIGTERM
    return 0;
}
```

**`Start()` + `Stop()`** is for embedding a worker inside a larger process that manages its own
signal handling or shutdown sequence:

```cpp
worker.Start();    // non-blocking; pollers are running

// ... your server/main loop ...

// On shutdown:
worker.Stop();     // drain in-flight tasks and join poller threads
```

:::note
`Stop()` signals the pollers to stop accepting new tasks and joins the threads. In-flight activities
that have already been picked up will run to completion (or until their `start_to_close_timeout`
expires). There is no configurable drain timeout yet — `Stop()` blocks until all pollers have exited.
:::

---

## Structured logging

The SDK never writes to `stdout`/`stderr` directly. All internal output flows through a
`temporal::log::Logger` that you supply on `ClientOptions`. The same logger is used by the worker,
the replay engine, and the gRPC layer, so you get a single structured log stream for the entire SDK.

### Plugging in your logger

```cpp
#include <temporal/log/logger.h>

class MyLogger : public temporal::log::Logger {
 public:
  void Log(temporal::log::Level level,
           std::string_view message,
           const std::vector<temporal::log::Field>& fields) override {
    // Route to slog, spdlog, absl::log, or any structured sink:
    auto* rec = my_log_library::NewRecord(to_severity(level), message);
    for (const auto& f : fields) {
      rec->AddField(f.key, f.value);
    }
    rec->Flush();
  }
};

temporal::ClientOptions opts;
opts.logger = std::make_shared<MyLogger>();

auto client = temporal::client::Client::Connect(opts);
// Worker constructed from this client inherits the same logger.
```

If you do not supply a logger, `temporal::log::DefaultLogger()` is used — it writes one
line-per-record to `stderr`, which is fine for development.

### Log levels and helper constructors

```cpp
namespace temporal::log {

enum class Level : std::uint8_t { Debug, Info, Warn, Error };

struct Field { std::string key; std::string value; };

// Convenience constructor:
Field F(std::string key, std::string value);

// Logger interface:
virtual void Log(Level level, std::string_view message,
                 const std::vector<Field>& fields) = 0;
}
```

The SDK emits `Debug`-level messages for internal lifecycle events (poller starts, task dispatch,
cache hits/misses), `Info` for significant state changes (worker start/stop), and `Warn`/`Error` for
unexpected conditions. Suppress `Debug` in production unless you are diagnosing a specific issue.

---

## Not yet for production {#not-yet-for-production}

:::warning
The following capabilities are **not implemented**. Do not rely on them in production — they will not
degrade gracefully; they simply do not exist yet.

| Feature | Status | Impact |
|---|---|---|
| TLS / mTLS | ❌ | All connections are insecure plaintext gRPC. Do not expose the Temporal frontend on an untrusted network without a sidecar/proxy. |
| API-key authentication | ❌ | Workers and clients cannot authenticate to Temporal Cloud or a hardened self-hosted cluster. |
| Metrics (Prometheus / OTel) | ❌ | No SDK-emitted metrics (task latency, slot utilisation, schedule-to-start, etc.). |
| Distributed tracing / OpenTelemetry | ❌ | No trace propagation through workflow/activity boundaries. |
| Worker versioning / Build IDs | ❌ | You cannot deploy multiple worker versions to the same task queue with routing. |
| Poller autoscaling / graceful drain | ❌ | No built-in long-poll drain on `Stop()`; no autoscaling. |
| Interceptors | ❌ | No middleware hooks on the client or worker. |
| Payload codecs (encryption/compression) | ❌ | Payloads are stored in Temporal history as plain JSON. |

See the full [parity matrix](/parity) for everything that is and is not implemented.
:::

### Workarounds for the most critical gaps

**Insecure transport**: run `temporal-cpp-sdk` workers inside a mTLS-terminating service mesh (Istio,
Linkerd) or behind an Envoy sidecar that terminates TLS before forwarding to the Temporal frontend.

**No metrics**: export `cache_hits()` and `replays()` on a periodic timer to your metrics backend as
a minimal proxy for worker health until SDK-level metrics land.

**No tracing**: instrument activity entry/exit manually using your preferred tracing library; pass
trace context through workflow memo or signal payloads until header/context propagation is
implemented.

---

## Putting it together

A minimal production-ready worker process, incorporating all of the guidance on this page:

```cpp
#include <temporal/client/client.h>
#include <temporal/common/options.h>
#include <temporal/log/logger.h>
#include <temporal/worker/worker.h>

#include "my_logger.h"       // your structured logger adapter
#include "my_workflows.h"    // OrderWorkflow, etc.
#include "my_activities.h"   // ChargeCard, SendEmail, etc.

int main() {
    // --- Client ---
    temporal::ClientOptions client_opts;
    client_opts.target   = "temporal-frontend.internal:7233";  // plaintext only for now
    client_opts.ns       = "production";
    client_opts.identity = "order-worker-v1";
    client_opts.logger   = std::make_shared<MyStructuredLogger>();

    auto client = temporal::client::Client::Connect(client_opts);

    // --- Worker ---
    temporal::WorkerOptions worker_opts;
    worker_opts.max_concurrent_activities     = 100;
    worker_opts.max_concurrent_workflow_tasks = 20;
    worker_opts.activity_task_pollers         = 8;
    worker_opts.workflow_task_pollers         = 2;
    worker_opts.max_cached_workflows          = 500;
    // Keep BlockWorkflow (default) — safe for rolling deployments.
    worker_opts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;

    temporal::worker::Worker worker(client, "orders", worker_opts);
    worker.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
    worker.RegisterActivity("ChargeCard",   ChargeCard);
    worker.RegisterActivity("SendEmail",    SendEmail);

    // Emit cache counters to your metrics sink periodically.
    // (No built-in metrics yet — see "Not yet for production".)
    std::thread metrics_thread([&] {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            my_metrics::Gauge("temporal.cache_hits",  worker.cache_hits());
            my_metrics::Gauge("temporal.replays",     worker.replays());
        }
    });
    metrics_thread.detach();

    // Blocks until SIGINT / SIGTERM, then drains and exits.
    worker.Run();
    return 0;
}
```
