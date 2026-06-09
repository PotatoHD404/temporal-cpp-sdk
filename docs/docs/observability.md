---
title: Observability
description: Emit SDK metrics through a MetricsHandler, route structured logs through a pluggable Logger, and propagate distributed traces with the interceptor framework — adapted to the temporal-cpp-sdk API.
---

# Observability

Observability for a `temporal-cpp-sdk` worker has three pillars, each pluggable so you can route it
into whatever backend you already run:

- **[Metrics](#metrics)** — a `MetricsHandler` sink you set on `WorkerOptions`; the SDK emits a set of
  poller, task-latency, slot, and sticky-cache series through it.
- **[Logging](#logging)** — a `temporal::log::Logger` you set on `ClientOptions`; the SDK never writes
  to `stdout`/`stderr` directly. Inside a workflow or activity, reach the same logger through the
  context.
- **[Tracing](#tracing)** — an interceptor-based framework with a bring-your-own `Tracer`; a span is
  started around each workflow/activity and its context propagates across the boundary through a
  Temporal header.

:::note
There is no bundled Prometheus scrape endpoint, no OpenTelemetry exporter, and no metrics push
pipeline — each pillar is an interface you implement against your own stack. What the SDK guarantees is
that it routes its signals through these interfaces; the export is yours. See the
[parity matrix](/parity) for the wider picture.
:::

---

## Metrics

A worker reports its internal telemetry through the `temporal::MetricsHandler` interface
(`<temporal/common/options.h>`). Implement it to forward to Prometheus, StatsD, OpenTelemetry, or any
backend, then set it on `WorkerOptions::metrics_handler`. If you leave the handler `nullptr` (the
default), the worker emits nothing — there is no overhead.

### The interface

```cpp
namespace temporal {

class MetricsHandler {
 public:
  using Tags = std::map<std::string, std::string>;

  virtual void Counter(const std::string& name, std::int64_t value, const Tags& tags) = 0;
  virtual void Gauge(const std::string& name, double value, const Tags& tags) = 0;
  virtual void Timer(const std::string& name, std::chrono::nanoseconds value, const Tags& tags) = 0;
};

}  // namespace temporal
```

Three primitives: `Counter` for monotonic event counts, `Gauge` for point-in-time levels, and `Timer`
for durations (delivered as `std::chrono::nanoseconds` — convert to whatever unit your backend wants).
Every call carries a `Tags` map; map each tag to your backend's label/dimension concept.

:::note
The handler is called from the worker's poller threads, concurrently. Your implementation must be
**thread-safe**. The counters in the example below use `std::atomic`; a real adapter typically forwards
straight to a thread-safe client.
:::

### Setting it on the worker

```cpp
#include <temporal/common/options.h>
#include <temporal/worker/worker.h>

auto metrics = std::make_shared<MyMetricsHandler>();

temporal::WorkerOptions opts;
opts.metrics_handler = metrics;   // nullptr (default) disables emission

temporal::worker::Worker worker(client, "my-task-queue", opts);
```

### Metrics the SDK emits

The worker emits the following series. Names are stable identifiers; tags are listed where they are
attached (series with no tags listed are emitted with an empty tag map).

#### Poller lifecycle

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `temporal_poller_start` | Counter | `task_queue`, `poller_type` | A poller thread started. `poller_type` is `workflow`, `sticky`, `activity`, `session`, or `nexus`. |
| `temporal_pollers_in_flight` | Gauge | `task_queue` | Currently-active pollers for that loop kind (the live count under [poller autoscaling](/production#worker-options)). |
| `temporal_workflow_poll_success` / `temporal_activity_poll_success` / `temporal_nexus_poll_success` | Counter | — | A long-poll returned a task. |
| `temporal_workflow_poll_timeout` / `temporal_activity_poll_timeout` / `temporal_nexus_poll_timeout` | Counter | — | A long-poll returned empty (no task before the server's poll timeout). |

#### Task latency timers

The worker times task processing in phases — schedule-to-start, execution, and (for workflow and
activity tasks) end-to-end:

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `temporal_workflow_task_schedule_to_start_latency` | Timer | `task_queue` | Time a workflow task waited locally (concurrency gate / rate limiter) between **receipt** and **execution start**. |
| `temporal_workflow_task_execution_latency` | Timer | — | Wall-clock time the handler spent executing the workflow task. |
| `temporal_workflow_task_end_to_end_latency` | Timer | `task_queue` | Receipt-to-completion span for the whole task (schedule-to-start **plus** execution). |
| `temporal_activity_task_schedule_to_start_latency` | Timer | `task_queue` | As above, for an activity task. |
| `temporal_activity_task_execution_latency` | Timer | — | Time the activity function ran. |
| `temporal_activity_task_end_to_end_latency` | Timer | `task_queue` | Receipt-to-completion for the activity task. |
| `temporal_nexus_task_schedule_to_start_latency` / `temporal_nexus_task_execution_latency` | Timer | `task_queue` / — | The same two phases for a Nexus task. |

:::note
These schedule-to-start timers measure the **local** wait on this worker (gate + rate limiter), not
the server-side schedule-to-start the Go/Java SDKs report. They are a proxy for "is this worker
backing up?", not for end-to-end queue latency on the server.
:::

#### Task outcome counters

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `temporal_workflow_task_handled` / `temporal_activity_task_handled` / `temporal_nexus_task_handled` | Counter | — | A task completed (its handler returned without throwing). |
| `temporal_workflow_task_failed` / `temporal_activity_task_failed` / `temporal_nexus_task_failed` | Counter | `task_queue` | The poll/dispatch loop caught an exception while processing a task. |

#### Slots and in-flight gauges

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `temporal_workflow_tasks_in_flight` / `temporal_activity_tasks_in_flight` | Gauge | — | Tasks currently executing (held by the concurrency gate). |
| `temporal_worker_task_slots_available` | Gauge | `task_queue`, `worker_type` | Free execution slots = configured cap − in-flight. **Only emitted when the corresponding cap is set** (`max_concurrent_workflow_task_executions` / `max_concurrent_activity_executions`); with an unbounded cap there is no slot count to report. `worker_type` is `workflow`, `activity`, or `session`. |

#### Sticky-cache efficiency

Emitted after each handled workflow task (see [Sticky cache](/production#sticky-cache) for the design):

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `temporal_sticky_cache_hit` | Counter | `task_queue` | Workflow task served from the resident coroutine — no replay. Emitted as a delta. |
| `temporal_sticky_cache_miss` | Counter | `task_queue` | Workflow task that required a full-history replay (cold start or post-eviction). Emitted as a delta. |
| `temporal_sticky_cache_total_hits` / `temporal_sticky_cache_total_misses` | Gauge | `task_queue` | Cumulative hit/miss totals since the worker started (the same numbers `worker.cache_hits()` / `worker.replays()` return). |
| `temporal_sticky_cache_size` | Gauge | `task_queue` | The configured cache capacity — **only emitted when `max_cached_workflows > 0`** (no accessor exposes the live resident count, so the bound is reported). |

#### Deadlock detection

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `temporal_workflow_task_deadlock` | Counter | — | A workflow task overran `deadlock_detection_timeout` and was aborted. **A firing counter is a code defect**, not a tuning signal — see [Deadlock detection](/production#deadlock-detection). |

### A custom MetricsHandler

A minimal handler that forwards counters, gauges, and timers to your own metrics client. The full
`Tags` map is passed through so your backend can attach labels:

```cpp
#include <temporal/common/options.h>

#include "my_metrics_client.h"  // your StatsD / Prometheus / OTel client

class MyMetricsHandler : public temporal::MetricsHandler {
 public:
  explicit MyMetricsHandler(std::shared_ptr<my_metrics::Client> client)
      : client_(std::move(client)) {}

  void Counter(const std::string& name, std::int64_t value, const Tags& tags) override {
    client_->IncrementBy(name, value, tags);
  }

  void Gauge(const std::string& name, double value, const Tags& tags) override {
    client_->SetGauge(name, value, tags);
  }

  void Timer(const std::string& name, std::chrono::nanoseconds value, const Tags& tags) override {
    // Convert to your backend's preferred unit; here, milliseconds.
    const double ms =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(value).count();
    client_->Timing(name, ms, tags);
  }

 private:
  std::shared_ptr<my_metrics::Client> client_;  // assumed thread-safe
};
```

If you only want a couple of series, filter by `name` inside each method — the worker calls the handler
for every metric, and ignoring the ones you don't care about is free.

### Polling cache counters directly

The two most important worker-health numbers are also available as plain accessors, independent of the
metrics handler, so you can sample them on your own schedule:

```cpp
long hits   = worker.cache_hits();  // == temporal_sticky_cache_total_hits
long miss   = worker.replays();     // == temporal_sticky_cache_total_misses
double rate = (double)hits / std::max(1L, hits + miss);
```

See [Observing cache efficiency](/production#sticky-cache) for how to interpret the hit rate.

---

## Logging

The SDK has a strict rule: **nothing is written to `stdout`/`stderr` directly** — no `printf`, no
`std::cout`, no bare logging anywhere. Every internal message flows through a
`temporal::log::Logger` (`<temporal/log/logger.h>`) that you supply, so the worker, the replay engine,
and the gRPC layer all share one structured stream you control.

### The interface

```cpp
namespace temporal::log {

enum class Level : std::uint8_t { Debug, Info, Warn, Error };

// A structured field — key/value, both strings.
struct Field {
  std::string key;
  std::string value;
};

// Convenience constructor used at every call site: log::F("key", "value").
inline Field F(std::string key, std::string value);

class Logger {
 public:
  // The single method you implement.
  virtual void Log(Level level, std::string_view message, const std::vector<Field>& fields) = 0;

  // Convenience wrappers (non-virtual) that call Log() at a fixed level.
  void Debug(std::string_view m, const std::vector<Field>& f = {});
  void Info(std::string_view m, const std::vector<Field>& f = {});
  void Warn(std::string_view m, const std::vector<Field>& f = {});
  void Error(std::string_view m, const std::vector<Field>& f = {});
};

// Process-wide default: one structured line per record to stderr.
std::shared_ptr<Logger> DefaultLogger();

}  // namespace temporal::log
```

### Plugging in your logger

Set it once on `ClientOptions`; the worker built from that client inherits the same logger:

```cpp
#include <temporal/log/logger.h>

class MyLogger : public temporal::log::Logger {
 public:
  void Log(temporal::log::Level level, std::string_view message,
           const std::vector<temporal::log::Field>& fields) override {
    // Route to slog / spdlog / absl::log / any structured sink.
    auto rec = my_log_library::NewRecord(to_severity(level), message);
    for (const auto& f : fields) {
      rec.AddField(f.key, f.value);
    }
    rec.Flush();
  }
};

temporal::ClientOptions opts;
opts.logger = std::make_shared<MyLogger>();
auto client = temporal::client::Client::Connect(opts);
```

If you set no logger, `temporal::log::DefaultLogger()` is used — one structured line per record to
`stderr`, which is fine for development.

The SDK uses `Debug` for internal lifecycle events (poller starts, task dispatch, cache hits/misses),
`Info` for significant state changes, and `Warn`/`Error` for unexpected conditions. Suppress `Debug` in
production unless you are diagnosing something specific.

### Logging from a workflow

Inside a workflow, get the shared logger from the context — do not capture a global:

```cpp
std::string GreetWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);
  std::string greeting = ctx.ExecuteActivity<std::string>(opts, "ComposeGreeting", name).Get();

  ctx.GetLogger().Info("composed greeting", {temporal::log::F("greeting", greeting)});
  return greeting;
}
```

`ctx.GetLogger()` returns a reference to the same `log::Logger` the worker uses, so workflow logs land
in your one stream.

:::warning Replay and duplicate logs
The logger returned by `ctx.GetLogger()` is **not automatically silenced during replay**. A workflow
task re-runs your workflow body from the start on a cold start or after a cache eviction, so any
unguarded log line is emitted again on every replay. Guard logs you only want once with
`ctx.IsReplaying()`:

```cpp
if (!ctx.IsReplaying()) {
  ctx.GetLogger().Info("charged customer", {temporal::log::F("order_id", id)});
}
```

This is the same `IsReplaying()` flag interceptors use to avoid double-counting (see the tracing test
idiom below). For workflow code, always reach for `ctx.GetLogger()` rather than your application logger,
and gate user-facing log lines on `!ctx.IsReplaying()`.
:::

### Logging from an activity

An activity runs in real time with no replay, so duplicate-log concerns do not apply. The activity
`Context` does **not** expose a `GetLogger()` accessor — use your application's own logger directly, and
attach correlation fields from `ctx.GetInfo()`:

```cpp
std::string ChargeCard(temporal::activity::Context& ctx, ChargeRequest req) {
  const auto& info = ctx.GetInfo();
  my_app::log().Info("charging card",
                     {{"workflow_id", info.workflow_id},
                      {"activity_id", info.activity_id},
                      {"attempt", std::to_string(info.attempt)}});
  // ... real work, real I/O ...
  return "charged";
}
```

`ActivityInfo` carries `workflow_id`, `run_id`, `activity_id`, `activity_type`, `task_queue`,
`attempt`, and the propagated `headers` — everything you need to correlate an activity log line back to
its workflow.

---

## Tracing

Distributed tracing is built on the **interceptor framework** (`<temporal/interceptor/interceptor.h>`)
plus a tracing interceptor and a **bring-your-own** `Tracer` (`<temporal/interceptor/tracing.h>`).

:::warning No exporter is bundled
The SDK ships the tracing *plumbing* — span lifecycle around workflow/activity execution and
context propagation across the boundary — but **no OpenTelemetry, OpenTracing, or Jaeger exporter**.
You implement the abstract `Tracer`/`Span` interfaces by bridging to your tracing backend. The only
`Tracer` implementations the SDK provides are `NoopTracer` (does nothing) and `InMemoryTracer`
(test-only, records spans in-process). See the [parity matrix](/parity).
:::

### How a trace propagates workflow → activity

The interceptor framework is wired into the worker and client call paths. The bundled
`TracingInterceptor`:

1. **Inbound** — when a workflow or activity execution starts, reads the inbound Temporal header,
   asks the `Tracer` to `Extract` a parent span context from it, and starts a span as that parent's
   child (a root span if there is none).
2. **Outbound** — when the workflow schedules an activity / child workflow / signal (or the client
   starts/signals a workflow), it asks the `Tracer` to `Inject` the current span's context into a flat
   `map<string,string>`, serializes that to a single `Payload`, and writes it onto the Temporal
   **header** under one key (default `"_tracer-data"`).

Because the context rides on the same header maps the SDK already propagates
(`StartWorkflowOptions::headers` → `WorkflowInfo::headers` → `ActivityInfo::headers`), the activity's
inbound side extracts the parent and the two spans share one trace. The wire format is a single
JSON-encoded header value, matching other Temporal SDKs in spirit so cross-language traces can link.

### Enabling tracing on a worker

Construct a `TracingInterceptor` over your `Tracer` and add it to `WorkerOptions::interceptors`:

```cpp
#include <temporal/interceptor/tracing.h>

auto tracer  = std::make_shared<MyTracer>();                  // your backend adapter
auto tracing = std::make_shared<temporal::interceptor::TracingInterceptor>(tracer.get());

temporal::WorkerOptions wo;
wo.interceptors.push_back(tracing);

temporal::worker::Worker worker(client, "my-task-queue", wo);
```

To also start a span at the *client* call site (so the workflow's span has a parent), add the same
interceptor to `ClientOptions::interceptors`:

```cpp
temporal::ClientOptions opts;
opts.interceptors.push_back(tracing);
auto client = temporal::client::Client::Connect(opts);
```

:::note
The `Tracer` is held non-owning by the `TracingInterceptor` (it takes a raw `Tracer*`). Keep the
`Tracer` alive for at least as long as the worker/client that uses it — the `std::shared_ptr` in the
snippets above does exactly that.
:::

### The Tracer / Span interfaces

A real backend adapter implements `Tracer`; `StartSpan` returns a `Span`, and `Inject`/`Extract` carry
the context across the wire as a flat string map:

```cpp
namespace temporal::interceptor {

class Span {
 public:
  virtual void SetTag(const std::string& key, const std::string& value) = 0;
  virtual void End(bool error = false) = 0;          // error => the traced op failed
  virtual const SpanContext& context() const = 0;     // parent for a child / source for Inject
};

class Tracer {
 public:
  virtual std::unique_ptr<Span> StartSpan(const StartSpanOptions& options) = 0;  // never null
  virtual std::map<std::string, std::string> Inject(const Span& span) const = 0;  // empty => no header
  virtual std::optional<SpanContext> Extract(
      const std::map<std::string, std::string>& data) const = 0;  // nullopt => no parent
};

}  // namespace temporal::interceptor
```

`StartSpanOptions` carries the high-level `operation` (e.g. `"RunWorkflow"`, `"RunActivity"`), the
specific workflow/activity `name`, an optional `parent` `SpanContext*` (extracted from the inbound
header), and a `tags` map. A `NoopTracer` is provided so call sites need no null checks when tracing is
"enabled" but unconfigured.

### Verifying propagation with the in-memory tracer

For tests and local runs, `InMemoryTracer` records every span in-process and round-trips the
trace/span ids through the propagation map, so you can assert that the activity span inherited the
workflow span's trace without standing up a backend:

```cpp
auto tracer  = std::make_shared<temporal::interceptor::InMemoryTracer>();
auto tracing = std::make_shared<temporal::interceptor::TracingInterceptor>(tracer.get());

temporal::WorkerOptions wo;
wo.interceptors.push_back(tracing);
temporal::worker::Worker worker(client, task_queue, wo);
worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
worker.RegisterActivity("Echo", EchoActivity);
worker.Start();

// ... run a workflow ...

worker.Stop();  // join worker threads before reading recorded spans

const auto& recs = tracer->records();   // every span, in start order
// recs contains at least a workflow span and an activity span sharing one trace_id.
```

Each `InMemoryTracer::Record` exposes `operation`, `name`, `trace_id`, `span_id`, `parent_span_id`
(empty for a root), `tags`, and `ended`/`error` flags — enough to assert the trace topology in a unit
test.

### Interceptors beyond tracing

`TracingInterceptor` is one implementation of the general interceptor surface. The same
`WorkerOptions::interceptors` / `ClientOptions::interceptors` lists accept any
`temporal::interceptor::Interceptor`, letting you wrap workflow-inbound, activity-inbound,
workflow-outbound, and client-outbound calls for your own cross-cutting concerns (auth context,
custom metrics, structured audit). Subclass the relevant `…InterceptorBase` and override only the
method you need; a workflow-inbound interceptor, for instance, can gate side effects on
`ctx.IsReplaying()` to run them exactly once:

```cpp
temporal::Payloads ExecuteWorkflow(temporal::workflow::Context& ctx,
                                   temporal::interceptor::ExecuteWorkflowInput& in,
                                   const temporal::interceptor::Header& header) override {
  if (!ctx.IsReplaying()) {
    ++live_executions_;  // counted once per real run, never on replay
  }
  return next_->ExecuteWorkflow(ctx, in, header);
}
```

---

## See also

- **[Running in production](/production)** — sticky-cache tuning, the `temporal_workflow_task_deadlock`
  counter and deadlock detection, worker sizing, and the honest list of what is not yet
  production-ready.
- **[Testing](/testing)** — replaying recorded histories, the test workflow environment, and unit-test
  idioms (including the in-memory tracer above).
- **[Capabilities & parity](/parity)** — exactly which observability features are implemented versus
  stubbed.
