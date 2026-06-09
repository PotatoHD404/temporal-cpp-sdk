---
title: Activities & timers
description: Calling activities, awaiting futures, timers, retries, and determinism rules.
---

# Activities & timers

A workflow function receives a `temporal::workflow::Context&` as its first parameter and any number
of (data-convertible) arguments after it:

```cpp
std::string OrderWorkflow(temporal::workflow::Context& ctx, std::string order_id) {
  // ... orchestrate ...
}
```

Register it on a worker by name:

```cpp
worker.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
```

## Executing activities

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);

temporal::workflow::Future<std::string> fut =
    ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, amount);

std::string receipt = fut.Get();   // blocks (parks) until the activity completes
```

- `ExecuteActivity<R>(options, activity_type, args...)` schedules the activity and returns a
  `Future<R>`. Arguments and the result are serialized via the [data converter](/data-conversion).
- The activity runs on whichever worker is polling its task queue (the workflow's task queue by
  default).
- If the activity fails (and is non-retryable, or exhausts retries), `Future::Get()` throws
  `temporal::ActivityError`.

### Typed activity handles

Instead of a string name plus an explicit `<R>`, you can declare a typed handle with
`TEMPORAL_ACTIVITY` and let the type name *and* the result type come from the C++ function — a
misspelled name or a wrong argument type becomes a compile error:

```cpp
int Increment(temporal::activity::Context& ctx, int n) { return n + 1; }
TEMPORAL_ACTIVITY(Increment);   // declares Increment_activity, type name "Increment"

int Add2(temporal::workflow::Context& ctx, int base) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);
  int a = ctx.ExecuteActivity(o, Increment_activity, base).Get();   // R deduced, no <int>
  return ctx.ExecuteActivity(o, Increment_activity, a).Get();
}
```

Register the same handle on the worker with `worker.Register(Increment_activity)`. Typed handles
lower to the identical string name, so they interoperate freely with the string-based API.

### Running activities in parallel

Schedule several activities *before* awaiting any of them — they run concurrently:

```cpp
auto a = ctx.ExecuteActivity<int>(opts, "Fetch", 1);
auto b = ctx.ExecuteActivity<int>(opts, "Fetch", 2);
auto c = ctx.ExecuteActivity<int>(opts, "Fetch", 3);
int total = a.Get() + b.Get() + c.Get();   // all three ran in parallel
```

### Activity options

```cpp
struct ActivityOptions {
  std::string task_queue;                                  // default: the workflow's task queue
  std::chrono::milliseconds schedule_to_close_timeout{0};
  std::chrono::milliseconds schedule_to_start_timeout{0};
  std::chrono::milliseconds start_to_close_timeout{0};     // effectively required
  std::chrono::milliseconds heartbeat_timeout{0};
  std::optional<RetryPolicy> retry_policy;                 // unset => server default retry behavior
};
```

To control retries, set the whole `retry_policy` optional — an unset optional means the server's
default retry behavior:

```cpp
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};
```

```cpp
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};
  double backoff_coefficient{2.0};
  std::chrono::milliseconds maximum_interval{0};   // 0 => 100 × initial_interval
  int maximum_attempts{0};                         // 0 => unlimited
  std::vector<std::string> non_retryable_error_types;
};
```

## Writing activities

```cpp
std::string ChargeCard(temporal::activity::Context& ctx, std::string order_id, double amount) {
  // real I/O is fine here
  return charge(order_id, amount);
}
worker.RegisterActivity("ChargeCard", ChargeCard);
```

Throw `temporal::ApplicationError` to signal a failure (optionally non-retryable):

```cpp
throw temporal::ApplicationError("card declined", "CardDeclined", /*non_retryable=*/true);
```

### Heartbeating

Long-running activities should heartbeat so the server knows they're alive (and to fail fast if the
worker dies). Set a `heartbeat_timeout` on the activity options and call `RecordHeartbeat`
periodically:

```cpp
std::string ProcessBatch(temporal::activity::Context& ctx, int n) {
  for (int i = 0; i < n; ++i) {
    do_chunk(i);
    ctx.RecordHeartbeat(i);   // optional progress detail
  }
  return "done";
}
```

If the activity doesn't heartbeat within `heartbeat_timeout`, the server times it out and retries.

## Local activities

A **local activity** runs inline in the workflow worker — no activity-task round-trip — and records
its result as a marker; retries happen inline within the workflow task. Best for short, idempotent
steps where the scheduling overhead would dominate. Unlike `ExecuteActivity`, it resolves within the
call and returns the value directly (no `Future`):

```cpp
temporal::LocalActivityOptions o;
o.start_to_close_timeout = std::chrono::seconds(5);
o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};

int total = ctx.ExecuteLocalActivity<int>(o, "LocalAdd", base, 5);
```

## Timers

```cpp
using namespace temporal::literals;            // brings in the chrono duration literals

ctx.Sleep(5s);                                  // block for 5s

auto timer = ctx.NewTimer(30s);
// ... do other things ...
timer.Get();                                    // block until it fires
```

`Sleep`/`NewTimer` take any `std::chrono` duration; `temporal::literals` re-exports the standard
duration literals (`5s`, `30s`, `24h`, …) so options and timers read naturally. Timers are durable:
a workflow sleeping for 30 days survives worker restarts.

## Escaping determinism: side effects & versioning

Workflow code is replayed, so it can't directly call `rand()`, read a clock, or generate a UUID. The
context provides recorded escape hatches for exactly these cases:

```cpp
// Runs fn once, records the result to history; on replay the recorded value is returned.
std::string id = ctx.SideEffect<std::string>([] { return new_uuid(); });

// Like SideEffect, but keyed and only writes a new marker when the value changes.
// R is deduced from the callable — no explicit template argument.
int cfg = ctx.MutableSideEffect("config", [&] { return load_config_version(); });
```

When you change a deployed workflow's logic, `GetVersion` lets old and new histories coexist on
replay. It records a version marker the first time it runs and returns `kDefaultVersion` (`-1`) when
replaying history recorded before the call existed:

```cpp
int v = ctx.GetVersion("greeting-change", temporal::workflow::kDefaultVersion, 1);
if (v == temporal::workflow::kDefaultVersion) {
  // original behavior
} else {
  // new behavior (v == 1)
}
```

## Search attributes

Make a running workflow discoverable in visibility queries by upserting indexed search attributes.
Build typed values with the `temporal::sa::` helpers (the attribute must already be registered on the
namespace):

```cpp
#include <temporal/common/search_attribute.h>

ctx.UpsertSearchAttributes({{"Tier", temporal::sa::Keyword("gold")}});
```

## Determinism rules

Workflow code is replayed, so it must be deterministic. Inside a workflow:

- ✅ Use `ctx.ExecuteActivity`, `ctx.Sleep`/`NewTimer`, signals, queries, selectors, child workflows.
- ✅ Use `ctx.GetLogger()` for logging and `ctx.GetInfo()` for metadata.
- ✅ Reach for `ctx.SideEffect` / `ctx.MutableSideEffect` when you need a one-time recorded value
  (id, randomness, a clock read), and `ctx.GetVersion` to evolve deployed workflow logic safely.
- ❌ Don't call `rand()`, read the wall clock, sleep with `std::this_thread::sleep_for`, do network
  or disk I/O, or spawn threads. Put all of that in **activities**.
- ❌ Don't `catch (...)` around an awaiting call in a way that could swallow the engine's internal
  control-flow exceptions.

:::note
The worker detects non-determinism on replay: if a workflow's replayed commands diverge from
recorded history, it applies the `WorkflowPanicPolicy` (default `BlockWorkflow` — fail the task so a
corrected build can recover). Use `ctx.GetVersion` to change deployed workflow logic without breaking
in-flight executions. See the [parity matrix](/parity).
:::
