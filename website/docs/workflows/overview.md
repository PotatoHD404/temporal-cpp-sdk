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
  RetryPolicy retry_policy;
  bool retry_policy_set = false;                           // set true to apply retry_policy
};
```

To control retries, set `retry_policy` and `retry_policy_set = true`:

```cpp
opts.retry_policy.maximum_attempts = 3;
opts.retry_policy_set = true;
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

## Timers

```cpp
ctx.Sleep(std::chrono::seconds(5));            // block for 5s

auto timer = ctx.NewTimer(std::chrono::seconds(30));
// ... do other things ...
timer.Get();                                    // block until it fires
```

Timers are durable: a workflow sleeping for 30 days survives worker restarts.

## Determinism rules

Workflow code is replayed, so it must be deterministic. Inside a workflow:

- ✅ Use `ctx.ExecuteActivity`, `ctx.Sleep`/`NewTimer`, signals, queries, selectors, child workflows.
- ✅ Use `ctx.GetLogger()` for logging and `ctx.GetInfo()` for metadata.
- ❌ Don't call `rand()`, read the wall clock, sleep with `std::this_thread::sleep_for`, do network
  or disk I/O, or spawn threads. Put all of that in **activities**.
- ❌ Don't `catch (...)` around an awaiting call in a way that could swallow the engine's internal
  control-flow exceptions.

:::note
This SDK does not yet implement automatic non-determinism *detection* or workflow *versioning*
(`GetVersion`/patching). Until it does, changing a deployed workflow's logic can break in-flight
executions on replay — the same hazard every Temporal SDK has, but without the guard rails. See the
[parity matrix](/parity).
:::
