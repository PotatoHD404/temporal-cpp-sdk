---
title: Timeouts & retries
description: How the SDK detects activity and workflow failures with timeouts and mitigates them with retry policies and heartbeats.
---

# Timeouts & retries

Temporal's failure-detection model rests on a simple division of labor:
**timeouts detect failures, retries mitigate them.** A timeout is how the server
notices that something went wrong — a worker died, a task sat in the queue too
long, an attempt ran away. A retry policy is how the server reacts — schedule
another attempt, with backoff, until the work succeeds or you give up.

This page is the reference for the timeout and retry knobs the C++ SDK exposes.
All option types live in `<temporal/common/options.h>`; the activity-side
heartbeat API lives in `<temporal/activity/activity.h>`. For the *failure types*
that surface when these limits are hit — `ActivityError`, `WorkflowFailedError`,
`ApplicationError` — see [Errors, retries & timeouts](/error-handling).

Examples assume the chrono literals are in scope:

```cpp
using namespace temporal::literals;  // 30s, 500ms, 10min, …
```

## Activity timeouts

`ActivityOptions` carries four timeout fields, all
`std::chrono::milliseconds` (assign any `std::chrono` duration — `30s`, `500ms`,
`10min` — and it converts):

```cpp
struct ActivityOptions {
  std::string task_queue;                                 // default: the workflow's task queue
  std::chrono::milliseconds schedule_to_close_timeout{0}; // total budget, all attempts
  std::chrono::milliseconds schedule_to_start_timeout{0}; // max time queued before pickup
  std::chrono::milliseconds start_to_close_timeout{0};    // one attempt's max run (effectively required)
  std::chrono::milliseconds heartbeat_timeout{0};         // max gap between heartbeats
  std::optional<RetryPolicy> retry_policy;                // unset => server default retry behavior
};
```

Each measures a different segment of an activity's life:

- **`start_to_close_timeout`** — the maximum time a **single attempt** may run
  once a worker has started executing it. This is the one you almost always set.
  When an attempt exceeds it, the server fails that attempt and (per the retry
  policy) schedules another. The header marks it *effectively required*: the
  server rejects a schedule command that carries no timeout at all, so always set
  this or `schedule_to_close_timeout`.
- **`schedule_to_start_timeout`** — the maximum time a task may wait **in the
  task queue** before a worker picks it up. It catches the "no healthy worker is
  polling this queue" condition. A task that times out here has not run yet, so it
  is safe to retry even for non-idempotent work. Leave it unset unless you have a
  reason to fail fast on queue backlog.
- **`schedule_to_close_timeout`** — the **total** budget from the moment the
  activity is scheduled until it finally succeeds or fails, **spanning all
  retries**. Use it to put an absolute ceiling on an activity that would otherwise
  retry for a long time under `start_to_close_timeout` × many attempts.
- **`heartbeat_timeout`** — the maximum gap allowed between
  `RecordHeartbeat` calls. It is how the server detects a worker that died
  mid-execution **before** `start_to_close_timeout` would expire. Required in
  practice for any long-running activity (see [Heartbeating](#heartbeating)).

```cpp
std::string ChargeCardWorkflow(temporal::workflow::Context& ctx,
                               std::string order_id, std::string amount) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout    = 30s;   // each attempt gets 30 s to run
  o.schedule_to_start_timeout = 10s;   // fail fast if no worker picks it up
  o.schedule_to_close_timeout = 5min;  // give up entirely after 5 min of retries
  return ctx.ExecuteActivity<std::string>(o, "ChargeCard", order_id, amount).Get();
}
```

:::note
`start_to_close_timeout` bounds **one attempt**; `schedule_to_close_timeout`
bounds **all attempts together**. If you set both, the activity fails as soon as
*either* limit is hit. Setting only `schedule_to_close_timeout` also satisfies the
"a timeout is required" rule, but `start_to_close_timeout` is usually the clearer
choice because it bounds the unit of work that actually retries.
:::

## Heartbeating

A long-running activity should periodically tell the server it is still alive by
calling `RecordHeartbeat` on its `activity::Context`. Pair it with a
`heartbeat_timeout` shorter than the longest gap between calls: if the activity
stops heartbeating within that window — because the worker crashed, hung, or lost
the network — the server treats the activity as timed out and schedules a retry,
rather than waiting out a much longer `start_to_close_timeout`.

```cpp
temporal::ActivityOptions o;
o.start_to_close_timeout = 10min;  // a single pass may legitimately take minutes
o.heartbeat_timeout      = 15s;    // but it must check in every 15 s
o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};
```

`RecordHeartbeat` is variadic — pass any data-convertible progress **details**
and they are reported to the server:

```cpp
std::string ProcessLargeFile(temporal::activity::Context& ctx, std::string path) {
  const int total_chunks = CountChunks(path);
  for (int chunk = 0; chunk < total_chunks; ++chunk) {
    ProcessChunk(path, chunk);
    ctx.RecordHeartbeat(chunk);        // report liveness + progress; resets the heartbeat clock
    if (ctx.IsCancelled()) {           // server asked us to stop
      return "cancelled";              // return promptly and cooperatively
    }
  }
  return "done";
}
```

Two behaviors make this safe to call from a tight loop:

- **Auto-throttling.** The worker only sends an *actual* server report when at
  least ~80% of the `heartbeat_timeout` has elapsed since the last one (the first
  call always reports). Calling `RecordHeartbeat` thousands of times a second does
  **not** flood the server — only the periodic reports hit the wire. The throttle
  interval is derived from the activity's `heartbeat_timeout` automatically.
- **Cancellation via heartbeat.** Cancellation is delivered on the heartbeat
  channel, so `IsCancelled()` reflects the cancel state observed by the most
  recent *actual* report. A throttled call does not refresh it, so it may lag the
  server by up to one throttle interval — harmless, because the next un-throttled
  heartbeat surfaces it. Poll `IsCancelled()` in your loop and return promptly
  when it becomes true.

The progress **details** you report survive into the next attempt: when a
heartbeating activity times out and the server retries it, the new attempt can
read the last reported details and resume rather than restart from scratch.

:::note
Only a heartbeating activity can observe cancellation — `IsCancelled()` is
refreshed exclusively by heartbeat reports. An activity that never calls
`RecordHeartbeat` will never see a cancel request through its context.
:::

## Workflow timeouts

`StartWorkflowOptions` carries three workflow-level timeouts (also
`std::chrono::milliseconds`). These bound the workflow execution itself, not the
activities it schedules:

```cpp
struct StartWorkflowOptions {
  std::string id;                              // default: a random UUID
  std::string task_queue;                      // required
  std::chrono::milliseconds execution_timeout{0};
  std::chrono::milliseconds run_timeout{0};
  std::chrono::milliseconds task_timeout{0};
  std::optional<RetryPolicy> retry_policy;     // unset => server default retry behavior
  // … memo, search_attributes, headers …
};
```

- **`execution_timeout`** — the maximum duration of the **entire workflow chain**,
  including all retries of the workflow and every `ContinueAsNew` link. This is
  the absolute wall-clock ceiling on "this business process," end to end.
- **`run_timeout`** — the maximum duration of a **single run** (one execution
  between continue-as-new boundaries). With no continue-as-new and no workflow
  retries, a run and the execution coincide.
- **`task_timeout`** — the maximum time for a single **workflow task** (one slice
  of decision/replay work on a worker). This is an internal tuning knob and is
  **rarely changed**; the default suits almost all workflows. Raise it only if a
  workflow task does unusually heavy in-process computation between awaits.

```cpp
temporal::StartWorkflowOptions o;
o.task_queue        = "orders";
o.execution_timeout = 24h;   // the whole order saga must finish within a day
o.run_timeout       = 1h;    // any single run is capped at an hour
auto handle = client.StartWorkflow(o, "OrderSaga", order_id);
```

When a workflow hits one of these limits it ends non-successfully, and the client
sees a `WorkflowFailedError` from `WorkflowHandle::Result<R>()` (with an empty
`type()`, since a timeout is not an application failure). See
[Workflow failure](/error-handling#workflow-failure).

## Retry policies

A `RetryPolicy` controls how the server re-attempts failed work. The same
`std::optional<RetryPolicy>` field appears on `ActivityOptions`,
`ChildWorkflowOptions`, `LocalActivityOptions`, and `StartWorkflowOptions`:

```cpp
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};  // delay before the first retry (default 1 s)
  double backoff_coefficient{2.0};                   // interval multiplier per retry (default 2.0)
  std::chrono::milliseconds maximum_interval{0};     // cap on the interval; 0 => 100 × initial_interval
  int maximum_attempts{0};                           // total attempts; 0 => unlimited
  std::vector<std::string> non_retryable_error_types;
};
```

The delay before retry *n* is `initial_interval × backoff_coefficient^(n-1)`,
clamped to `maximum_interval`. With the defaults that is 1 s, 2 s, 4 s, 8 s, …

**Leaving `retry_policy` unset is not the same as disabling retries.** An unset
optional means *server defaults*, which are unlimited retries with exponential
backoff. To cap or change retry behavior you must assign a policy:

```cpp
temporal::ActivityOptions o;
o.start_to_close_timeout = 30s;
o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};  // at most 3 attempts
```

:::note
Designated-initializer syntax (`RetryPolicy{.maximum_attempts = 3}`) only sets the
fields you name; the rest keep their struct defaults (`initial_interval = 1s`,
`backoff_coefficient = 2.0`, …). This is the idiom used throughout the integration
tests.
:::

A few common shapes:

```cpp
// Tune backoff and cap attempts:
o.retry_policy = temporal::RetryPolicy{
    .initial_interval    = 500ms,
    .backoff_coefficient = 1.5,
    .maximum_interval    = 30s,
    .maximum_attempts    = 5,
};

// Exactly one attempt — fail fast, no retries:
o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 1};

// Never retry these failure types, regardless of maximum_attempts:
o.retry_policy = temporal::RetryPolicy{
    .non_retryable_error_types = {"CardDeclined", "InvalidInput"},
};
```

### Stopping retries from inside an activity

There are two ways an activity attempt can declare a failure permanent, so the
server abandons it immediately instead of scheduling further attempts:

1. **Throw a non-retryable `ApplicationError`** — set the constructor's third
   argument:

   ```cpp
   // Inside the activity:
   throw temporal::ApplicationError("card declined — do not retry",
                                    "CardDeclined", /*non_retryable=*/true);
   ```

2. **List the error's type in `non_retryable_error_types`** — if the type string
   of a thrown `ApplicationError` appears in that list, the activity is
   non-retryable even when the constructor flag is `false` and `maximum_attempts`
   has not been reached.

Either way the retry loop stops on that attempt. Any other thrown exception is
retryable by default and will be re-attempted until the policy is exhausted. See
[Throwing failures from an activity](/error-handling#throwing-failures-from-an-activity)
for the full constructor.

## How a failure reaches the workflow

Retries and timeouts are handled by the server, transparently to the workflow —
the workflow does not see the intermediate failed attempts. It only observes the
outcome when it awaits the activity's result. Once retries are exhausted (or
`maximum_attempts` was 1, or a non-retryable error was thrown), the
`Future<R>::Get()` call throws `temporal::ActivityError`:

```cpp
temporal::ActivityOptions o;
o.start_to_close_timeout = 30s;
o.heartbeat_timeout      = 15s;
o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};

try {
  std::string result = ctx.ExecuteActivity<std::string>(o, "ProcessLargeFile", path).Get();
  // every attempt's failures were absorbed by retries; this ran successfully
} catch (const temporal::ActivityError& e) {
  // retries exhausted, a non-retryable error, or a timeout
  if (e.type().find("Timeout") != std::string::npos) {
    ctx.GetLogger().Warn("activity timed out", {{"type", e.type()}});
  }
  throw;  // re-throw to fail the workflow, or handle gracefully
}
```

`ActivityError::type()` echoes the failure type. For a timeout it is one of
`"TimeoutType_START_TO_CLOSE"`, `"TimeoutType_SCHEDULE_TO_CLOSE"`, or
`"TimeoutType_HEARTBEAT"`; for an application failure it is the type string you
passed to `ApplicationError`. Matching on it is covered in
[Catching activity failures in a workflow](/error-handling#catching-activity-failures-in-a-workflow).

## Choosing timeouts

A practical starting point:

- **Always set `start_to_close_timeout`** to a generous-but-finite bound on a
  single attempt of the activity — long enough that a healthy run never trips it,
  short enough that a hung worker is noticed in reasonable time.
- **Add `heartbeat_timeout` (and heartbeat) for any activity that can run longer
  than a few seconds.** It is the only timeout that detects a dead worker
  *during* an attempt; without it you wait out the full `start_to_close_timeout`.
- **Use `schedule_to_close_timeout`** when you need an absolute end-to-end
  ceiling across retries, e.g. "this must succeed within 10 minutes or escalate."
- **Reach for `schedule_to_start_timeout`** only to fail fast on queue backlog;
  it is safe to retry because the task has not started running.
- **Leave `task_timeout` (and usually `run_timeout`) at the default.** Set
  `execution_timeout` to bound the whole business process when one applies.
- **Cap retries deliberately.** The server default is unlimited; assign a
  `RetryPolicy` with `maximum_attempts` (and mark genuinely permanent failures
  non-retryable) so a doomed activity does not retry forever.

See the [parity matrix](/parity) for the full feature status, and
[Errors, retries & timeouts](/error-handling) for the failure types these limits
produce.
