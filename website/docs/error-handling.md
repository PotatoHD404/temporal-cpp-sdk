---
title: Errors, retries & timeouts
description: How to signal failures from activities, configure retry policies and timeouts, and handle errors in workflow code.
---

# Errors, retries & timeouts

This page covers how the SDK propagates failures between activities and workflows,
how you control retry behavior and timeouts, and what happens when a workflow
itself fails or encounters a non-determinism violation.

All error types live in `<temporal/common/errors.h>`.

## Error hierarchy

```
std::runtime_error
  └── temporal::TemporalError
        ├── temporal::ApplicationError     — thrown by your activity/workflow code
        ├── temporal::ActivityError        — received in the workflow when an activity fails
        ├── temporal::WorkflowFailedError  — received by a client when a workflow ends badly
        ├── temporal::DataConverterError   — payload encode/decode failure
        └── temporal::RpcError             — transport / gRPC failure to the Temporal service
              └── temporal::NotFoundError  — server returned NOT_FOUND (unknown workflow / schedule / namespace)
```

`NotFoundError` derives from `RpcError`, so a `catch (const temporal::RpcError&)`
still catches it (and `not_found()` returns `true`); new code can
`catch (const temporal::NotFoundError&)` directly to treat "absent" distinctly
from a transport failure.

## Throwing failures from an activity

Inside an activity, throw `temporal::ApplicationError` to signal a named,
typed failure. The constructor takes a human-readable message, an optional
**type string**, and an optional **non-retryable flag**:

```cpp
// Retryable by default (the server will try again per the retry policy):
throw temporal::ApplicationError("downstream service unavailable", "ServiceUnavailable");

// Non-retryable: the server will not schedule another attempt.
throw temporal::ApplicationError("card declined — do not retry", "CardDeclined",
                                 /*non_retryable=*/true);

// Message only (type is empty string, retryable):
throw temporal::ApplicationError("unexpected nil response");
```

The type string is a free-form identifier you choose. It is used in two ways:

1. **`RetryPolicy::non_retryable_error_types`** — if the type of a thrown
   `ApplicationError` appears in this list, the activity is treated as
   non-retryable regardless of the `non_retryable` constructor flag.
2. **Catch sites in the workflow** — you can inspect `ActivityError::type()` to
   distinguish failure kinds.

You can also throw `ApplicationError` from a workflow or an update validator; see
[Update validators](/advanced#update-validators) for that usage.

## Catching activity failures in a workflow

When a `Future<R>::Get()` call resolves to a failed activity (one that exhausted
all retry attempts or threw a non-retryable error), it throws
`temporal::ActivityError`. The `type()` accessor returns the error type string
from the original `ApplicationError`.

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};

try {
  std::string receipt =
      ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, amount).Get();
  // activity succeeded
} catch (const temporal::ActivityError& e) {
  ctx.GetLogger().Error("ChargeCard failed", {{"type", e.type()}, {"what", e.what()}});

  if (e.type() == "CardDeclined") {
    // non-retryable, handle gracefully
    return "payment-declined";
  }
  throw;  // re-throw other failures to fail the workflow
}
```

:::note
Only `temporal::ActivityError` is thrown by `Future::Get()` for activity
failures. You do not need to catch `ApplicationError` in workflow code — that
exception type is for the *activity side* to throw, not the workflow side to
receive.
:::

## Configuring retries with `RetryPolicy`

`ActivityOptions::retry_policy` is a `std::optional<RetryPolicy>`. Leave it unset
(the default) and the server applies its built-in defaults; assign a value to
override them — `opts.retry_policy = temporal::RetryPolicy{...};`. The same
optional field appears on `ChildWorkflowOptions`, `LocalActivityOptions`, and
`StartWorkflowOptions`.

```cpp
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};  // first retry delay (default 1 s)
  double backoff_coefficient{2.0};                   // multiplier per retry
  std::chrono::milliseconds maximum_interval{0};     // 0 => 100 × initial_interval
  int maximum_attempts{0};                           // 0 => unlimited
  std::vector<std::string> non_retryable_error_types;
};

// On ActivityOptions:
std::optional<RetryPolicy> retry_policy;  // unset => server default retry behavior
```

### Typical patterns

**Limit attempts and tune backoff:**

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);

opts.retry_policy = temporal::RetryPolicy{
    .initial_interval    = std::chrono::milliseconds(500),
    .backoff_coefficient = 1.5,
    .maximum_interval    = std::chrono::seconds(30),
    .maximum_attempts    = 5,
};
```

**Mark specific error types as non-retryable by name:**

```cpp
opts.retry_policy = temporal::RetryPolicy{
    .non_retryable_error_types = {"CardDeclined", "InvalidInput"},
};
```

If the activity throws `ApplicationError("...", "CardDeclined")`, the server
stops retrying immediately — even if `maximum_attempts` has not been reached
and the `non_retryable` constructor flag is `false`.

**Fail on the first attempt (no retries):**

This is the pattern used in the integration tests for the `FailWorkflow` case:

```cpp
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 1};
```

:::note
Leaving `retry_policy` unset is not the same as a one-attempt policy: an unset
optional means *server defaults* (unlimited retries with exponential backoff). To
cap or disable retries you must assign a `RetryPolicy` with the fields you want.
:::

## Activity timeouts

`ActivityOptions` exposes four timeout fields:

```cpp
struct ActivityOptions {
  std::chrono::milliseconds schedule_to_close_timeout{0}; // total budget from schedule to finish
  std::chrono::milliseconds schedule_to_start_timeout{0}; // max time waiting in the task queue
  std::chrono::milliseconds start_to_close_timeout{0};    // per-attempt execution time (effectively required)
  std::chrono::milliseconds heartbeat_timeout{0};         // max gap between RecordHeartbeat calls
  // ...
};
```

`start_to_close_timeout` is the one you almost always set. It bounds how long a
single execution attempt may run before the server considers the activity failed
and schedules a retry (or a final failure if retries are exhausted). The comment
in the header marks it *effectively required* — the server will reject a schedule
command that carries no timeout at all.

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);  // each attempt has 30 s
```

### Heartbeat timeout

For long-running activities that call `ctx.RecordHeartbeat()`, set
`heartbeat_timeout` to a value shorter than the longest gap between heartbeat
calls. If the activity fails to heartbeat within the window, the server treats
the activity as timed out and schedules a retry:

```cpp
opts.start_to_close_timeout  = std::chrono::minutes(10);
opts.heartbeat_timeout        = std::chrono::seconds(15);  // must heartbeat every 15 s
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};
```

`RecordHeartbeat` throttles the actual server round-trips automatically (to
roughly 80% of `heartbeat_timeout`), so a tight loop can call it freely — only
the periodic reports hit the wire, and the cached cancel state is refreshed on
each real report.

Inside the activity, heartbeat regularly and check `IsCancelled()` so the
activity can stop cooperatively when the server requests it:

```cpp
std::string ProcessLargeFile(temporal::activity::Context& ctx, std::string path) {
  for (int chunk = 0; chunk < total_chunks; ++chunk) {
    process(path, chunk);
    ctx.RecordHeartbeat(chunk);      // resets the heartbeat clock; returns cancel flag
    if (ctx.IsCancelled()) {
      return "cancelled";            // stop promptly when requested
    }
  }
  return "done";
}
```

### What a timeout looks like to the workflow

A timed-out activity follows the same retry path as any other failure. After the
last retry is exhausted (or if `maximum_attempts` was 1), `Future::Get()` throws
`temporal::ActivityError`. The `type()` string in that case will be
`"TimeoutType_SCHEDULE_TO_CLOSE"`, `"TimeoutType_START_TO_CLOSE"`, or
`"TimeoutType_HEARTBEAT"` — you can match on it if you need to distinguish a
timeout from an application-level failure:

```cpp
try {
  result = ctx.ExecuteActivity<std::string>(opts, "ProcessLargeFile", path).Get();
} catch (const temporal::ActivityError& e) {
  if (e.type().find("Timeout") != std::string::npos) {
    ctx.GetLogger().Warn("activity timed out", {{"type", e.type()}});
  }
  throw;
}
```

## Workflow failure

A workflow function that **throws an unhandled exception** (anything other than
the SDK's internal control-flow signals for `ContinueAsNew`, timers, etc.) fails
the workflow execution. The Temporal server marks the execution `FAILED` and
`WorkflowFailedError` is thrown on the client side when `WorkflowHandle::Result<R>()`
is called.

On the **client**, catching workflow failure looks like:

```cpp
try {
  std::string result = handle.Result<std::string>();
} catch (const temporal::WorkflowFailedError& e) {
  std::cerr << "Workflow failed: " << e.what() << '\n';
  if (e.type() == "CardDeclined") { /* application-failure type, decoded client-side */ }
}
```

`WorkflowFailedError::type()` echoes the failure *type* of the application error
the workflow threw, decoded on the client side from the close event — so you can
discriminate failure kinds without parsing the message. It is empty for
non-application closes (timeout, terminated, canceled). The same error is raised
when a workflow times out, is terminated, or is canceled.

:::note
If you deliberately want to fail a workflow from within its code, throw
`temporal::ApplicationError`. Throwing any other unhandled exception also fails
the workflow, but `ApplicationError` is the idiomatic, typed way to signal an
intentional terminal failure.
:::

## Non-determinism and `WorkflowPanicPolicy`

A different class of failure occurs when the workflow engine replays history and
the workflow's commands do not match what was recorded. This is a
**non-determinism violation** — it means the workflow code changed in an
incompatible way while executions were in flight.

The SDK responds per `WorkflowPanicPolicy`, configured on the worker:

```cpp
temporal::WorkerOptions wopts;
wopts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;  // default
temporal::worker::Worker worker(client, "my-task-queue", wopts);
```

- **`BlockWorkflow`** (default) — fails only the current **workflow task**. The
  server retries the task, so deploying a corrected worker recovers the
  workflow without data loss. Mirrors the Go/Java SDKs' `BlockWorkflow` behavior.
- **`FailWorkflow`** — fails the entire **workflow execution**. Terminal; use
  only when a stuck, unrecoverable workflow is worse than a failed one.

See [Advanced capabilities: Non-determinism detection](/advanced#non-determinism-detection)
for the full treatment, including `GetVersion` for safe code evolution and the
[Replay testing](/advanced#replay-testing) pattern to catch violations in CI
before they reach production.

## Related failure-handling capabilities

A few capabilities that touch error handling live elsewhere in the docs:

- **Custom failure converters** — plug in a `FailureConverter` to control how
  errors encode/decode on the wire; see [Data conversion](/data-conversion).
- **Async (manual) completion** — an activity can defer with
  `ctx.defer_completion()` and be finished out-of-band via
  `Client::CompleteActivity` / `Client::FailActivity`; see [Activities](#async-manual-activity-completion)
  below.
- **Activity-side cancellation** — a workflow's `Future::Cancel()` on an activity
  future requests cancellation; the activity observes it through
  `Context::IsCancelled()` on its next heartbeat.

See the [parity matrix](/parity) for the full feature status.

## Async (manual) activity completion

An activity does not have to return its result inline. Calling
`ctx.defer_completion()` tells the worker **not** to report a result when the
function returns, and hands back the task token to finish the activity later:

```cpp
std::string ChargeCard(temporal::activity::Context& ctx, std::string order_id) {
  const std::string token = ctx.defer_completion();  // sets async; returns the task token
  EnqueueExternalWork(order_id, token);              // some other system finishes it later
  return {};                                         // return value ignored once deferred
}
```

From anywhere holding a `Client`, complete or fail it by token:

```cpp
client.CompleteActivity(token, std::string("receipt-123"));      // workflow receives this result
client.FailActivity(token, "card declined", "CardDeclined");     // workflow sees an ActivityError
```

`FailActivity`'s third argument is the failure type (default `"ApplicationError"`),
which surfaces to the workflow as `ActivityError::type()` exactly like a thrown
`ApplicationError`.
