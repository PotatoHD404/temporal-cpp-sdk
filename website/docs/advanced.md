---
title: Advanced capabilities
description: Determinism detection, SideEffect/MutableSideEffect, GetVersion, update validators, cancellation, co_await, Nexus, cron, parent-close-policy, async completion, replay testing, and memo.
---

# Advanced capabilities

Beyond the core programming model, the SDK implements the determinism-critical and
production-leaning features below. Each is exercised by the test suite against a
real `temporal server start-dev`.

## `co_await` authoring mode {#co-await}

A workflow can be written in C++20-coroutine style — returning
`workflow::workflow_task<R>` and using `co_await` on a `Future` (then `co_return`)
instead of calling `.Get()`. It runs on the **same** stackful dispatcher as the
plain-function form: `co_await` delegates blocking to the existing engine, so the
emitted command order and replay are identical to the equivalent `.Get()`
workflow. There is no separate scheduler and no determinism change.

```cpp
#include <temporal/temporal.h>

temporal::workflow::workflow_task<std::string> CoAwaitWorkflow(
    temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);

  const std::string a = co_await ctx.ExecuteActivity<std::string>(o, "Echo", s);
  const std::string b = co_await ctx.ExecuteActivity<std::string>(o, "Echo", a + "!");
  co_return b;
}
```

Register and replay it exactly like any other workflow — `RegisterWorkflow` accepts
both forms, and a coroutine workflow's recorded history replays deterministically:

```cpp
worker.RegisterWorkflow("CoAwaitWorkflow", CoAwaitWorkflow);
// ... later, against its real history:
replayer.ReplayWorkflowHistory(history_json);  // clean — same commands as .Get()
```

You can still call `.Get()` on a future or `.Receive()` on a channel inside a
coroutine workflow; they block the same way. Use whichever style reads better — the
two are wire-identical.

## Non-determinism detection {#non-determinism-detection}

A workflow must be deterministic: replayed against its recorded history, it has to
emit exactly the same orchestration commands, in the same order. The engine
records the ordered stream of commands a workflow produces and matches it against
the command-generating events in history (modeled on the Go SDK's
`matchReplayWithHistory`). History is authoritative; the workflow may only emit
*additional* trailing commands (genuine forward progress).

A mismatch is surfaced per `WorkflowPanicPolicy`, set on the worker:

```cpp
temporal::WorkerOptions opts;
opts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;  // default
temporal::worker::Worker worker(client, "my-task-queue", opts);
```

- **`BlockWorkflow`** (default) — fail the workflow *task*. The server retries it,
  so deploying a corrected worker recovers the workflow without data loss.
- **`FailWorkflow`** — fail the workflow *execution* outright. Terminal.

The check runs only on a full-history replay (the resident sticky coroutine is the
source of truth and is never re-validated). See [Replay testing](#replay-testing)
to catch a non-deterministic change *before* you deploy it.

## SideEffect

`SideEffect` captures the result of a non-deterministic operation exactly once.
The first time it runs, your function executes and its result is recorded to
history; on every replay the recorded value is returned **without** running the
function again.

```cpp
int id = ctx.SideEffect<int>([] { return generate_random_id(); });
```

Use it for ids, randomness, or reading a clock — never for anything with
externally-visible effects (those belong in an activity).

## MutableSideEffect

`MutableSideEffect` is like `SideEffect` but keyed by an `id`: it only records a
new marker when the value *changes* (compared with `operator==`, or a custom
predicate). On replay the recorded value is returned without running the function.
Use it for a value that is recomputed often but usually stays the same — so you
avoid writing a marker on every call.

```cpp
// Records a marker only when the computed tier actually differs from last time.
// The result type is deduced from the function (no explicit template argument).
std::string tier = ctx.MutableSideEffect("tier", [&]() -> std::string {
  return compute_tier(state);
});

// Custom comparison: treat values within an epsilon as unchanged.
double price = ctx.MutableSideEffect(
    "price",
    [&]() -> double { return quote_price(); },
    [](double a, double b) { return std::abs(a - b) < 0.01; });
```

Like `SideEffect`, the function must return a value (it cannot be `void`).

## Local activities

`ExecuteLocalActivity` runs a *registered* activity inline in the workflow worker —
no activity-task round-trip — recording its result as a marker; on replay the
recorded result is returned without re-running. Retries happen inline per the
retry policy. Best for short, idempotent steps where the task-dispatch overhead
would dominate. Unlike `ExecuteActivity`, it resolves within the call (no `Future`):

```cpp
temporal::LocalActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(5);
// retry_policy is std::optional<RetryPolicy>; leave unset for the default inline
// retry behavior.

int total = ctx.ExecuteLocalActivity<int>(opts, "Tally", items);
```

## GetVersion (versioning / patching)

`GetVersion` lets you change workflow code while old executions are still running.
It records the chosen version the first time it runs and returns the recorded
version on replay, so both old and new histories stay deterministic.

```cpp
int v = ctx.GetVersion("greeting-change", temporal::workflow::kDefaultVersion, 1);
if (v == temporal::workflow::kDefaultVersion) {
  // original behavior, for executions that started before this change
  greet_v0(ctx);
} else {
  // new behavior
  greet_v1(ctx);
}
```

`kDefaultVersion` (-1) is returned when replaying history recorded *before* the
`GetVersion` call existed. Once every pre-change execution has drained, you can
drop the old branch and raise `min_supported`.

## Update validators {#update-validators}

An update handler can take an optional **read-only validator** that runs *before*
the update is accepted. If the validator throws, the update is rejected and the
handler never runs — and because a rejection is **not written to history**,
workflow state is untouched.

```cpp
ctx.SetUpdateHandler(
    "deposit",
    [&](int amount) { balance += amount; return balance; },  // handler
    [](int amount) {                                          // validator
      if (amount <= 0) {
        throw temporal::ApplicationError("amount must be positive", "InvalidDeposit");
      }
    });
```

On the client, a rejected update surfaces as a thrown exception:

```cpp
handle.Update<int>("deposit", -5);  // throws — validator rejected it
```

Validators must be read-only (no activities, timers, or state mutation), exactly
like query handlers.

## Cancelling operations

Operation futures expose `Cancel()`. For a **timer**, cancelling emits a
`CancelTimer` command and resolves the future immediately, so an awaiter unblocks
at once instead of waiting the timer out:

```cpp
auto timer = ctx.NewTimer(std::chrono::minutes(5));
// ... something else happened ...
timer.Cancel();   // the workflow won't wait the full 5 minutes
timer.Get();      // returns right away
```

Cancellation is deterministic — the workflow reproduces the `CancelTimer` command
on replay.

To **react** to a workflow-level cancellation (the "clean up and stop" pattern),
wait on `ctx.AwaitCancellation()` as a `Selector` case, racing it against your
work:

```cpp
auto work = ctx.NewTimer(std::chrono::minutes(5));
std::string out;
temporal::workflow::Selector sel(ctx);
sel.AddFuture(work, [&] { out = "done"; });
sel.AddFuture(ctx.AwaitCancellation(), [&] {
  work.Cancel();
  out = "cancelled";
});
sel.Select();
```

When the workflow is cancelled, `AwaitCancellation` completes, the selector takes
that branch, and the workflow cancels its timer and finishes promptly.

**Activities** can be cancelled the same way: call `Cancel()` on the activity's
`Future` to emit a `RequestCancelActivityTask`. A heartbeating activity observes
the request through `activity::Context::IsCancelled()` and returns promptly:

```cpp
std::string MyActivity(temporal::activity::Context& ctx, int n) {
  for (int i = 0; i < n; ++i) {
    do_some_work();
    ctx.RecordHeartbeat(i);
    if (ctx.IsCancelled()) return "cancelled";  // observed the cancel request
  }
  return "done";
}
```

Cancellation is delivered through heartbeats, so only a heartbeating activity can
observe it.

## Signalling and cancelling external workflows {#external-workflows}

A workflow can reach *another* running workflow by id — fire-and-forget, no handle
required. `SignalExternalWorkflow` encodes its arguments through the data converter
and delivers a signal; `CancelExternalWorkflow` requests the target's cancellation.

```cpp
// Deliver a "setName" signal to another workflow by id.
ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));

// Request cancellation of another workflow by id.
ctx.CancelExternalWorkflow(target_id);
```

Both are the standard way to signal a **child** workflow too: pass the child's id
(set `ChildWorkflowOptions::id` when you start it, or read it back) to
`SignalExternalWorkflow`. The child handle returned by `ExecuteChildWorkflow` is a
`Future` — it carries `Get()` and `Cancel()`, not a signal method.

## Replay testing

Replay a recorded history against your current workflow code — **without a
server** — to catch a non-deterministic change before it reaches production. This
is the single most valuable workflow unit test you can write.

```cpp
// Export a real history (e.g. from a workflow you just ran, or
// `temporal workflow show -o json`):
std::string history_json = handle.FetchHistoryJson();

// Replay it against the registered workflow; throws on non-determinism.
temporal::worker::Worker replayer(client, "replay");
replayer.RegisterWorkflow("MyWorkflow", MyWorkflow);
replayer.ReplayWorkflowHistory(history_json);  // throws if MyWorkflow diverged
```

Keep a few representative histories as test fixtures and replay them in CI; any
incompatible edit to a workflow (a reordered activity, a removed timer) fails the
test instead of a running workflow in production.

## Nexus operations {#nexus}

Nexus lets one workflow call an operation served by another team's worker —
possibly in another namespace — without coupling to that team's workflow types. The
SDK supports the full round-trip: a worker registers an operation handler, a client
registers the endpoint that routes to it, and a workflow calls it.

Register a handler `R Fn(Arg)` for a `(service, operation)` pair on the worker that
should serve it. Unlike an activity, a Nexus operation takes a **single** input
value and returns a **single** value (each one `Payload`):

```cpp
// Handler: R Fn(Arg) — one input, one result.
std::string Greet(std::string name) { return "Hello, " + name; }

worker.RegisterNexusOperation("greeting", "say-hello", Greet);
```

Register the endpoint on the client. It names the worker target — the task queue
(in this client's namespace) whose worker handles the operation — and returns the
new endpoint id:

```cpp
std::string endpoint_id =
    client.CreateNexusEndpoint("my-endpoint", "nexus-handler-tq");

// Look up / enumerate registered endpoints:
temporal::client::NexusEndpointDescription d = client.GetNexusEndpoint(endpoint_id);
std::vector<std::string> names = client.ListNexusEndpoints();
```

Call the operation from a workflow. `ExecuteNexusOperation<R, Arg>` takes the
endpoint *name*, the service, the operation, and the single input; an optional
`schedule_to_close` bounds the whole call (`0`/default = the server default). It
returns a `Future<R>`:

```cpp
std::string WorkflowUsingNexus(temporal::workflow::Context& ctx, std::string who) {
  return ctx.ExecuteNexusOperation<std::string, std::string>(
      "my-endpoint",   // endpoint name
      "greeting",      // service
      "say-hello",     // operation
      who              // single input value
  ).Get();             // or: co_await ... in coroutine style
}
```

:::note
The dev server only accepts the endpoint-management RPCs (and dispatches Nexus
operations) when Nexus is enabled, e.g.
`temporal server start-dev --dynamic-config-value system.enableNexus=true`.
Otherwise `CreateNexusEndpoint` throws `RpcError`.
:::

## Cron / calendar schedules {#cron}

`Client::CreateSchedule` runs a workflow on a recurring spec. Alongside a fixed
`interval`, a schedule can carry one or more **cron / calendar expressions** in
`ScheduleOptions::cron_expressions`: standard 5-field cron, optionally with a
leading seconds field and/or a trailing `CRON_TZ=<zone>`. Each string is one
trigger; combine them with `interval` or use them alone.

```cpp
temporal::ScheduleOptions opts;
opts.cron_expressions = {"0 9 * * MON-FRI"};  // weekdays at 09:00
opts.workflow_type = "ReportWorkflow";
opts.task_queue = "reports";

client.CreateSchedule("daily-report", opts);
bool exists = client.DescribeSchedule("daily-report");
client.DeleteSchedule("daily-report");
```

```cpp
// Seconds field + timezone: every 30 minutes, evaluated in Europe/Berlin.
opts.cron_expressions = {"0 0,30 * * * * CRON_TZ=Europe/Berlin"};
```

## Child workflows: parent-close policy {#parent-close-policy}

`ChildWorkflowOptions::parent_close_policy` decides what happens to a *running*
child when its parent closes:

- **`ParentClosePolicy::Terminate`** (default) — the server terminates the child.
- **`ParentClosePolicy::Abandon`** — the child keeps running independently.
- **`ParentClosePolicy::RequestCancel`** — the child is requested to cancel.

```cpp
std::string StartAbandonedChild(temporal::workflow::Context& ctx,
                                std::string child_id) {
  temporal::ChildWorkflowOptions co;
  co.id = child_id;
  co.parent_close_policy = temporal::ParentClosePolicy::Abandon;

  // Fire-and-forget: start the child and return without awaiting it. With
  // Abandon, the child outlives this parent.
  ctx.ExecuteChildWorkflow<std::string>(co, "LongChild");
  return "parent-done";
}
```

To signal the child later, use its id with `SignalExternalWorkflow` (see
[Signalling and cancelling external workflows](#external-workflows)).

## Async / manual activity completion {#async-completion}

An activity can finish *out of band*: instead of returning a result, it defers
completion and hands its task token to some external system (a webhook callback, a
human approval, another service), which later completes it through the client.

Call `defer_completion()` in the activity — it marks the task as
completing-asynchronously and returns the task token. The activity's own return
value is then ignored; the workflow's `Future` stays pending until the token is
resolved:

```cpp
std::string ApprovalActivity(temporal::activity::Context& ctx, std::string req) {
  const std::string token = ctx.defer_completion();  // async; returns the task token
  enqueue_for_human_review(req, token);              // hand it off elsewhere
  return {};                                          // ignored — completed later
}
```

Later — from anywhere holding a `Client` and the token — complete or fail it:

```cpp
// Success: the encoded result is what the workflow's Future yields.
client.CompleteActivity(token, std::string("approved"));

// Or failure (message + optional failure type):
client.FailActivity(token, "rejected by reviewer", "ApprovalRejected");
```

The token is `activity::Context::GetInfo().task_token`; `defer_completion()` is the
one-call shorthand for `SetWillCompleteAsync()` + reading that token.

## Search attributes, memo & sessions {#visibility-sessions}

A workflow can upsert **indexed search attributes** at runtime for visibility
queries (`Client::ListWorkflows` / `CountWorkflows`). Build typed values with the
`temporal::sa::` helpers; the named attribute must be registered on the namespace.

```cpp
ctx.UpsertSearchAttributes({{"Tier", temporal::sa::Keyword("gold")}});
```

Search attributes can also be set at start (`StartWorkflowOptions::search_attributes`)
the same way; non-indexed metadata goes in `memo` (see [Memo & Describe](#memo--describe)).

**Sessions** pin a sequence of activities to one worker host — useful when they
share host-local state (a downloaded file, a warmed cache). Create a session,
schedule activities on the returned task queue, then complete it to release the
slot. The worker must be started with `WorkerOptions::enable_sessions = true`.

```cpp
auto session = ctx.CreateSession();           // reserves a slot on some host
temporal::ActivityOptions opts;
opts.task_queue = session.task_queue;         // pin to that host
opts.start_to_close_timeout = std::chrono::seconds(30);
ctx.ExecuteActivity<void>(opts, "Download", url).Get();
ctx.ExecuteActivity<std::string>(opts, "Process").Get();  // same host
ctx.CompleteSession(session);                 // release the slot
```

## Memo & Describe {#memo--describe}

Attach non-indexed metadata to a workflow at start, and read a point-in-time
snapshot back:

```cpp
temporal::StartWorkflowOptions o;
o.task_queue = "my-task-queue";
o.memo["owner"] = dc->ToPayload(std::string("alice"));
auto handle = client.StartWorkflow(o, "MyWorkflow");

temporal::client::WorkflowDescription d = handle.Describe();
// d.status == "RUNNING", d.run_id, d.memo["owner"] -> "alice"
```

`Describe()` returns the workflow id, run id, status (e.g. `RUNNING`, `COMPLETED`),
and the memo. Memo is not indexed for search; typed/indexed search attributes are
not implemented yet.
