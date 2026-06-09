---
title: Selectors, child workflows & more
description: Selectors, child workflows, continue-as-new, and cancellation.
---

# Selectors, child workflows & more

## Selectors

A `Selector` waits on several futures and proceeds when **any** one is ready — the canonical
"activity OR timeout" pattern:

```cpp
#include <temporal/workflow/selector.h>

std::string WithTimeout(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);

  auto work    = ctx.ExecuteActivity<std::string>(o, "SlowWork", "x");
  auto timeout = ctx.NewTimer(std::chrono::seconds(5));

  std::string out;
  temporal::workflow::Selector selector(ctx);
  selector.AddFuture<std::string>(work, [&](std::string r) { out = "done: " + r; });
  selector.AddFuture(timeout, [&]() { out = "timed out"; });
  selector.Select();   // runs the first ready case's handler
  return out;
}
```

- `AddFuture<T>(future, handler)` registers a value future; the `AddFuture(future, handler)` overload
  for a `Future<void>` (a timer, or `AwaitCancellation()`) is **not** templated — no `<void>`.
- `AddReceive<T>(channel, handler)` adds a signal-channel case: ready when a signal is buffered,
  consuming it and passing the value to the handler.
- `AddDefault(handler)` makes `Select()` non-blocking (runs the default if nothing is ready).
- `Select()` parks the workflow (via the coroutine) until a case becomes ready.

The same selector can mix futures, timers, and signal receives — e.g. "first signal OR timeout":

```cpp
std::string out;
temporal::workflow::Selector selector(ctx);
selector.AddReceive<std::string>(ctx.GetSignalChannel<std::string>("go"),
                                 [&](std::string s) { out = "signal:" + s; });
selector.AddFuture(ctx.NewTimer(std::chrono::seconds(5)), [&] { out = "timeout"; });
selector.Select();
```

## Child workflows

Start another workflow as a child and await its result:

```cpp
std::string Parent(temporal::workflow::Context& ctx, std::string name) {
  temporal::ChildWorkflowOptions o;
  o.task_queue = ctx.GetInfo().task_queue;   // default: the parent's task queue
  return ctx.ExecuteChildWorkflow<std::string>(o, "GreetChild", name).Get();
}

std::string GreetChild(temporal::workflow::Context& ctx, std::string name) {
  return "Hello, " + name;
}
```

The child runs as an independent workflow execution; the parent's `Future` resolves when the child
completes (or throws `ActivityError` if it fails). The child's id is derived deterministically from
the parent's (override with `ChildWorkflowOptions::id`).

### Parent-close policy

`ChildWorkflowOptions::parent_close_policy` decides what happens to a still-running child when the
parent closes:

```cpp
temporal::ChildWorkflowOptions o;
o.parent_close_policy = temporal::ParentClosePolicy::Abandon;   // let it outlive the parent
ctx.ExecuteChildWorkflow<std::string>(o, "SleeperChild");       // fire-and-forget (don't await)
```

- `Terminate` *(default)* — the server kills the child when the parent closes.
- `Abandon` — the child keeps running independently.
- `RequestCancel` — the server requests the child's cancellation.

To signal or cancel a specific child while it runs, give it a known `id` and use
`ctx.SignalExternalWorkflow(child_id, ...)` / `ctx.CancelExternalWorkflow(child_id)` (see
[signals, queries & updates](/workflows/messaging)).

## Nexus operations

A workflow can call a **Nexus operation** served by another namespace/team through a registered
endpoint. Unlike an activity, a Nexus operation's input and result are each a *single* value:

```cpp
auto fut = ctx.ExecuteNexusOperation<std::string, std::string>(
    "payments-endpoint", "billing.v1", "Charge", order_id);
std::string receipt = fut.Get();
```

`ExecuteNexusOperation<R, Arg>(endpoint, service, operation, input, schedule_to_close = {})` returns
a `Future<R>`; the optional last argument bounds the whole call (0 = the server default). The
endpoint is registered via `Client::CreateNexusEndpoint` and served by a worker that called
`Worker::RegisterNexusOperation`.

## Continue-as-new

For long-running or looping workflows, **continue-as-new** atomically completes the current run and
starts a fresh one — keeping history bounded:

```cpp
int Countdown(temporal::workflow::Context& ctx, int n) {
  if (n <= 0) return 0;
  ctx.ContinueAsNew("Countdown", n - 1);   // terminal: never returns
}
```

`ContinueAsNew(workflow_type, args...)` does not return — it restarts the workflow with new input.
The client's `Result()` transparently **follows the chain** to the final run:

```cpp
int result = handle.Result<int>();   // 0, after Countdown(3) -> 2 -> 1 -> 0
```

## Cancellation

A client can request cancellation of a running workflow:

```cpp
handle.Cancel();
```

The workflow observes it and decides how to react. The simplest form polls `ctx.IsCancelled()`:

```cpp
std::string Cancellable(temporal::workflow::Context& ctx) {
  auto channel = ctx.GetSignalChannel<std::string>("go");
  while (true) {
    if (ctx.IsCancelled()) {
      return "cleaned up";   // finish gracefully
    }
    channel.Receive();
  }
}
```

To *race* in-flight work against cancellation — and tear that work down when cancelled — add
`ctx.AwaitCancellation()` (a `Future<void>` that completes on cancel) as a Selector case and call
`Cancel()` on the operation's future:

```cpp
std::string RaceWork(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);
  auto act       = ctx.ExecuteActivity<std::string>(o, "SlowWork", "x");
  auto cancelled = ctx.AwaitCancellation();

  std::string out;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(act, [&](std::string r) { out = r; });
  sel.AddFuture(cancelled, [&] { act.Cancel(); out = "cancelled"; });   // tear down the activity
  sel.Select();
  return out;
}
```

`Future::Cancel()` requests cancellation of the underlying operation (timers today) and makes a
subsequent `Get()` unblock immediately.

:::note
Structured cancellation scopes are not yet exposed — propagation is explicit: observe via
`IsCancelled()` / `AwaitCancellation()` and cancel each operation's `Future` yourself. See the
[parity matrix](/parity).
:::

## Coroutine authoring (`co_await`)

As an alternative to `Future::Get()`, a workflow may return `workflow::workflow_task<R>` and use
`co_await` on a future plus `co_return`. It lowers to the same commands, so command order and replay
are identical:

```cpp
#include <temporal/workflow/coro.h>

temporal::workflow::workflow_task<std::string> CoAwaitWorkflow(
    temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);
  std::string a = co_await ctx.ExecuteActivity<std::string>(o, "Echo", s);
  co_return a;
}
```

Register it exactly like a plain workflow function (`worker.RegisterWorkflow("CoAwaitWorkflow", ...)`).
