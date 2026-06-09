---
title: Cancellation & termination
description: Cancel a workflow gracefully or terminate it forcefully, observe cancellation inside a workflow, and tear down in-flight timers, activities, and child workflows.
---

# Cancellation & termination

There are two ways to stop a running workflow from a client, and they are not the same:

- **`WorkflowHandle::Cancel()`** — *cooperative / graceful.* The server records a cancel request;
  the workflow **observes** it and decides how to react: run a compensating activity, tear down
  in-flight work, persist final state, then return. Nothing is forced.
- **`WorkflowHandle::Terminate(reason)`** — *forceful / immediate.* The server closes the execution
  on the spot. The workflow gets **no chance to clean up** — no further code runs, in-flight
  activities are abandoned (their cancel is best-effort), and the reason is recorded on the close
  event.

```cpp
auto handle = client.GetHandle("order-123");
handle.Cancel();                      // ask the workflow to wind down
handle.Terminate("stuck; operator");  // kill it now, no cleanup
```

Reach for `Cancel()` whenever the workflow holds resources that need releasing or has external
side effects to compensate. Use `Terminate()` only when the workflow is wedged (e.g. a
non-deterministic bug, a deadlock) and graceful shutdown is impossible or not worth waiting for.

Either way, the client side surfaces the close as a `WorkflowFailedError` from `Result<R>()` — a
cancelled or terminated workflow did **not** complete successfully:

```cpp
try {
  handle.Result<std::string>();
} catch (const temporal::WorkflowFailedError& e) {
  // cancelled, terminated, failed, or timed out
}
```

## Observing cancellation inside a workflow

A cancel request is only useful if the workflow looks at it. Two primitives expose it.

### Poll with `IsCancelled()`

`ctx.IsCancelled()` returns `true` once a cancel has been requested. It is cheap and deterministic —
poll it between steps of a loop:

```cpp
std::string Cancellable(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("go");
  while (true) {
    if (ctx.IsCancelled()) {
      return "cancelled";   // wind down and finish
    }
    if (signals.Receive() == "stop") {
      return "stopped";
    }
  }
}
```

### Await with `AwaitCancellation()`

`ctx.AwaitCancellation()` returns a `Future<void>` that completes when the workflow is cancelled.
Unlike polling, this lets the workflow **block** on real work and still wake the instant a cancel
arrives — add it as a [`Selector`](/workflows/composition) case alongside the work:

```cpp
#include <temporal/workflow/selector.h>

std::string CancelAware(temporal::workflow::Context& ctx) {
  auto timer     = ctx.NewTimer(std::chrono::seconds(60));
  auto cancelled = ctx.AwaitCancellation();

  std::string out;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture(timer,     [&] { out = "timer-fired"; });
  sel.AddFuture(cancelled, [&] {
    timer.Cancel();        // tear down the in-flight work
    out = "cancelled";
  });
  sel.Select();            // wakes on whichever happens first
  return out;
}
```

Because `AwaitCancellation()` yields a `Future<void>`, register it with the **non-templated**
`AddFuture(future, handler)` overload — no `<void>` (same as a timer; see
[selectors](/workflows/composition)).

### Cleanup-on-cancel pattern

A graceful workflow runs a final compensating step before returning. The catch: that compensation
itself involves a normal activity, and the workflow is in a cancelled state — so do the cleanup
*after* observing the cancel, on the way out:

```cpp
std::string Booking(temporal::workflow::Context& ctx, std::string order) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);

  ctx.ExecuteActivity<void>(o, "Reserve", order).Get();

  // Wait for the go-ahead, but bail out on cancel.
  auto confirm   = ctx.GetSignalChannel<std::string>("confirm");
  auto cancelled = ctx.AwaitCancellation();

  bool aborted = false;
  temporal::workflow::Selector sel(ctx);
  sel.AddReceive<std::string>(confirm, [&](std::string) { /* proceed */ });
  sel.AddFuture(cancelled, [&] { aborted = true; });
  sel.Select();

  if (aborted) {
    ctx.ExecuteActivity<void>(o, "ReleaseReservation", order).Get();  // compensate
    return "released";
  }
  ctx.ExecuteActivity<void>(o, "Charge", order).Get();
  return "confirmed";
}
```

## Cancelling in-flight operations

Cancelling the *workflow* does not automatically cancel the *operations* it started — propagation is
explicit. Call `Future::Cancel()` on the specific future you want to tear down. The cancel command
is recorded to history, so it replays deterministically.

### Timers

`Cancel()` on a timer's future resolves it immediately — a 60-second timer that is cancelled returns
at once instead of waiting it out (history records `StartTimer` + `CancelTimer`):

```cpp
std::string TimerCancel(temporal::workflow::Context& ctx) {
  auto timer = ctx.NewTimer(std::chrono::seconds(60));
  timer.Cancel();
  timer.Get();          // returns immediately, not after 60s
  return "cancelled";
}
```

### Activities

`Cancel()` on an activity's future emits a `RequestCancelActivityTask`. The running activity sees the
request through its **heartbeat** — so only a heartbeating activity can observe a cancel. On the
activity side, call `RecordHeartbeat(...)` periodically and check `activity::Context::IsCancelled()`:

```cpp
// Activity: heartbeats, then returns promptly once cancel is requested.
std::string CancellableActivity(temporal::activity::Context& ctx, int) {
  for (int i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ctx.RecordHeartbeat(i);          // cancel is delivered via the heartbeat
    if (ctx.IsCancelled()) {
      return "cancelled";
    }
  }
  return "finished";
}

// Workflow: when cancelled, cancel the activity and report its result.
std::string ActivityCancel(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(60);
  o.heartbeat_timeout      = std::chrono::seconds(5);   // required to deliver cancel

  auto act       = ctx.ExecuteActivity<std::string>(o, "CancellableActivity", 0);
  auto cancelled = ctx.AwaitCancellation();

  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(act, [](std::string) {});  // activity finished on its own
  sel.AddFuture(cancelled, [&] { act.Cancel(); });      // workflow cancel -> cancel the activity
  sel.Select();
  return act.Get();   // "cancelled" once the activity observes the request
}
```

:::note Activities need a heartbeat timeout
An activity that never calls `RecordHeartbeat` — or whose `ActivityOptions::heartbeat_timeout` is
unset — cannot observe cancellation. Set the heartbeat timeout and heartbeat from any long-running
activity you intend to cancel.
:::

### Child workflows

`Cancel()` on a child-workflow future requests the child's cancellation; the child observes it like
any other workflow (`IsCancelled()` / `AwaitCancellation()`) and finishes on its own terms:

```cpp
std::string CancelChild(temporal::workflow::Context& ctx) {
  temporal::ChildWorkflowOptions o;
  auto child = ctx.ExecuteChildWorkflow<std::string>(o, "CancelAware");
  ctx.Sleep(std::chrono::seconds(1));   // let the child start first
  child.Cancel();
  return child.Get();                   // the child's result, e.g. "cancelled"
}
```

:::note Don't start and cancel a child in the same task
Temporal forbids starting a child workflow and requesting its cancellation within the *same*
workflow task. Force the cancel into a later task first — e.g. with a short `ctx.Sleep(...)` as
above — otherwise the server rejects the command.
:::

To control a still-running child when the parent *closes* (rather than mid-run), set
`ChildWorkflowOptions::parent_close_policy` (`Terminate` / `Abandon` / `RequestCancel`) — see
[child workflows](/workflows/composition).

## Cancelling and signalling external workflows

A workflow can reach an **unrelated** running workflow by id — fire-and-forget, no handle needed:

```cpp
std::string Canceller(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.CancelExternalWorkflow(target_id);   // request the other workflow's cancellation
  ctx.Sleep(std::chrono::seconds(3));      // stay alive so the request is delivered
  return "done";
}

std::string Notifier(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));  // encodes args
  ctx.Sleep(std::chrono::seconds(3));
  return "done";
}
```

`CancelExternalWorkflow(workflow_id)` and `SignalExternalWorkflow(workflow_id, signal_name, args...)`
are both deterministic commands. They are delivered asynchronously, so a workflow that does nothing
but cancel/signal another should stay alive briefly (as above) to ensure the request leaves before
it closes. The same calls target a known child by its id — see
[signals, queries & updates](/workflows/messaging).

:::note No structured cancellation scopes
There is **no** nesting/scope API (no equivalent of the Go SDK's `CancelChildContext` /
disconnected-context tree). Cancellation propagation is **explicit**: observe via `IsCancelled()` /
`AwaitCancellation()`, and cancel each operation's `Future` yourself (timer, activity, or child
workflow). This covers the same cases without an implicit scope graph — see the
[parity matrix](/parity).
:::
