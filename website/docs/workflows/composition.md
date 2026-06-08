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

- `AddFuture<T>(future, handler)` registers a value future; `AddFuture(Future<void>, handler)` a
  timer.
- `AddDefault(handler)` makes `Select()` non-blocking (runs the default if nothing is ready).
- `Select()` parks the workflow (via the coroutine) until a case becomes ready.

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

The workflow observes it and decides how to react:

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

:::note
Cancellation is currently **observe-only** via `IsCancelled()`. Automatic cancellation propagation to
in-flight activities/timers and structured cancellation scopes are not yet implemented — see the
[parity matrix](/parity).
:::
