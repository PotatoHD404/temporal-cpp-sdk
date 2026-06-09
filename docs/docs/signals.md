---
title: Signals
description: Asynchronous, durable messages delivered into a running workflow.
---

# Signals

A **signal** is an asynchronous, durable message delivered into a *running* workflow. Sending one is
fire-and-forget: the caller doesn't wait for the workflow to react and gets no return value. The
delivery is recorded in the workflow's history, so it is **at-least-once** and survives worker
restarts — the signal is re-delivered on replay exactly where it landed the first time.

Signals are one of three ways to interact with a running workflow:

| Mechanism | What it does |
|-----------|--------------|
| **Signal** | fire-and-forget message *into* the workflow; may mutate state; returns nothing. |
| **Query**  | read-only request *out of* the workflow; returns a value synchronously. |
| **Update** | request/response that *may mutate* state and returns a value synchronously. |

Queries and updates are covered on the [signals, queries & updates](/workflows/messaging) page; this
page is the full reference for signals. For the underlying model — history, replay, the sticky cache —
see [concepts](/concepts).

## Receiving signals in a workflow

A workflow reads signals from a **channel** keyed by name. Get one with `GetSignalChannel<T>` and call
`Receive()` — it blocks (parks the workflow) until a signal is buffered, then decodes the payload to
`T` through the [data converter](/data-conversion):

```cpp
std::string GreetBySignal(temporal::workflow::Context& ctx) {
  auto channel = ctx.GetSignalChannel<std::string>("setName");
  std::string name = channel.Receive();   // blocks (parks) until a signal arrives
  return "Hello, " + name;
}
```

Parking is free: a parked workflow consumes no worker thread and is evicted from the sticky cache as
usual; when the signal arrives the workflow resumes deterministically from history.

### Non-blocking receive

`ReceiveAsync()` drains the next buffered signal without blocking, returning `std::optional<T>` that is
`std::nullopt` when nothing is queued:

```cpp
auto channel = ctx.GetSignalChannel<int>("add");
while (auto v = channel.ReceiveAsync()) {   // drain everything buffered right now
  total += *v;
}
```

There is also a bool + out-param form, `bool ReceiveAsync(T& out)`, kept for source compatibility.

### Buffering & ordering guarantees

Signals are **buffered and ordered**. Several can pile up before the workflow gets around to reading
them, and the Nth `Receive()` always returns the Nth signal sent to that name. Because the buffer is
reconstructed deterministically from history on every replay, ordering is stable across worker
restarts and re-runs.

:::note Drain before completing
A workflow function that returns while signals are still buffered discards them — they are *not*
re-delivered to a future run. If completion races with in-flight signals, drain the channel with
`ReceiveAsync()` (or a sentinel signal, below) before returning.
:::

## Sending a signal from a client

From outside the workflow, get a `WorkflowHandle` (from `StartWorkflow` or `Client::GetHandle`) and
call `Signal`. The variadic overload encodes the argument(s) for you through the data converter:

```cpp
handle.Signal("setName", std::string("World"));   // encodes via the data converter
```

This is fire-and-forget: it returns as soon as the server records the signal, not when the workflow
processes it. Signaling a workflow id the server doesn't know throws `temporal::NotFoundError` (a
subclass of `RpcError`).

If you already hold encoded `Payloads`, the pre-encoded overload `Signal(name, payloads)` sends them
as-is:

```cpp
const auto dc = temporal::DataConverter::Default();
handle.Signal("add", dc->ToPayloads(5));   // pre-encoded payloads
```

## Typed signal handles

Restating a wire name as a string plus an explicit `<T>` at every call site is error-prone. Declare a
`temporal::SignalRef<T>` once at namespace scope — it binds the name and the payload type together —
and both the receiving channel's `T` and the sent value are checked and deduced from it:

```cpp
inline constexpr temporal::SignalRef<bool> kStop{"stop"};
```

Inside the workflow, the ref replaces the name *and* the channel's `<T>`:

```cpp
ctx.GetSignalChannel(kStop).Receive();   // ReceiveChannel<bool>, deduced
```

And from the client, the value type is checked against the ref:

```cpp
handle.Signal(kStop, true);   // bool checked against SignalRef<bool>
```

Typed handles lower to the same string names, so they interoperate with the string-based API above and
replay identically.

## Signal-with-start

`Client::SignalWithStartWorkflow` atomically *starts the workflow if it isn't already running*, then
delivers a signal to it — in a single call. If the workflow id is already running, it is only
signaled. This is the canonical way to drive an "entity" workflow whose first interaction may also be
its creation.

The signal argument is passed pre-encoded as `Payloads`; any remaining arguments are the workflow's
start arguments:

```cpp
const auto dc = temporal::DataConverter::Default();
temporal::StartWorkflowOptions o;
o.task_queue = "greeter";
o.workflow_id = "greeter-42";   // stable id makes start-or-signal idempotent

auto handle = client.SignalWithStartWorkflow(
    o, "GreetBySignalWorkflow",        // workflow type
    "setName", dc->ToPayloads(std::string("Ada")));   // signal name + pre-encoded arg
// handle now refers to the running workflow, which already has the "setName" signal buffered.
```

## Signaling another workflow from a workflow

A running workflow can signal *another* running workflow by id with `SignalExternalWorkflow` — also
fire-and-forget. It encodes its arguments through the data converter, just like the client's variadic
`Signal`. This works for any workflow id, including a child the current workflow started:

```cpp
std::string SignalExternalWf(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));   // encodes args
  ctx.Sleep(std::chrono::seconds(3));   // stay alive so the request leaves before we close
  return "done";
}
```

:::note Deliver before completing
`SignalExternalWorkflow` is a deterministic command delivered asynchronously. A workflow that does
nothing but signal another and then returns immediately can close before the request leaves — keep it
alive briefly (a short `Sleep`, as above, or other pending work). See also
[cancellation](/cancellation) for the matching `CancelExternalWorkflow`.
:::

## Racing a signal against a timer or cancel

To wait for a signal *but not forever*, race the channel against a timer (or against
`AwaitCancellation()`) with a [`Selector`](/cancellation). `AddReceive` adds a channel case that
becomes ready when a signal is buffered; `Select()` runs whichever case fires first:

```cpp
std::string WaitForGoOrTimeout(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("go");
  auto timeout = ctx.NewTimer(std::chrono::seconds(30));
  std::string out;

  temporal::workflow::Selector sel(ctx);
  sel.AddReceive<std::string>(signals, [&](std::string s) { out = "signal:" + s; });
  sel.AddFuture(timeout, [&]() { out = "timeout"; });
  sel.Select();   // blocks until the signal arrives OR the timer fires

  return out;
}
```

Add `sel.AddDefault([&]{ ... })` to make the select non-blocking (run the default when nothing is
ready yet).

## A complete example

A workflow that loops receiving `"input"` signals and accumulates them until a `"done"` sentinel
signal arrives, plus a client that sends three signals and reads the result:

```cpp
#include <temporal/temporal.h>
#include <iostream>
#include <string>

// Counts "input" signals until a "done" signal arrives, then completes with the count.
int CountSignalsWorkflow(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("input");
  int count = 0;
  while (signals.Receive() != "done") {   // blocks per iteration; "done" is the sentinel
    ++count;
  }
  return count;
}

int main() {
  auto client = temporal::client::Client::Connect({.target = "localhost:7233"});

  temporal::worker::Worker worker(client, "signals");
  worker.RegisterWorkflow("CountSignalsWorkflow", CountSignalsWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions opts;
  opts.task_queue = "signals";
  auto handle = client.StartWorkflow(opts, "CountSignalsWorkflow");

  handle.Signal("input", std::string("a"));      // variadic: encodes for us
  handle.Signal("input", std::string("b"));
  handle.Signal("input", std::string("done"));   // sentinel completes the loop

  std::cout << handle.Result<int>() << "\n";      // -> 2
  worker.Stop();
}
```

An integer channel uses the same shape with a numeric sentinel, e.g. a negative value to finish:

```cpp
int Sum(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<int>("add");
  int sum = 0;
  while (true) {
    const int v = signals.Receive();
    if (v < 0) return sum;   // negative sentinel completes the workflow
    sum += v;
  }
}
```

## Common patterns & gotchas

- **Use a sentinel to finish a loop.** A long-lived workflow that loops on `Receive()` needs an
  explicit way to stop — a dedicated `"done"`/`"stop"` signal or an out-of-band value (a negative
  number, an empty string). Without one it parks forever.
- **Drain buffered signals before completing.** Returning while signals are still queued silently
  drops them; they are not re-delivered to a later run. `while (auto v = ch.ReceiveAsync()) { ... }`
  drains everything buffered right now.
- **The caller must stay alive long enough to deliver.** A workflow whose only job is to
  `SignalExternalWorkflow` another and return can close before the command is sent. Keep it alive
  briefly (a short `Sleep` or other pending work). Client-side, a process that signals and exits
  immediately is fine — the signal is durable once the `Signal` call returns.
- **Signals are at-least-once and unordered *across names*.** Ordering is guaranteed only *within* a
  single signal name. Don't assume a signal on channel `"a"` is observed before one on channel `"b"`.
- **Signals can't return a value.** If the sender needs a response, use an
  [update](/workflows/messaging) instead — it's a synchronous request/response that may also mutate
  state.
- **Querying right after signaling is eventually consistent.** A signal mutates state asynchronously,
  so a [query](/workflows/messaging) issued immediately afterward may not yet reflect it — poll if you
  need to observe a specific write.
