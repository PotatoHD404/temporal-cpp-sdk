---
title: Signals, queries & updates
description: Communicating with a running workflow.
---

# Signals, queries & updates

Three ways to interact with a *running* workflow, from a client or another workflow.

| Mechanism | Direction | Can mutate state? | Returns a value? |
|-----------|-----------|-------------------|------------------|
| **Signal** | fire-and-forget into the workflow | yes | no |
| **Query**  | read-only request | no | yes (synchronous) |
| **Update** | request + response | yes | yes (synchronous) |

## Signals

A signal delivers data into a running workflow asynchronously. The workflow reads them from a
**channel**:

```cpp
std::string GreetBySignal(temporal::workflow::Context& ctx) {
  auto channel = ctx.GetSignalChannel<std::string>("setName");
  std::string name = channel.Receive();   // blocks (parks) until a signal arrives
  return "Hello, " + name;
}
```

- `Receive()` blocks until a signal is buffered; `ReceiveAsync()` returns `std::optional<T>` (or use
  the `ReceiveAsync(out)` bool form) for the non-blocking variant.
- Signals are **buffered and ordered**: the Nth `Receive()` returns the Nth signal sent to that name.

Send one from the client — the variadic overload encodes the argument(s) for you:

```cpp
handle.Signal("setName", std::string("World"));   // encodes via the data converter
```

A common pattern is a loop that accumulates signals:

```cpp
int Sum(temporal::workflow::Context& ctx) {
  auto channel = ctx.GetSignalChannel<int>("add");
  int total = 0;
  while (true) {
    int n = channel.Receive();
    if (n < 0) return total;   // sentinel to finish
    total += n;
  }
}
```

## Queries

A query reads a workflow's current state without changing it. Register a handler in the workflow,
then call it from the client:

```cpp
int Counter(temporal::workflow::Context& ctx) {
  int count = 0;
  ctx.SetQueryHandler("getCount", [&] { return count; });   // read-only
  auto channel = ctx.GetSignalChannel<int>("add");
  while (true) count += channel.Receive();
}
```

```cpp
int n = handle.Query<int>("getCount");
```

- Query handlers must be **read-only** (no activities, timers, or state changes).
- They run against the live, suspended workflow state — which is why this SDK keeps workflows
  resident in the [sticky cache](/architecture).
- Queries are read-after-write *eventually* consistent vs. just-sent signals; poll if you need to
  observe a specific write.

## Updates

An update is a synchronous request/response that **may mutate** workflow state — like a query that's
also recorded in history. Register a handler and call it from the client:

```cpp
int Account(temporal::workflow::Context& ctx) {
  int balance = 0;
  ctx.SetUpdateHandler("deposit", [&](int amount) {
    balance += amount;
    return balance;          // returned to the caller
  });
  ctx.GetSignalChannel<std::string>("close").Receive();   // keep running
  return balance;
}
```

```cpp
int new_balance = handle.Update<int>("deposit", 100);   // blocks until applied; returns 100
int again       = handle.Update<int>("deposit", 50);    // returns 150
```

State accumulates across updates because the workflow stays resident in the sticky cache, and
accepted updates are re-applied if the workflow later replays from history after a cache eviction.

### Update validators

Pass a third argument to `SetUpdateHandler` to register a **read-only validator**. It takes the same
arguments as the handler and runs first; if it throws, the update is rejected before acceptance —
nothing is written to history and the handler never runs:

```cpp
ctx.SetUpdateHandler(
    "deposit",
    [&](int amount) { balance += amount; return balance; },
    [](int amount) {
      if (amount <= 0) {
        throw temporal::ApplicationError("must be positive", "InvalidUpdate");
      }
    });
```

Validators must not mutate state or schedule activities/timers.

## Typed handles

Restating a wire name as a string plus an explicit template argument at every call site is
error-prone. Declare a typed handle once at namespace scope and the channel type, sent value, and
result type are all checked and deduced:

```cpp
inline constexpr temporal::SignalRef<bool> kStop{"stop"};
inline constexpr temporal::QueryRef<int>   kSum{"sum"};
inline constexpr temporal::UpdateRef<int>  kBump{"bump"};
```

Inside the workflow, the handle replaces the name (and the channel's `<T>`):

```cpp
ctx.SetQueryHandler(kSum, [&] { return sum; });
ctx.SetUpdateHandler(kBump, [&](int by) { sum += by; return sum; });
ctx.GetSignalChannel(kStop).Receive();   // ReceiveChannel<bool>, deduced
```

And from the client, the value and result types are checked against the ref:

```cpp
int after = h.Update(kBump, 5);   // arg + result int deduced
int total = h.Query(kSum);        // result int deduced
h.Signal(kStop, true);            // bool value checked against SignalRef<bool>
```

Typed handles lower to the same string names, so they interoperate with the string-based API above.

## Signaling another workflow

A workflow can signal or cancel *another* running workflow by id (fire-and-forget) — including a
child it started, by passing the child's workflow id:

```cpp
ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));
ctx.CancelExternalWorkflow(target_id);
```

`SignalExternalWorkflow` encodes its arguments through the data converter, like the client's variadic
`Signal`.

:::note Unknown ids
Client-side calls against a workflow id the server doesn't know throw `temporal::NotFoundError` (a
subclass of `RpcError`, so existing `catch (RpcError&)` sites still catch it).
:::
