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

- `Receive()` blocks until a signal is buffered; `ReceiveAsync(out)` is the non-blocking variant.
- Signals are **buffered and ordered**: the Nth `Receive()` returns the Nth signal sent to that name.

Send one from the client:

```cpp
auto dc = temporal::DataConverter::Default();
handle.Signal("setName", dc->ToPayloads(std::string("World")));
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

State accumulates across updates because the workflow stays resident in the sticky cache.

:::note Update limitations
Update **validators** (a pre-acceptance validation phase that can reject) and **replay
re-application** of accepted updates after a cache eviction are not yet implemented. In practice the
sticky cache keeps a running workflow resident, so updates apply on the live path. See the
[parity matrix](/parity).
:::
