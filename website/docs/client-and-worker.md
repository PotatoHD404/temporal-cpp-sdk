---
title: Client & worker
description: Connecting a client, starting workflows, and running workers.
---

# Client & worker

## Client

Connect to the Temporal frontend:

```cpp
temporal::ClientOptions opts;
opts.target = "localhost:7233";   // host:port
opts.ns = "default";              // Temporal namespace
auto client = temporal::client::Client::Connect(opts);
```

```cpp
struct ClientOptions {
  std::string target = "localhost:7233";
  std::string ns = "default";
  std::string identity;                            // default: "<pid>@<host>"
  std::shared_ptr<log::Logger> logger;             // default: console logger
  std::shared_ptr<DataConverter> data_converter;   // default: JSON converter
};
```

:::note
TLS/mTLS and API-key authentication are **not yet supported** — connections are insecure. See the
[parity matrix](/parity).
:::

### Starting workflows

```cpp
temporal::StartWorkflowOptions wf;
wf.id = "order-123";              // optional; default is a random UUID
wf.task_queue = "orders";        // required
wf.run_timeout = std::chrono::hours(1);   // optional

auto handle = client.StartWorkflow(wf, "OrderWorkflow", order_id, amount);
```

`StartWorkflow(options, workflow_type, args...)` returns a `WorkflowHandle`.

### Working with a handle

```cpp
handle.id();                                  // workflow id
handle.run_id();                              // run id

R result = handle.Result<R>();                // block until close; follows continue-as-new
R answer = handle.Query<R>("queryName", a);   // synchronous query
R out    = handle.Update<R>("updateName", a); // synchronous update

handle.Signal("signalName", payloads);        // fire-and-forget
handle.Cancel();                              // request cancellation
handle.Terminate("reason");                   // force-terminate
```

`Result<R>()` long-polls the workflow's history for the close event and throws
`temporal::WorkflowFailedError` if it failed, timed out, was terminated, or canceled. Get a handle to
an existing execution with `client.GetHandle(workflow_id, run_id)`.

## Worker

A worker polls a task queue and dispatches to your registered functions:

```cpp
temporal::worker::Worker worker(client, "orders");
worker.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
worker.RegisterActivity("ChargeCard", ChargeCard);

worker.Start();   // non-blocking: spawns poller threads
// ... run your program ...
worker.Stop();    // signal pollers to stop and join

// or, for a long-running worker process:
worker.Run();     // blocks until SIGINT/SIGTERM, then stops
```

Functions are registered by name and may be any plain callable
`R(workflow::Context&, Args...)` / `R(activity::Context&, Args...)` — arguments are decoded and the
result encoded automatically by the [data converter](/data-conversion).

### Sticky cache observability

The worker keeps running workflows resident (the [sticky cache](/architecture)). You can inspect how
many tasks were served as cache continuations vs. full replays:

```cpp
worker.cache_hits();   // continuation tasks served from the in-memory cache
worker.replays();      // full-history replays (first tasks + cache misses)
```

```cpp
struct WorkerOptions {
  int max_concurrent_activities = 0;      // 0 => library default
  int max_concurrent_workflow_tasks = 0;
  int workflow_task_pollers = 1;
  int activity_task_pollers = 1;
};
```
