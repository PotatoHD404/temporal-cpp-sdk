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
  TlsConfig tls;                                   // disabled by default (insecure channel)
  std::string api_key;                             // sent as "Authorization: Bearer <key>" per RPC
  std::vector<std::shared_ptr<interceptor::Interceptor>> interceptors;  // client-outbound chain
};
```

For a secure connection, populate `tls` (PEM *contents*, not file paths — set
`client_cert` + `client_key` too for mutual TLS) and/or `api_key`:

```cpp
opts.tls.enabled = true;
opts.tls.server_ca_cert = ca_pem;   // empty => system trust store
opts.api_key = std::getenv("TEMPORAL_API_KEY");
```

:::note
TLS/mTLS + API-key auth are wired (gRPC `SslCredentials` and a `Bearer` metadata
header) and unit-verified against an in-process TLS server, but **not yet
exercised against a real Temporal server**. See the [parity matrix](/parity).
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
R answer = handle.Query<R>("queryName", a);   // synchronous query (args encoded for you)
R out    = handle.Update<R>("updateName", a); // synchronous update

handle.Signal("signalName", a, b);            // fire-and-forget; args encoded variadically
handle.Cancel();                              // request cancellation
handle.Terminate("reason");                   // force-terminate
```

`Result<R>()` long-polls the workflow's history for the close event and throws
`temporal::WorkflowFailedError` if it failed, timed out, was terminated, or canceled. Get a handle to
an existing execution with `client.GetHandle(workflow_id, run_id)`. A call that
targets an unknown workflow id throws `temporal::NotFoundError` (a subtype of
`RpcError` whose `not_found()` is `true`).

### Typed signal / query / update handles

Declare a typed reference once and the wire name, payload type, and result type
are all checked and deduced — no restated string or explicit `<R>`:

```cpp
inline constexpr temporal::SignalRef<bool> kStop{"stop"};
inline constexpr temporal::QueryRef<int>   kSum{"sum"};
inline constexpr temporal::UpdateRef<int>  kBump{"bump"};

int sum   = handle.Query(kSum);        // result type int deduced
int after = handle.Update(kBump, 5);   // arg + result type checked
handle.Signal(kStop, true);            // payload type bool checked
```

These lower to the same string names as the untyped calls, so the two forms
interoperate and replay identically.

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

For activities you can also register through a typed handle so the type name
can't drift from the call site (`TEMPORAL_ACTIVITY(fn)` declares `fn_activity`):

```cpp
TEMPORAL_ACTIVITY(ChargeCard);          // at namespace scope, next to the function
// ...
worker.Register(ChargeCard_activity);   // name comes from the handle
```

Register a Nexus operation handler `R Fn(Arg)` for a `(service, operation)` pair
(a Nexus operation takes a single input and returns a single value):

```cpp
worker.RegisterNexusOperation("payments", "charge", ChargeOperation);
```

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
  // If > 0, a workflow task that overruns this deadline (a blocking call or
  // non-yielding loop in workflow code) is ABORTED: the task is failed so the
  // server retries it, and the worker keeps serving other workflows instead of
  // hanging on the stuck task.
  std::chrono::milliseconds deadlock_detection_timeout{0};
  // ... plus concurrency caps, panic policy, sticky-cache bound, metrics,
  // interceptors, poller autoscaling, sessions — see <temporal/common/options.h>.
};
```
