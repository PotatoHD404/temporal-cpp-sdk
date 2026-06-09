---
title: Nexus operations
description: Cross-service operations — endpoint management, a worker handler, and a deterministic workflow call.
---

# Nexus operations

Nexus lets one workflow call an operation served by **another** worker — possibly
owned by a different team and running in a different namespace — without coupling
to that team's workflow types. The caller knows three strings (an endpoint, a
service, an operation) and the shape of the input/result; everything else lives
behind the endpoint. The call is durable and replay-safe, just like an activity.

The SDK supports the full round-trip:

1. A **worker** registers an operation handler.
2. A **client** registers the **endpoint** that routes to that worker's task queue.
3. A **workflow** calls the operation through the endpoint.

See [Concepts → Nexus](/concepts#nexus) for where this sits relative to activities
and child workflows, and [Client & worker](/client-and-worker) for the client and
worker setup used throughout the examples below.

## Endpoint vs service vs operation

- **Endpoint** — a server-side registration (created via the client) that names a
  worker **target**: the namespace + task queue whose worker handles incoming
  Nexus tasks. Callers address the endpoint by its unique **name**.
- **Service** — a namespace for related operations, chosen by the handler (a plain
  string, e.g. `"greeting"`).
- **Operation** — a single named operation within a service (e.g. `"say-hello"`),
  implemented by one handler function.

A caller routes by `(endpoint, service, operation)`; the endpoint decides *which
worker*, and the worker's registered `(service, operation)` decides *which handler*.

## Enabling Nexus on the dev server

Nexus is gated by a server feature flag. Start the dev server with it on:

```bash
temporal server start-dev --dynamic-config-value system.enableNexus=true
```

:::note
Without `system.enableNexus=true`, the endpoint-management RPCs are rejected and
Nexus operations are not dispatched. `CreateNexusEndpoint` (and friends) throw
`temporal::RpcError` in that case — handle it if you want to skip cleanly rather
than fail.
:::

## Endpoint management (client)

The client owns the endpoint registry (these are `OperatorService` RPCs — they
only register/describe/list endpoints; the actual operation call and handler live
on the workflow context and the worker respectively).

```cpp
// Register an endpoint named "my-endpoint" whose target is the worker polling
// "nexus-handler-tq" in this client's namespace. Returns the new endpoint id.
std::string endpoint_id =
    client.CreateNexusEndpoint("my-endpoint", "nexus-handler-tq");

// Describe a single endpoint by its server-assigned id.
temporal::client::NexusEndpointDescription d = client.GetNexusEndpoint(endpoint_id);
// d.id              — server-assigned, opaque
// d.name            — "my-endpoint"
// d.target_namespace
// d.target_task_queue == "nexus-handler-tq"

// Enumerate every registered endpoint by name (pages through results internally).
std::vector<std::string> names = client.ListNexusEndpoints();
```

`NexusEndpointDescription` is:

```cpp
struct NexusEndpointDescription {
  std::string id;                 // server-assigned endpoint id (opaque)
  std::string name;               // unique endpoint name
  std::string target_namespace;   // worker target: namespace that handles ops
  std::string target_task_queue;  // worker target: task queue that handles ops
};
```

:::note
Endpoint creation is eventually consistent: an endpoint may not appear in
`ListNexusEndpoints()` the instant `CreateNexusEndpoint` returns, even though
`GetNexusEndpoint(id)` already resolves it. Poll briefly if you list right after
creating.
:::

## Implementing a handler (worker)

Register a handler `R Fn(Arg)` for a `(service, operation)` pair on the worker
that should serve it. **Unlike an activity**, a Nexus operation takes a *single*
input value and returns a *single* value (each encoded to one `Payload`), and the
handler is a plain function — there is no `Context` parameter:

```cpp
// Handler: R Fn(Arg) — one input, one result.
std::string Greet(std::string name) { return "Hello, " + name; }

temporal::worker::Worker worker(client, "nexus-handler-tq");
worker.RegisterNexusOperation("greeting", "say-hello", Greet);
worker.Start();
```

The handler runs on whatever worker polls the endpoint's **target task queue**
(here `"nexus-handler-tq"`). It is dispatched synchronously and completes the
operation inline — return the result (or throw to fail the operation).

A `void`-returning handler is allowed (it produces an empty result):

```cpp
void Audit(std::string event) { /* side effect, no return value */ }
worker.RegisterNexusOperation("audit", "record", Audit);
```

:::note
The handler signature is checked at compile time: it must take **exactly one**
argument. `R Fn()` or `R Fn(A, B)` fails to compile.
:::

## Calling an operation (workflow)

From inside a workflow, call the operation through the **endpoint name**:

```cpp
template <class R, class Arg>
Future<R> Context::ExecuteNexusOperation(
    const std::string& endpoint, const std::string& service,
    const std::string& operation, const Arg& input,
    std::chrono::nanoseconds schedule_to_close = {});
```

`Arg` is deduced from `input`, so in practice you only spell `R`:

```cpp
std::string WorkflowUsingNexus(temporal::workflow::Context& ctx, std::string who) {
  return ctx.ExecuteNexusOperation<std::string>(
      "my-endpoint",   // endpoint NAME (not id)
      "greeting",      // service
      "say-hello",     // operation
      who              // single input value
  ).Get();             // block on the Future; or co_await in coroutine style
}
```

Pass an explicit `schedule_to_close` to bound the whole call; the default (`{}` /
`0`) uses the server default:

```cpp
using namespace std::chrono_literals;
auto result = ctx.ExecuteNexusOperation<std::string>(
    "my-endpoint", "greeting", "say-hello", who, /*schedule_to_close=*/30s).Get();
```

It returns a `Future<R>`. As with activities and child workflows, you can keep the
`Future` and `.Get()` it later (or `co_await` it in a coroutine workflow) to run
work concurrently in between.

### Determinism

`ExecuteNexusOperation` is a recorded workflow **command**, not a side effect. The
schedule is written to history (`NexusOperationScheduled`) and the result is read
back from history (`NexusOperationCompleted`) on replay — the handler does **not**
re-run during replay, exactly like an activity result. The call is therefore
replay-safe; you may call it directly from workflow code.

## End-to-end example

A worker that both **serves** the operation and **runs the caller workflow** (the
endpoint targets that same task queue), then a client that creates the endpoint and
starts the workflow. This mirrors the integration test in
`tests/integration/nexus_operation_edge_test.cpp`.

```cpp
#include <string>
#include <temporal/temporal.h>

// 1. The Nexus operation handler: R Fn(Arg) — one input, one result, no Context.
std::string GreetOp(std::string name) { return "hello " + name; }

// 2. The caller workflow: invokes the operation on `endpoint` and returns its
//    result. Passing the endpoint name as input lets the caller pick it at runtime.
std::string CallerWorkflow(temporal::workflow::Context& ctx, std::string endpoint) {
  return ctx.ExecuteNexusOperation<std::string>(
      endpoint, "svc", "op", std::string("world")).Get();
}

int main() {
  temporal::ClientOptions opts;
  opts.target = "localhost:7233";
  opts.ns = "default";
  auto client = temporal::client::Client::Connect(opts);

  const std::string task_queue = "nexus-handler-tq";
  const std::string endpoint = "demo-endpoint";

  // 3. Create the endpoint targeting the worker's task queue. Requires Nexus to be
  //    enabled on the server, otherwise this throws RpcError.
  client.CreateNexusEndpoint(endpoint, task_queue);

  // 4. One worker serves the operation AND runs the caller workflow.
  temporal::worker::Worker worker(client, task_queue);
  worker.RegisterWorkflow("CallerWorkflow", CallerWorkflow);
  worker.RegisterNexusOperation("svc", "op", GreetOp);
  worker.Start();  // non-blocking

  // 5. Start the workflow; the result flows back from the handler.
  temporal::StartWorkflowOptions wo;
  wo.task_queue = task_queue;
  auto handle = client.StartWorkflow(wo, "CallerWorkflow", endpoint);
  std::string result = handle.Result<std::string>();  // "hello world"

  worker.Stop();
  return 0;
}
```

In a real cross-team deployment the handler worker, the endpoint registration, and
the caller workflow live in **separate** processes (and often separate
namespaces); the endpoint's `target_task_queue` is the only thing that ties the
caller to the handler.

## Limits

What the SDK implements today, stated honestly:

- **Synchronous start-operation only.** The handler runs inline and completes the
  operation immediately; there is no async / long-running Nexus operation model
  (no operation token, no separate completion callback).
- **Single input, single result.** A handler is `R Fn(Arg)` — exactly one argument
  and one return value, each one `Payload`. This is narrower than an activity
  (which takes multiple args).
- **No handler `Context`.** The handler is a plain function; there is no Nexus
  operation context to read metadata, headers, or a cancellation signal from.
- Endpoint management covers create / get (describe) / list. There is no
  update-endpoint or delete-endpoint call in this SDK.

See the [parity matrix](/parity) for how this row compares to the official SDKs.
