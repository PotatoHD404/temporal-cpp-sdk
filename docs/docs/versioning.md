---
title: Versioning & patching
description: Safely evolve running workflow code with GetVersion, capture non-deterministic values with SideEffect/MutableSideEffect, and catch breakage with the offline replayer.
---

# Versioning & patching

A workflow is a deterministic program whose execution is reconstructed by
**replaying its history**. When a worker picks up a workflow task it does not
resume from a saved snapshot of your C++ objects — it re-runs your workflow
function from the top, feeding it the recorded events, and matches the commands
it emits against the events already in history (see
[Non-determinism detection](advanced.md#non-determinism-detection)). This is what
makes a workflow durable, and it is also what makes *changing the code* of a
**running** workflow dangerous.

This page is the authoritative guide to evolving workflow code without breaking
in-flight executions.

## Why versioning {#why}

Suppose v1 of a workflow runs activity `A` then activity `B`:

```cpp
int OrderWorkflow(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);
  const int a = ctx.ExecuteActivity<int>(o, "A").Get();
  const int b = ctx.ExecuteActivity<int>(o, "B", a).Get();
  return b;
}
```

A workflow starts, runs `A`, records `ScheduleActivityTask{A}` /
`ActivityTaskCompleted{A}` in its history, and then parks (waiting on `B`, a
timer, a signal — anything that spans more than one workflow task). While it is
parked you deploy a new worker where someone "improved" the code to call `C`
before `B`:

```cpp
const int a = ctx.ExecuteActivity<int>(o, "A").Get();
const int c = ctx.ExecuteActivity<int>(o, "C").Get();   // NEW first step
const int b = ctx.ExecuteActivity<int>(o, "B", a + c).Get();
```

When the parked execution gets its next workflow task, the new worker replays
from the top. History's first command-generating event is `ScheduleActivityTask{A}`
and the code still emits `A` — fine. But history's *second* such event is
`ScheduleActivityTask{B}`, while the new code now emits `C`. The replayed command
stream diverges from history: **non-determinism**. The task fails per
[`WorkflowPanicPolicy`](#panic-policy), and depending on the policy the workflow
either gets stuck (retried forever against still-broken code) or fails outright.

The fix is never to mutate the path an existing history already took. Instead you
**gate the new behavior behind a version check** so old histories keep taking the
old path and only new executions take the new one. That is what `GetVersion` is
for.

## `GetVersion`: marker-based patching {#getversion}

```cpp
int GetVersion(const std::string& change_id, int min_supported, int max_supported);
```

`ctx.GetVersion` returns a version number for the named change. The **first** time
a workflow reaches the call it picks `max_supported`, records it to history as a
marker, and returns it. On every **replay** it reads the recorded number back
from that marker and returns *that* — so an execution that first ran at version 1
always sees version 1, even after you bump `max_supported` to 2 in the code.

The one special case is history recorded *before* the `GetVersion` call existed:
there is no marker to read, so `GetVersion` returns
`temporal::workflow::kDefaultVersion` (which is `-1`). Passing `kDefaultVersion`
as `min_supported` means "I still support the un-versioned, pre-patch branch."

```cpp
inline constexpr int kDefaultVersion = -1;  // temporal::workflow::kDefaultVersion
```

### Add a step

Introduce `C` between `A` and `B` for new executions only, while parked v1
histories keep going straight from `A` to `B`:

```cpp
int OrderWorkflow(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);

  const int a = ctx.ExecuteActivity<int>(o, "A").Get();

  int extra = 0;
  const int v = ctx.GetVersion("add-step-C", temporal::workflow::kDefaultVersion, 1);
  if (v >= 1) {
    extra = ctx.ExecuteActivity<int>(o, "C").Get();  // only new executions run C
  }
  // pre-patch executions: v == kDefaultVersion, extra stays 0, history unchanged

  return ctx.ExecuteActivity<int>(o, "B", a + extra).Get();
}
```

An execution started before this deploy replays with `v == kDefaultVersion`,
skips `C`, and reproduces its original `A`→`B` command stream — deterministic. An
execution started after the deploy records `add-step-C → 1` and runs `C`.

### Change a step

Replacing one activity with another is the same shape — branch on the version
instead of editing in place:

```cpp
const int v = ctx.GetVersion("swap-B-for-B2", temporal::workflow::kDefaultVersion, 1);
if (v == temporal::workflow::kDefaultVersion) {
  result = ctx.ExecuteActivity<int>(o, "B", a).Get();    // old executions
} else {
  result = ctx.ExecuteActivity<int>(o, "B2", a).Get();   // new executions
}
```

This is exactly the edit that, done *without* the guard, the replayer flags as
non-determinism (the test suite's `ReplayFwV1` → `ReplayFwV2` swaps `AddOne` for
`Multiply` and the offline replayer catches it — see [Catching breakage](#catching-breakage)).

### Make several incompatible changes over time

`max_supported` grows as you keep changing the same region. Each value maps to one
era of the code; old histories pin to their recorded number:

```cpp
const int v = ctx.GetVersion("pricing", temporal::workflow::kDefaultVersion, 2);
if (v == temporal::workflow::kDefaultVersion) {
  price = legacy_price(ctx);          // pre-patch executions
} else if (v == 1) {
  price = ctx.ExecuteActivity<int>(o, "PriceV1").Get();
} else {
  price = ctx.ExecuteActivity<int>(o, "PriceV2").Get();  // == 2
}
```

### Retire an old version

Once **no running execution** can still be on an old branch — every workflow that
recorded the lower version (or the un-versioned marker) has closed — you can prune
that branch and raise `min_supported`. Use [visibility queries](advanced.md#visibility-sessions)
(`client.CountWorkflows("WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running'")`)
or your own bookkeeping to be sure none remain.

Dropping the `kDefaultVersion` branch first:

```cpp
// Was: GetVersion("pricing", kDefaultVersion, 2). No pre-patch executions remain,
// so version 1 is now the floor. min_supported == max_supported pins everyone to 2.
const int v = ctx.GetVersion("pricing", 1, 2);
if (v == 1) {
  price = ctx.ExecuteActivity<int>(o, "PriceV1").Get();
} else {
  price = ctx.ExecuteActivity<int>(o, "PriceV2").Get();
}
```

When `min_supported == max_supported`, every execution (new and replayed)
resolves to that single number, and you can eventually delete the call entirely —
but only after no history references the change id at all.

:::warning Never change the arguments of an existing `GetVersion` call carelessly.
Lowering `min_supported` past a version that some history recorded, or removing a
branch that a running execution still replays through, reintroduces exactly the
non-determinism `GetVersion` was added to prevent. Retire from the bottom up, and
only when running counts are zero.
:::

## Non-deterministic values {#non-deterministic-values}

Versioning protects the *shape* of a workflow's command stream. The other source
of non-determinism is reading a value that differs between the original run and a
replay — the wall clock, a random number, a freshly generated UUID. Calling
`std::chrono::system_clock::now()`, `rand()`, or a UUID library directly in a
workflow is a determinism bug: the replay computes a different value than history
implies.

Capture such values through the context so they are recorded once and replayed
verbatim. Both are covered in depth under
[Advanced capabilities](advanced.md#sideeffect); in brief:

- **`ctx.SideEffect<T>(fn)`** runs `fn` exactly once, records its result to
  history, and returns the recorded value on every replay:

  ```cpp
  std::string id = ctx.SideEffect<std::string>([] { return make_uuid(); });
  ```

- **`ctx.MutableSideEffect(id, fn[, equals])`** is keyed by `id` and only writes a
  new marker when the value *changes* (by `operator==`, or a custom predicate);
  the result type `R` is **deduced from `fn`**, so you do not pass it explicitly:

  ```cpp
  // Records a marker only when the tier actually differs from the last call.
  std::string tier = ctx.MutableSideEffect("tier", [&] { return compute_tier(state); });
  ```

Neither replaces an [activity](concepts.md#activities): a side effect must be a
pure local computation with no externally visible result. Anything that touches
the outside world belongs in an activity, whose result is durably recorded the
same way.

## Catching breakage {#catching-breakage}

The whole point of this page is to make changes *before* a running workflow hits
them. The tool for that is the offline replayer:

```cpp
void Worker::ReplayWorkflowHistory(const std::string& history_json);
```

It replays a recorded history against your **current** workflow code without
contacting a server, and throws if the replayed commands diverge from history.
Export a real history with `WorkflowHandle::FetchHistoryJson()` (or
`temporal workflow show -o json`), keep it as a fixture, and replay it:

```cpp
// 1) Capture a representative history once (e.g. from a workflow you just ran):
std::string history_json = handle.FetchHistoryJson();

// 2) In a unit test, replay it against the code you are about to ship:
temporal::worker::Worker replayer(client, "replay");  // never Start()ed — no RPCs
replayer.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
replayer.ReplayWorkflowHistory(history_json);  // throws if OrderWorkflow diverged
```

A compatible edit (anything correctly gated behind `GetVersion`) replays clean; an
incompatible one (a reordered activity, a swapped activity type, a removed timer)
throws. Wire a fixture replay into CI so an unsafe change fails the build instead
of a production workflow — see [Testing → CI](testing.md#ci) and
[Replaying recorded histories](testing.md#replaying-recorded-histories).

### `WorkflowPanicPolicy` {#panic-policy}

When non-determinism *does* reach a running worker, what happens is governed by
`WorkerOptions::panic_policy` (full treatment in
[Non-determinism detection](advanced.md#non-determinism-detection)):

```cpp
temporal::WorkerOptions opts;
opts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;  // default
temporal::worker::Worker worker(client, "my-task-queue", opts);
```

- **`BlockWorkflow`** (default) — fail the workflow *task*. The server retries it,
  so rolling back to a compatible worker recovers the execution with no data loss.
  This is what makes a bad deploy survivable: fix the code (or revert), redeploy,
  the task succeeds.
- **`FailWorkflow`** — fail the workflow *execution* outright. Terminal.

Prefer the default in production: a non-deterministic deploy parks the affected
workflows instead of killing them, buying you time to ship a fix.

## Worker Build-ID / versioning {#worker-versioning}

`GetVersion` is *code-level* versioning — it lives inside one workflow function.
At the **deployment** level, Temporal can also route tasks by **worker build id**
so that a workflow stays pinned to the worker version that started it, letting you
roll out a new binary without `GetVersion` guards. The SDK exposes the client RPCs
for this; the actual pinning is enforced **server-side** and the relevant APIs
generally require dynamic config enabled on the server, so treat the methods below
as the control surface, not a turnkey feature.

The build-id **compatibility-set** API (`Client`):

```cpp
// Add `build_id` as a brand-new default set on the task queue.
client.UpdateWorkerBuildIdCompatibility("my-task-queue", "v2");
// Promote an existing set (the one containing `build_id`) to be the default.
client.PromoteWorkerBuildIdSet("my-task-queue", "v1");
// Read the sets back; each inner vector is one compatible set, last id = default.
std::vector<std::vector<std::string>> sets =
    client.GetWorkerBuildIdCompatibility("my-task-queue");
```

The newer **rules-based** API (assignment + redirect rules — distinct from the
sets above):

```cpp
// Route new executions to a build id (assignment rule at the head of the list).
client.InsertWorkerAssignmentRule("my-task-queue", "v2");
// Redirect an old build id to a newer one (gradual rollout).
client.AddWorkerRedirectRule("my-task-queue", /*source=*/"v1", /*target=*/"v2");
// Read the current rules + the conflict token for the next update.
temporal::client::WorkerVersioningRules rules =
    client.GetWorkerVersioningRules("my-task-queue");
```

And the modern **Worker Deployments** API:

```cpp
std::vector<std::string> deployments = client.ListWorkerDeployments();
temporal::client::WorkerDeploymentDescription d =
    client.DescribeWorkerDeployment("my-deployment");
// Promote a build id to current (optimistic concurrency via the conflict token):
std::string token = client.SetWorkerDeploymentCurrentVersion(
    "my-deployment", "v2", d.conflict_token);
```

:::note These RPCs need server-side configuration to do anything useful. The
build-id and rules APIs require the worker-versioning dynamic config (e.g.
`frontend.workerVersioningRuleAPIs=true`), and the deployment APIs require
`system.enableDeploymentVersions=true`. On a stock dev server some are no-ops or
throw. When in doubt, reach for `GetVersion` — it works everywhere with no server
config.
:::

## Deploying safely {#deploying}

How a change reaches running workflows in practice:

- **Sticky execution.** A worker keeps a hot, in-memory ("sticky") copy of a
  workflow's state and continues it from there for subsequent tasks — no replay.
  A full replay (and thus your new code) only happens when that cache is gone:
  the worker restarted, was redeployed, the entry was evicted, or the task was
  routed to a *different* worker. You can watch the split with `worker.cache_hits()`
  vs. `worker.replays()`. The implication: during a rolling deploy, **old and new
  workers run side by side**, and a given workflow may be continued by an old
  worker on one task and replayed from scratch by a new one on the next. Both must
  agree on the command stream — which is precisely why guarded changes are safe and
  unguarded ones are not.

- **Run old and new code together.** Because of the above, assume any deploy is a
  mixed fleet for its duration. A `GetVersion`-guarded change is correct under
  mixing: old executions resolve to the old version on either worker, new ones to
  the new version. An unguarded incompatible change is *not* — it only "works" on
  the workers whose code happens to match each history.

### Safe-change checklist

1. **Adding** an activity, timer, child workflow, or any new command to an
   *existing* code path → gate it behind `ctx.GetVersion`.
2. **Removing or reordering** an existing command on a path old histories took →
   gate the old vs. new path behind `ctx.GetVersion`; never edit in place.
3. **Renaming** a workflow or activity *type string* is an incompatible change —
   treat it as "change a step."
4. Reading the **clock, randomness, or a UUID** directly → wrap in
   `ctx.SideEffect` / `ctx.MutableSideEffect`.
5. Changing a workflow's **signal/query/update handler set or argument types** can
   strand in-flight messages — version the handler behavior the same way.
6. **Before deploying:** replay representative fixture histories with
   `Worker::ReplayWorkflowHistory` in CI ([testing.md#ci](testing.md#ci)).
7. **Keep `panic_policy` at `BlockWorkflow`** in production so a missed case parks
   rather than kills affected executions.
8. **Retire a version only** once `CountWorkflows(... ExecutionStatus = 'Running')`
   confirms no execution can still be on the old branch, then raise `min_supported`
   from the bottom up.

Changes that are *always* safe (no versioning needed): editing activity **bodies**
(activities are not replayed — only their recorded results are), adjusting
`ActivityOptions`/timeouts/retry policy, and any change to non-workflow code.
