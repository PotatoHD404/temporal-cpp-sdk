---
title: Schedules
description: Server-side recurring workflow starts — create an interval or cron schedule, then describe, update, pause/unpause, trigger, list, and delete it.
---

# Schedules

A **Schedule** is a server-side object that starts a workflow on a recurring spec.
Unlike a loop in your own process, a schedule lives in the Temporal cluster: it
keeps firing across client and worker restarts, and you manage it with a stable
`schedule_id` rather than a handle to a single run. Each firing starts the
configured workflow as an independent execution.

Schedules are managed entirely from the client — `temporal::client::Client`
exposes the full lifecycle. The spec and the start action are carried in
`temporal::ScheduleOptions`.

:::note
This SDK exposes a deliberately minimal subset of Temporal's schedule API: an
**interval** spec and/or **cron** expressions, plus a start-workflow action. See
[Limits](#limits) for what is and isn't surfaced.
:::

## Creating a schedule {#creating}

Fill in `ScheduleOptions` and call `CreateSchedule(schedule_id, options)`. The
action is always "start this workflow"; you must set `workflow_type` and
`task_queue`. `workflow_id` is optional and defaults to `<schedule id>-workflow`.

```cpp
struct ScheduleOptions {
  std::chrono::seconds interval{0};            // run the action every `interval`
  std::vector<std::string> cron_expressions;   // calendar/cron triggers
  std::string workflow_type;                   // required: the workflow to start
  std::string task_queue;                      // required: its task queue
  std::string workflow_id;                     // default: "<schedule id>-workflow"
};
```

### Interval spec {#interval}

Set `interval` to fire on a fixed period. The field is a `std::chrono::seconds`,
so any chrono duration that converts to seconds works:

```cpp
#include <temporal/temporal.h>

temporal::ScheduleOptions opts;
opts.interval = std::chrono::hours(1);   // run once an hour
opts.workflow_type = "EchoWorkflow";
opts.task_queue = "reports";

client.CreateSchedule("hourly-report", opts);
```

### Cron / calendar spec {#cron}

Set `cron_expressions` to fire on a calendar. Each string is **one** trigger:
standard 5-field cron (`minute hour day-of-month month day-of-week`), optionally
with a **leading seconds field** (making it 6 fields) and/or a trailing
`CRON_TZ=<zone>`. Provide several expressions to trigger on any of them.

```cpp
temporal::ScheduleOptions opts;
opts.cron_expressions = {"0 9 * * MON-FRI"};   // weekdays at 09:00
opts.workflow_type = "ReportWorkflow";
opts.task_queue = "reports";

client.CreateSchedule("daily-report", opts);
```

```cpp
// 6-field form (leading seconds) + timezone: every 30 minutes, in Europe/Berlin.
opts.cron_expressions = {"0 0,30 * * * * CRON_TZ=Europe/Berlin"};
```

`interval` and `cron_expressions` are not mutually exclusive — set both and the
schedule fires on the interval **and** on every cron trigger. Set just one (or, as
above, several cron strings) to use that spec alone.

## Lifecycle {#lifecycle}

All lifecycle calls take the `schedule_id` you created the schedule with. The
relevant signatures on `Client`:

```cpp
void                     CreateSchedule(const std::string& schedule_id, const ScheduleOptions& options);
bool                     DescribeSchedule(const std::string& schedule_id);
void                     UpdateSchedule(const std::string& schedule_id, const ScheduleOptions& options);
void                     TriggerSchedule(const std::string& schedule_id);
void                     PauseSchedule(const std::string& schedule_id, const std::string& note = "");
void                     UnpauseSchedule(const std::string& schedule_id, const std::string& note = "");
void                     DeleteSchedule(const std::string& schedule_id);
std::vector<std::string> ListSchedules();
```

### Describe {#describe}

`DescribeSchedule` returns whether the schedule **exists** — `true` if present,
`false` if not found. It throws only on errors other than not-found, so it doubles
as an existence check:

```cpp
bool exists = client.DescribeSchedule("daily-report");
```

### Update {#update}

`UpdateSchedule` replaces the schedule's spec and action wholesale with a fresh
`ScheduleOptions` — there is no partial patch. Build the options you want the
schedule to have from now on and pass them:

```cpp
temporal::ScheduleOptions opts;
opts.interval = std::chrono::hours(2);   // was hourly; now every two hours
opts.workflow_type = "ReportWorkflow";
opts.task_queue = "reports";

client.UpdateSchedule("daily-report", opts);
```

### Pause / unpause {#pause}

Pausing stops the schedule from firing while keeping it defined; unpausing resumes
it. Both take an optional `note` recorded on the schedule. A paused schedule still
exists and is still describable.

```cpp
client.PauseSchedule("daily-report", "freezing reports during migration");
// ... later ...
client.UnpauseSchedule("daily-report", "migration done");
```

### Trigger (run now) {#trigger}

`TriggerSchedule` starts the action **immediately**, regardless of the spec and
even while the schedule is paused. Use it to run a scheduled workflow on demand
without waiting for the next firing.

```cpp
client.TriggerSchedule("daily-report");
```

### List {#list}

`ListSchedules` returns every schedule id in the namespace, paging through results
internally. Listing is **eventually consistent** — a just-created schedule may not
appear immediately:

```cpp
std::vector<std::string> ids = client.ListSchedules();
for (const std::string& id : ids) {
  // ...
}
```

### Delete {#delete}

`DeleteSchedule` removes the schedule. It does not affect workflow runs the
schedule already started — only future firings.

```cpp
client.DeleteSchedule("daily-report");
```

## End-to-end example {#example}

Create a cron schedule, confirm it exists, run it once on demand, pause it, and
finally delete it:

```cpp
#include <temporal/temporal.h>

void ManageDailyReport(temporal::client::Client& client) {
  const std::string schedule_id = "daily-report";

  // 1. Create — weekday mornings at 09:00.
  temporal::ScheduleOptions opts;
  opts.cron_expressions = {"0 9 * * MON-FRI"};
  opts.workflow_type = "ReportWorkflow";
  opts.task_queue = "reports";
  client.CreateSchedule(schedule_id, opts);

  // 2. Describe — confirm the server registered it.
  if (!client.DescribeSchedule(schedule_id)) {
    throw std::runtime_error("schedule was not created");
  }

  // 3. Trigger — run it once right now, without waiting for 09:00.
  client.TriggerSchedule(schedule_id);

  // 4. Pause — stop future firings (the manual trigger above still ran).
  client.PauseSchedule(schedule_id, "paused after a manual run");

  // 5. Delete — tear it down (already-started runs are unaffected).
  client.DeleteSchedule(schedule_id);
}
```

## Limits {#limits}

The schedule surface here is intentionally small. Verified against the current API:

- **Action is start-workflow only.** Every schedule starts the workflow named by
  `ScheduleOptions::workflow_type` on `task_queue`; no other action types exist.
- **No workflow arguments in the action.** `ScheduleOptions` carries the workflow
  type, task queue, and (optional) workflow id — there is no field for start
  arguments, memo, or search attributes on the scheduled run.
- **`DescribeSchedule` returns only existence.** It yields a `bool`, not a rich
  description object — there is no schedule-info / next-run-time / recent-actions
  struct in this SDK.
- **Spec is interval and/or cron.** Only `interval` and `cron_expressions` are
  exposed. Structured calendar specs (explicit day/month lists, skip ranges,
  jitter) and an **overlap policy** for concurrent runs are **not** surfaced.
- **No pause-on-create.** A schedule starts active; pause it explicitly with
  `PauseSchedule` if you need it to begin paused.
- **`ListSchedules` is eventually consistent.** A newly created schedule may take a
  moment to appear; poll if you need to observe it right after creating it.
