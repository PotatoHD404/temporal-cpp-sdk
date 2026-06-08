#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <temporal/common/payload.h>

namespace temporal {

namespace log {
class Logger;
}
class DataConverter;

// Retry behavior for activities (and, where supported, workflows). Mirrors
// `temporal.api.common.v1.RetryPolicy`. Zero fields fall back to server defaults.
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};
  double backoff_coefficient{2.0};
  std::chrono::milliseconds maximum_interval{0};  // 0 => 100 * initial_interval
  int maximum_attempts{0};                        // 0 => unlimited
  std::vector<std::string> non_retryable_error_types;
};

// Options for connecting a Client to the Temporal frontend service.
struct ClientOptions {
  std::string target = "localhost:7233";
  std::string ns = "default";  // Temporal namespace ('namespace' is a keyword)
  std::string identity;        // default: "<pid>@<host>"
  std::shared_ptr<log::Logger> logger;            // default: console logger
  std::shared_ptr<DataConverter> data_converter;  // default: JSON converter
};

// Options for `Client::StartWorkflow`.
struct StartWorkflowOptions {
  std::string id;          // default: a random UUID
  std::string task_queue;  // required
  std::chrono::milliseconds execution_timeout{0};
  std::chrono::milliseconds run_timeout{0};
  std::chrono::milliseconds task_timeout{0};
  RetryPolicy retry_policy;
  bool retry_policy_set = false;
  // Non-indexed metadata attached to the workflow, returned by Describe. Build
  // values with the data converter, e.g. `o.memo["owner"] = dc->ToPayload("me")`.
  std::map<std::string, Payload> memo;
  // Indexed search attributes for visibility queries. Build typed values with the
  // `temporal::sa::` helpers, e.g. `o.search_attributes["Tier"] = sa::Keyword("gold")`.
  // The named attribute must be registered on the namespace.
  std::map<std::string, Payload> search_attributes;
};

// Options for `workflow::Context::ExecuteActivity`.
struct ActivityOptions {
  std::string task_queue;  // default: the workflow's task queue
  std::chrono::milliseconds schedule_to_close_timeout{0};
  std::chrono::milliseconds schedule_to_start_timeout{0};
  std::chrono::milliseconds start_to_close_timeout{0};  // effectively required
  std::chrono::milliseconds heartbeat_timeout{0};
  RetryPolicy retry_policy;
  bool retry_policy_set = false;
};

// Options for `workflow::Context::ExecuteChildWorkflow`.
struct ChildWorkflowOptions {
  std::string id;          // default: "<parent id>_c<seq>"
  std::string task_queue;  // default: the parent's task queue
};

// How a worker reacts to a non-deterministic workflow detected during replay
// (the workflow's replayed commands diverge from recorded history).
enum class WorkflowPanicPolicy : unsigned char {
  // Fail the workflow *task*. The server retries it, so a corrected worker
  // build can recover the workflow without data loss. This is the safe default
  // and matches the Go/Java SDKs' BlockWorkflow behavior.
  BlockWorkflow,
  // Fail the workflow *execution* outright. Terminal; use only when a stuck
  // workflow is worse than a failed one.
  FailWorkflow,
};

// Options for a Worker.
struct WorkerOptions {
  int max_concurrent_activities = 0;      // 0 => library default
  int max_concurrent_workflow_tasks = 0;  // 0 => library default
  int workflow_task_pollers = 1;
  int activity_task_pollers = 1;
  WorkflowPanicPolicy panic_policy = WorkflowPanicPolicy::BlockWorkflow;
  // Max resident (sticky-cached) workflows; 0 => unbounded. Beyond this, the
  // least-recently-used workflow is evicted (its next task triggers a replay).
  int max_cached_workflows = 0;
};

// Options for `Client::CreateSchedule`. The schedule runs the given workflow on a
// fixed interval. (A minimal subset of the schedule API: interval spec +
// start-workflow action; calendars, overlap policy, and pause are not exposed.)
struct ScheduleOptions {
  std::chrono::seconds interval{0};  // run the action every `interval`
  std::string workflow_type;         // required: the workflow to start
  std::string task_queue;            // required: its task queue
  std::string workflow_id;           // default: "<schedule id>-workflow"
};

}  // namespace temporal
