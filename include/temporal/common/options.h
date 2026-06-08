#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

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

// Options for a Worker.
struct WorkerOptions {
  int max_concurrent_activities = 0;      // 0 => library default
  int max_concurrent_workflow_tasks = 0;  // 0 => library default
  int workflow_task_pollers = 1;
  int activity_task_pollers = 1;
};

}  // namespace temporal
