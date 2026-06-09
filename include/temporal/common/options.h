#pragma once

#include <chrono>
#include <cstdint>
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
namespace interceptor {
class Interceptor;  // forward-declared; full type in <temporal/interceptor/interceptor.h>
}  // namespace interceptor

// Retry behavior for activities (and, where supported, workflows). Mirrors
// `temporal.api.common.v1.RetryPolicy`. Zero fields fall back to server defaults.
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};
  double backoff_coefficient{2.0};
  std::chrono::milliseconds maximum_interval{0};  // 0 => 100 * initial_interval
  int maximum_attempts{0};                        // 0 => unlimited
  std::vector<std::string> non_retryable_error_types;
};

// TLS / mTLS settings for the client connection. Cert/key fields are PEM
// *contents* (not file paths); leave server_ca_cert empty for the system trust
// store. Set client_cert + client_key for mutual TLS.
struct TlsConfig {
  bool enabled = false;
  std::string server_ca_cert;  // PEM root CA(s)
  std::string client_cert;     // PEM client cert chain (mTLS)
  std::string client_key;      // PEM client private key (mTLS)
  std::string server_name;     // SNI / certificate name override (optional)
};

// Options for connecting a Client to the Temporal frontend service.
struct ClientOptions {
  std::string target = "localhost:7233";
  std::string ns = "default";  // Temporal namespace ('namespace' is a keyword)
  std::string identity;        // default: "<pid>@<host>"
  std::shared_ptr<log::Logger> logger;            // default: console logger
  std::shared_ptr<DataConverter> data_converter;  // default: JSON converter
  TlsConfig tls;        // disabled by default (insecure channel)
  std::string api_key;  // sent as an "Authorization: Bearer <key>" header per RPC
  // Client-outbound interceptors, applied in order (front = outermost).
  std::vector<std::shared_ptr<interceptor::Interceptor>> interceptors;
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
  // Context-propagation headers. Readable in the workflow (Context::GetHeader)
  // and auto-propagated to the activities/child workflows it starts.
  std::map<std::string, Payload> headers;
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

// Options for `workflow::Context::ExecuteLocalActivity`. A local activity runs
// inline in the workflow worker (no activity-task round-trip) and records its
// result as a marker; retries happen inline within the workflow task.
struct LocalActivityOptions {
  std::chrono::milliseconds start_to_close_timeout{0};  // advisory bound on inline time
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

// Sink for worker metrics. Implement to forward to a backend (Prometheus, StatsD,
// OpenTelemetry, …) and set it on WorkerOptions::metrics_handler.
class MetricsHandler {
 public:
  MetricsHandler() = default;
  virtual ~MetricsHandler() = default;
  MetricsHandler(const MetricsHandler&) = delete;
  MetricsHandler& operator=(const MetricsHandler&) = delete;
  MetricsHandler(MetricsHandler&&) = delete;
  MetricsHandler& operator=(MetricsHandler&&) = delete;

  using Tags = std::map<std::string, std::string>;
  virtual void Counter(const std::string& name, std::int64_t value, const Tags& tags) = 0;
  virtual void Gauge(const std::string& name, double value, const Tags& tags) = 0;
  virtual void Timer(const std::string& name, std::chrono::nanoseconds value, const Tags& tags) = 0;
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
  // Optional metrics sink; nullptr disables metric emission.
  std::shared_ptr<MetricsHandler> metrics_handler;
  // Worker interceptors (activity-inbound is wired; workflow-inbound pending),
  // applied in order (front = outermost).
  std::vector<std::shared_ptr<interceptor::Interceptor>> interceptors;

  // Caps how many activity / workflow tasks may execute concurrently across all
  // pollers (each poller blocks before dispatching once the cap is reached).
  // 0 => unlimited. Distinct from the poller counts above, which bound how many
  // long-polls are in flight, not how many tasks run at once.
  int max_concurrent_activity_executions = 0;
  int max_concurrent_workflow_task_executions = 0;

  // If > 0, a watchdog reports (metric + log) any workflow task whose handling
  // exceeds this deadline — a likely deadlock (blocking call / infinite loop in
  // workflow code). Detection only: the coroutine runs on the poller thread and
  // can't be safely interrupted, so the task is flagged, not aborted. Note this
  // measures total task time, which includes inline local activities.
  std::chrono::milliseconds deadlock_detection_timeout{0};

  // Caps the rate at which this worker starts activity executions, in activities
  // per second across all activity pollers (0 => unlimited). Paces starts evenly
  // (no burst); mirrors the Go SDK's WorkerActivitiesPerSecond.
  double max_activities_per_second = 0.0;

  // On Stop(), how long to wait for in-flight task executions to finish after
  // polling for new tasks ceases. 0 => don't wait (join pollers immediately,
  // which still lets any in-flight Handle() run to completion before its thread
  // exits). A positive value bounds the drain so Stop() cannot hang forever.
  std::chrono::milliseconds graceful_shutdown_timeout{0};

  // Poller autoscaling (demand-driven elasticity). When enabled, the worker spawns
  // a pool of up to `max_concurrent_pollers` poller threads per loop kind; only
  // `min_concurrent_pollers` long-poll at all times, and the rest are activated on
  // demand. A poll that returns a task scales the active count up (toward the max);
  // `autoscaling_idle_polls_before_park` consecutive empty polls scale it back down
  // (toward the min), so idle excess pollers park instead of hot-spinning the
  // long-poll. `max_concurrent_pollers` <= the base poller count falls back to a
  // fixed pool sized at the base count (i.e. no elasticity).
  bool enable_poller_autoscaling = false;
  int min_concurrent_pollers = 1;
  int max_concurrent_pollers = 4;
  int autoscaling_idle_polls_before_park = 5;

  // Session workers (host pinning). When enabled, the worker long-polls a
  // host-unique session task queue and registers the built-in session
  // creation/completion activities used by workflow::Context::CreateSession /
  // CompleteSession. CreateSession reserves a session slot on the handling worker
  // (bounded by `max_concurrent_sessions`, 0 => unlimited) and returns that
  // host's session queue, so activities scheduled on it run on the same worker
  // (e.g. to share host-local files); CompleteSession releases the slot.
  bool enable_sessions = false;
  int max_concurrent_sessions = 0;
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
