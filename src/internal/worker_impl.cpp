#include "internal/worker_impl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <string>
#include <thread>
#include <utility>

#include "google/protobuf/util/json_util.h"
#include "temporal/api/enums/v1/task_queue.pb.h"

#include "internal/grpc_client.h"
#include "internal/proto_util.h"

#include <temporal/common/errors.h>
#include <temporal/common/session.h>

namespace temporal::internal {
namespace {

namespace enums = ::temporal::api::enums::v1;

// Set by SIGINT/SIGTERM so a blocking Run() can return. Process-global because
// signal handlers cannot carry context.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> g_interrupted{false};

void HandleSignal(int /*signum*/) { g_interrupted.store(true); }

}  // namespace

WorkerImpl::WorkerImpl(std::shared_ptr<GrpcClient> grpc, std::shared_ptr<DataConverter> converter,
                       std::shared_ptr<log::Logger> logger, std::string task_queue,
                       WorkerOptions options)
    : grpc_(std::move(grpc)),
      converter_(std::move(converter)),
      logger_(std::move(logger)),
      task_queue_(std::move(task_queue)),
      sticky_queue_("temporal-cpp-sdk-sticky-" + NewUuid()),
      session_queue_(task_queue_ + "-session-" + NewUuid()),
      options_(options),
      workflow_handler_(grpc_.get(), converter_, logger_, task_queue_, sticky_queue_,
                        options.panic_policy, options.max_cached_workflows,
                        options.deadlock_detection_timeout),
      activity_handler_(grpc_.get(), converter_, logger_, task_queue_),
      nexus_handler_(grpc_.get(), converter_, logger_, task_queue_),
      workflow_gate_(options.max_concurrent_workflow_task_executions),
      activity_gate_(options.max_concurrent_activity_executions),
      session_gate_(options.max_concurrent_sessions),
      nexus_gate_(options.max_concurrent_activity_executions),
      activity_rate_limiter_(options.max_activities_per_second) {
  // Let workflows run registered activities inline as local activities (the
  // workflow handler resolves activity functions from the activity registry).
  workflow_handler_.SetLocalActivityResolver(
      [this](const std::string& type) { return activity_handler_.Lookup(type); });
  // Inbound interceptors run around every activity / workflow execution.
  activity_handler_.SetInterceptors(options_.interceptors);
  workflow_handler_.SetInterceptors(options_.interceptors);
  if (options_.enable_sessions) {
    RegisterSessionActivities();
  }
}

void WorkerImpl::RegisterSessionActivities() {
  // Creation runs on the base queue: reserve a session slot (bounded by
  // max_concurrent_sessions; a retryable error when full makes the server retry
  // until a slot frees or creation_timeout elapses) and return THIS worker's
  // host-unique session queue, pinning the session's later activities here.
  activity_handler_.Register(
      kSessionCreationActivityType, [this](activity::Context&, const Payloads&) -> Payloads {
        const int cap = options_.max_concurrent_sessions;
        if (cap > 0) {
          for (int cur = active_sessions_.load();;) {
            if (cur >= cap) {
              throw ApplicationError("worker session capacity reached", "SessionCapacityError");
            }
            if (active_sessions_.compare_exchange_weak(cur, cur + 1)) {
              break;
            }
          }
        } else {
          active_sessions_.fetch_add(1);
        }
        return converter_->ToPayloads(session_queue_);
      });
  // Completion runs on the host session queue (so it reaches the owning worker):
  // release one session slot.
  activity_handler_.Register(kSessionCompletionActivityType,
                             [this](activity::Context&, const Payloads&) -> Payloads {
                               int cur = active_sessions_.load();
                               while (cur > 0 && !active_sessions_.compare_exchange_weak(cur, cur - 1)) {
                               }
                               return Payloads{};
                             });
}

WorkerImpl::~WorkerImpl() { Stop(); }

void WorkerImpl::ReplayWorkflowHistory(const std::string& history_json) {
  hist::History history;
  const auto status = google::protobuf::util::JsonStringToMessage(history_json, &history);
  if (!status.ok()) {
    throw ApplicationError("failed to parse history JSON: " + std::string(status.message()),
                           "ReplayError");
  }
  if (const auto mismatch = workflow_handler_.ReplayHistory(history)) {
    throw ApplicationError(*mismatch, "NonDeterministicError");
  }
}

void WorkerImpl::RegisterWorkflow(std::string name, worker::WorkflowFn fn) {
  workflow_handler_.Register(std::move(name), std::move(fn));
}

void WorkerImpl::RegisterActivity(std::string name, worker::ActivityFn fn) {
  activity_handler_.Register(std::move(name), std::move(fn));
}

void WorkerImpl::RegisterNexusOperation(std::string service, std::string operation,
                                        worker::NexusOperationFn fn) {
  nexus_handler_.Register(std::move(service), std::move(operation), std::move(fn));
}

void WorkerImpl::Start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    return;
  }
  const int wf_base = options_.workflow_task_pollers > 0 ? options_.workflow_task_pollers : 1;
  const int act_base = options_.activity_task_pollers > 0 ? options_.activity_task_pollers : 1;
  const bool scale = options_.enable_poller_autoscaling;
  const int min_hot = scale ? std::max(1, options_.min_concurrent_pollers) : 0;
  // Pool size per kind: the configured base, grown to max_concurrent_pollers when
  // autoscaling (never below the base or the minimum). Non-scaling = fixed base.
  const int wf_pool =
      scale ? std::max({wf_base, options_.max_concurrent_pollers, min_hot}) : wf_base;
  const int act_pool =
      scale ? std::max({act_base, options_.max_concurrent_pollers, min_hot}) : act_base;

  if (workflow_handler_.has_workflows()) {
    // Paired normal + sticky loops: the first min_hot of each kind stay hot (so
    // both queues are always polled), the remainder of the 2*wf_pool loops scale.
    const int always_hot = scale ? 2 * std::min(min_hot, wf_pool) : 2 * wf_pool;
    wf_scaler_.Configure(always_hot, (2 * wf_pool) - always_hot,
                         options_.autoscaling_idle_polls_before_park);
    for (int i = 0; i < wf_pool; ++i) {
      threads_.emplace_back([this, i] { WorkflowPollLoop(/*sticky=*/false, i); });
      threads_.emplace_back([this, i] { WorkflowPollLoop(/*sticky=*/true, i); });
    }
  }
  if (activity_handler_.has_activities()) {
    const int always_hot = scale ? std::min(min_hot, act_pool) : act_pool;
    act_scaler_.Configure(always_hot, act_pool - always_hot,
                          options_.autoscaling_idle_polls_before_park);
    for (int i = 0; i < act_pool; ++i) {
      threads_.emplace_back([this, i] { ActivityPollLoop(/*session=*/false, i); });
    }
    if (options_.enable_sessions) {
      // The session poller is always hot and outside the scaler (index ignored).
      threads_.emplace_back([this] { ActivityPollLoop(/*session=*/true, 0); });
    }
  }
  if (nexus_handler_.has_operations()) {
    // Single always-hot Nexus poller (Nexus tasks are infrequent vs. activities).
    threads_.emplace_back([this] { NexusPollLoop(); });
  }
  if (options_.deadlock_detection_timeout.count() > 0 && workflow_handler_.has_workflows()) {
    threads_.emplace_back([this] { DeadlockWatchLoop(); });
  }
  logger_->Info("worker started", {log::F("task_queue", task_queue_)});
}

void WorkerImpl::Run() {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);
  Start();
  while (!stop_.load() && !g_interrupted.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  Stop();
}

void WorkerImpl::Stop() {
  // Phase 1: stop accepting NEW tasks. draining_ makes pollers skip dispatch even
  // if a poll already returned a task; stop_ ends the loops.
  draining_.store(true);
  stop_.store(true);
  // Wake any poller blocked in ConcurrencyGate::Acquire() so it can exit instead
  // of waiting for a permit that may never free during shutdown.
  workflow_gate_.ReleaseAll();
  activity_gate_.ReleaseAll();
  session_gate_.ReleaseAll();
  nexus_gate_.ReleaseAll();
  // Wake any scalable poller parked in PollerScaler::Acquire() so it sees stop_.
  wf_scaler_.Wake();
  act_scaler_.Wake();

  // Phase 2: wait for in-flight Handle() calls to finish, bounded by the timeout.
  // Pollers blocked in a long-poll RPC are unaffected here; they return on the
  // server-side poll timeout and then observe stop_.
  if (options_.graceful_shutdown_timeout.count() > 0) {
    const auto deadline = std::chrono::steady_clock::now() + options_.graceful_shutdown_timeout;
    const bool wf_drained = workflow_gate_.Drain(deadline);
    const bool act_drained = activity_gate_.Drain(deadline);
    const bool sess_drained = session_gate_.Drain(deadline);
    const bool nexus_drained = nexus_gate_.Drain(deadline);
    if (!wf_drained || !act_drained || !sess_drained || !nexus_drained) {
      logger_->Warn("graceful shutdown timed out with tasks still in flight",
                    {log::F("task_queue", task_queue_)});
    }
  }

  // Phase 3: join. A thread mid-Handle() runs the call to completion before its
  // loop re-checks stop_ and exits, so no in-flight task is abandoned.
  for (auto& t : threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  threads_.clear();
}

void WorkerImpl::EmitStickyCacheMetrics(MetricsHandler* metrics) {
  // cache_hits() = continuations served from the sticky cache (a cache HIT);
  // replays() = full from-scratch replays (a cache MISS). Both are cumulative and
  // shared by the normal + sticky loops, so derive deltas under a mutex to avoid
  // double-counting, then emit the running totals as gauges.
  const long hits = workflow_handler_.cache_hits();
  const long misses = workflow_handler_.replays();
  long hit_delta = 0;
  long miss_delta = 0;
  {
    const std::lock_guard<std::mutex> lock(sticky_metrics_mu_);
    hit_delta = hits - last_cache_hits_;
    miss_delta = misses - last_replays_;
    last_cache_hits_ = hits;
    last_replays_ = misses;
  }
  const MetricsHandler::Tags tags{{"task_queue", task_queue_}};
  if (hit_delta > 0) {
    metrics->Counter("temporal_sticky_cache_hit", hit_delta, tags);
  }
  if (miss_delta > 0) {
    metrics->Counter("temporal_sticky_cache_miss", miss_delta, tags);
  }
  metrics->Gauge("temporal_sticky_cache_total_hits", static_cast<double>(hits), tags);
  metrics->Gauge("temporal_sticky_cache_total_misses", static_cast<double>(misses), tags);
  // No public accessor exposes the cache's resident entry count, so report the
  // configured capacity (when bounded) as the cache-size gauge.
  if (options_.max_cached_workflows > 0) {
    metrics->Gauge("temporal_sticky_cache_size",
                   static_cast<double>(options_.max_cached_workflows), tags);
  }
}

void WorkerImpl::WorkflowPollLoop(bool sticky, int index) {
  auto* metrics = options_.metrics_handler.get();
  const bool scalable =
      options_.enable_poller_autoscaling && index >= std::max(1, options_.min_concurrent_pollers);
  const MetricsHandler::Tags poller_tags{{"task_queue", task_queue_},
                                         {"poller_type", sticky ? "sticky" : "workflow"}};
  const MetricsHandler::Tags wf_tags{{"task_queue", task_queue_}};
  if (metrics) {
    metrics->Counter("temporal_poller_start", 1, poller_tags);
    metrics->Gauge("temporal_pollers_in_flight", static_cast<double>(wf_scaler_.active()), wf_tags);
  }
  while (!stop_.load()) {
    // Scalable pollers park here until demand raises the target (or Stop wakes us).
    if (scalable && !wf_scaler_.Acquire(stop_)) {
      break;
    }
    bool slot_released = !scalable;
    try {
      wsv::PollWorkflowTaskQueueRequest req;
      req.set_namespace_(grpc_->ns());
      if (sticky) {
        req.mutable_task_queue()->set_name(sticky_queue_);
        req.mutable_task_queue()->set_kind(enums::TASK_QUEUE_KIND_STICKY);
        req.mutable_task_queue()->set_normal_name(task_queue_);
      } else {
        req.mutable_task_queue()->set_name(task_queue_);
        req.mutable_task_queue()->set_kind(enums::TASK_QUEUE_KIND_NORMAL);
      }
      req.set_identity(grpc_->identity());
      const auto resp = grpc_->PollWorkflowTaskQueue(req);
      const auto recv = std::chrono::steady_clock::now();
      // An empty token is a poll timeout (no task); a non-empty one is a hit.
      const bool got_task = !resp.task_token().empty();
      if (options_.enable_poller_autoscaling) {
        wf_scaler_.Report(got_task);  // scale up on a task, down after empty streaks
      }
      if (scalable) {
        wf_scaler_.Release();  // hold the poll slot only across the long-poll itself
        slot_released = true;
      }
      if (stop_.load()) {
        break;
      }
      if (!got_task) {
        if (metrics) {
          metrics->Counter("temporal_workflow_poll_timeout", 1, {});
        }
        continue;
      }
      if (metrics) {
        metrics->Counter("temporal_workflow_poll_success", 1, {});
      }
      // Don't dispatch new work once draining; let the slot/thread wind down.
      if (draining_.load()) {
        continue;
      }
      // Bound concurrent executions. A released gate (Stop) returns false; bail.
      GateGuard permit(workflow_gate_, workflow_gate_.Acquire());
      if (!permit.held()) {
        break;
      }
      if (metrics) {
        metrics->Gauge("temporal_workflow_tasks_in_flight",
                       static_cast<double>(workflow_gate_.in_flight()), {});
        // Slots available = configured cap minus in-flight (only when bounded).
        const int cap = options_.max_concurrent_workflow_task_executions;
        if (cap > 0) {
          metrics->Gauge("temporal_worker_task_slots_available",
                         static_cast<double>(cap - workflow_gate_.in_flight()),
                         MetricsHandler::Tags{{"task_queue", task_queue_},
                                              {"worker_type", "workflow"}});
        }
      }
      const auto start = std::chrono::steady_clock::now();
      if (metrics) {
        // Time the task waited locally (gate/rate) between receipt and execution.
        metrics->Timer("temporal_workflow_task_schedule_to_start_latency", start - recv, wf_tags);
      }
      const bool watch = options_.deadlock_detection_timeout.count() > 0;
      if (watch) {
        deadlock_watch_.Begin();
      }
      workflow_handler_.Handle(resp);
      if (watch) {
        deadlock_watch_.End();
      }
      if (metrics) {
        const auto now = std::chrono::steady_clock::now();
        metrics->Timer("temporal_workflow_task_execution_latency", now - start, {});
        metrics->Timer("temporal_workflow_task_end_to_end_latency", now - recv, wf_tags);
        metrics->Counter("temporal_workflow_task_handled", 1, {});
        EmitStickyCacheMetrics(metrics);
      }
    } catch (const std::exception& e) {
      if (scalable && !slot_released) {
        wf_scaler_.Release();  // the poll RPC itself threw; free the slot
      }
      deadlock_watch_.End();  // clear this thread's entry if Handle threw
      if (stop_.load()) {
        break;
      }
      if (metrics) {
        metrics->Counter("temporal_workflow_task_failed", 1, wf_tags);
      }
      logger_->Error("workflow poll loop error", {log::F("error", e.what())});
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

void WorkerImpl::DeadlockWatchLoop() {
  auto* metrics = options_.metrics_handler.get();
  const auto deadline = options_.deadlock_detection_timeout;
  // Poll a few times per deadline window so an overrun is reported promptly, but
  // never faster than 50ms (and react quickly to Stop()).
  const auto interval = std::max(std::chrono::milliseconds(50),
                                 std::chrono::duration_cast<std::chrono::milliseconds>(deadline) / 4);
  while (!stop_.load()) {
    std::this_thread::sleep_for(interval);
    if (stop_.load()) {
      break;
    }
    const int overruns = deadlock_watch_.ReportOverruns(deadline);
    if (overruns > 0) {
      logger_->Error("possible workflow deadlock: a task exceeded its deadline",
                     {log::F("task_queue", task_queue_),
                      log::F("timeout_ms", std::to_string(deadline.count()))});
      if (metrics != nullptr) {
        metrics->Counter("temporal_workflow_task_deadlock", overruns, {});
      }
    }
  }
}

void WorkerImpl::ActivityPollLoop(bool session, int index) {
  auto* metrics = options_.metrics_handler.get();
  // Session activities draw permits from a dedicated gate so they can be capped
  // independently of normal activity executions. The session poller is always hot;
  // only surplus normal-activity pollers scale with demand.
  ConcurrencyGate& gate = session ? session_gate_ : activity_gate_;
  const bool scalable = options_.enable_poller_autoscaling && !session &&
                        index >= std::max(1, options_.min_concurrent_pollers);
  const MetricsHandler::Tags poller_tags{{"task_queue", task_queue_},
                                         {"poller_type", session ? "session" : "activity"}};
  const MetricsHandler::Tags act_tags{{"task_queue", task_queue_}};
  if (metrics) {
    metrics->Counter("temporal_poller_start", 1, poller_tags);
    metrics->Gauge("temporal_pollers_in_flight",
                   static_cast<double>(act_scaler_.active() + (options_.enable_sessions ? 1 : 0)),
                   act_tags);
  }
  while (!stop_.load()) {
    if (scalable && !act_scaler_.Acquire(stop_)) {
      break;
    }
    bool slot_released = !scalable;
    try {
      wsv::PollActivityTaskQueueRequest req;
      req.set_namespace_(grpc_->ns());
      req.mutable_task_queue()->set_name(session ? session_queue_ : task_queue_);
      req.mutable_task_queue()->set_kind(enums::TASK_QUEUE_KIND_NORMAL);
      req.set_identity(grpc_->identity());
      const auto resp = grpc_->PollActivityTaskQueue(req);
      const auto recv = std::chrono::steady_clock::now();
      const bool got_task = !resp.task_token().empty();
      if (options_.enable_poller_autoscaling && !session) {
        act_scaler_.Report(got_task);
      }
      if (scalable) {
        act_scaler_.Release();
        slot_released = true;
      }
      if (stop_.load()) {
        break;
      }
      if (!got_task) {
        if (metrics) {
          metrics->Counter("temporal_activity_poll_timeout", 1, {});
        }
        continue;
      }
      if (metrics) {
        metrics->Counter("temporal_activity_poll_success", 1, {});
      }
      if (draining_.load()) {
        continue;
      }
      // Pace activity starts to the configured per-second rate (no-op if unset).
      activity_rate_limiter_.Acquire(stop_);
      if (stop_.load()) {
        break;
      }
      GateGuard permit(gate, gate.Acquire());
      if (!permit.held()) {
        break;
      }
      if (metrics) {
        metrics->Gauge("temporal_activity_tasks_in_flight",
                       static_cast<double>(gate.in_flight()), {});
        // Slots available = configured cap minus in-flight (only when bounded).
        const int cap = session ? options_.max_concurrent_sessions
                                 : options_.max_concurrent_activity_executions;
        if (cap > 0) {
          metrics->Gauge("temporal_worker_task_slots_available",
                         static_cast<double>(cap - gate.in_flight()),
                         MetricsHandler::Tags{{"task_queue", task_queue_},
                                              {"worker_type", session ? "session" : "activity"}});
        }
      }
      const auto start = std::chrono::steady_clock::now();
      if (metrics) {
        // Time the task waited locally (gate/rate) between receipt and execution.
        metrics->Timer("temporal_activity_task_schedule_to_start_latency", start - recv, act_tags);
      }
      activity_handler_.Handle(resp);
      if (metrics) {
        const auto now = std::chrono::steady_clock::now();
        metrics->Timer("temporal_activity_task_execution_latency", now - start, {});
        metrics->Timer("temporal_activity_task_end_to_end_latency", now - recv, act_tags);
        metrics->Counter("temporal_activity_task_handled", 1, {});
      }
    } catch (const std::exception& e) {
      if (scalable && !slot_released) {
        act_scaler_.Release();  // the poll RPC itself threw; free the slot
      }
      if (stop_.load()) {
        break;
      }
      if (metrics) {
        metrics->Counter("temporal_activity_task_failed", 1, act_tags);
      }
      logger_->Error("activity poll loop error", {log::F("error", e.what())});
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

void WorkerImpl::NexusPollLoop() {
  auto* metrics = options_.metrics_handler.get();
  const MetricsHandler::Tags nexus_tags{{"task_queue", task_queue_}};
  if (metrics) {
    metrics->Counter("temporal_poller_start", 1,
                     MetricsHandler::Tags{{"task_queue", task_queue_}, {"poller_type", "nexus"}});
  }
  while (!stop_.load()) {
    try {
      wsv::PollNexusTaskQueueRequest req;
      req.set_namespace_(grpc_->ns());
      req.mutable_task_queue()->set_name(task_queue_);
      req.mutable_task_queue()->set_kind(enums::TASK_QUEUE_KIND_NORMAL);
      req.set_identity(grpc_->identity());
      const auto resp = grpc_->PollNexusTaskQueue(req);
      const auto recv = std::chrono::steady_clock::now();
      // An empty token is a poll timeout (no task); a non-empty one is a hit.
      const bool got_task = !resp.task_token().empty();
      if (stop_.load()) {
        break;
      }
      if (!got_task) {
        if (metrics) {
          metrics->Counter("temporal_nexus_poll_timeout", 1, {});
        }
        continue;
      }
      if (metrics) {
        metrics->Counter("temporal_nexus_poll_success", 1, {});
      }
      if (draining_.load()) {
        continue;
      }
      // Bound concurrent executions. A released gate (Stop) returns false; bail.
      GateGuard permit(nexus_gate_, nexus_gate_.Acquire());
      if (!permit.held()) {
        break;
      }
      const auto start = std::chrono::steady_clock::now();
      if (metrics) {
        metrics->Timer("temporal_nexus_task_schedule_to_start_latency", start - recv, nexus_tags);
      }
      nexus_handler_.Handle(resp);
      if (metrics) {
        const auto now = std::chrono::steady_clock::now();
        metrics->Timer("temporal_nexus_task_execution_latency", now - start, {});
        metrics->Counter("temporal_nexus_task_handled", 1, {});
      }
    } catch (const std::exception& e) {
      if (stop_.load()) {
        break;
      }
      if (metrics) {
        metrics->Counter("temporal_nexus_task_failed", 1, nexus_tags);
      }
      logger_->Error("nexus poll loop error", {log::F("error", e.what())});
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

}  // namespace temporal::internal
