#include "internal/worker_impl.h"

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

namespace temporal::internal {
namespace {

namespace enums = ::temporal::api::enums::v1;

// Set by SIGINT/SIGTERM so a blocking Run() can return. Process-global because
// signal handlers cannot carry context.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> g_interrupted{false};

void HandleSignal(int /*signum*/) { g_interrupted.store(true); }

// Conservative autoscaling backoff. Once a poller has seen `threshold` (or more)
// consecutive empty polls, it parks (sleeps a short, capped backoff before the
// next poll) instead of hot-spinning the long-poll — but only while at least
// `min_hot` pollers of this kind stay eager, so the queue is never left unpolled.
// `hot` tracks the eager count; the poller hands its slot back while parked and
// reclaims it on return. Returns immediately (no sleep) when not parking.
void AutoscaleBackoff(int empty_streak, int threshold, int min_hot, std::atomic<int>& hot,
                      const std::atomic<bool>& stop) {
  if (empty_streak < threshold) {
    return;
  }
  // Try to step down: succeed only if it leaves >= min_hot eager pollers.
  int cur = hot.load();
  while (cur > min_hot) {
    if (hot.compare_exchange_weak(cur, cur - 1)) {
      // Parked: back off in short slices so stop_ is observed promptly.
      for (int i = 0; i < 10 && !stop.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      hot.fetch_add(1);  // reclaim eager slot before polling again.
      return;
    }
  }
  // Could not park (would drop below min_hot): stay eager, no sleep.
}

}  // namespace

WorkerImpl::WorkerImpl(std::shared_ptr<GrpcClient> grpc, std::shared_ptr<DataConverter> converter,
                       std::shared_ptr<log::Logger> logger, std::string task_queue,
                       WorkerOptions options)
    : grpc_(std::move(grpc)),
      converter_(std::move(converter)),
      logger_(std::move(logger)),
      task_queue_(std::move(task_queue)),
      sticky_queue_("temporal-cpp-sticky-" + NewUuid()),
      session_queue_(task_queue_ + "-session-" + NewUuid()),
      options_(options),
      workflow_handler_(grpc_.get(), converter_, logger_, task_queue_, sticky_queue_,
                        options.panic_policy, options.max_cached_workflows),
      activity_handler_(grpc_.get(), converter_, logger_, task_queue_),
      workflow_gate_(options.max_concurrent_workflow_task_executions),
      activity_gate_(options.max_concurrent_activity_executions),
      session_gate_(options.max_concurrent_sessions),
      activity_rate_limiter_(options.max_activities_per_second) {
  // Let workflows run registered activities inline as local activities (the
  // workflow handler resolves activity functions from the activity registry).
  workflow_handler_.SetLocalActivityResolver(
      [this](const std::string& type) { return activity_handler_.Lookup(type); });
  // Activity-inbound interceptors run around every activity execution.
  activity_handler_.SetInterceptors(options_.interceptors);
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

void WorkerImpl::Start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    return;
  }
  const int wf_pollers = options_.workflow_task_pollers > 0 ? options_.workflow_task_pollers : 1;
  const int act_pollers = options_.activity_task_pollers > 0 ? options_.activity_task_pollers : 1;
  if (workflow_handler_.has_workflows()) {
    wf_hot_pollers_.store(2 * wf_pollers);  // normal + sticky loop per poller
    for (int i = 0; i < wf_pollers; ++i) {
      threads_.emplace_back([this] { WorkflowPollLoop(/*sticky=*/false); });
      threads_.emplace_back([this] { WorkflowPollLoop(/*sticky=*/true); });
    }
  }
  if (activity_handler_.has_activities()) {
    act_hot_pollers_.store(act_pollers + (options_.enable_sessions ? 1 : 0));
    for (int i = 0; i < act_pollers; ++i) {
      threads_.emplace_back([this] { ActivityPollLoop(/*session=*/false); });
    }
    if (options_.enable_sessions) {
      threads_.emplace_back([this] { ActivityPollLoop(/*session=*/true); });
    }
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

  // Phase 2: wait for in-flight Handle() calls to finish, bounded by the timeout.
  // Pollers blocked in a long-poll RPC are unaffected here; they return on the
  // server-side poll timeout and then observe stop_.
  if (options_.graceful_shutdown_timeout.count() > 0) {
    const auto deadline = std::chrono::steady_clock::now() + options_.graceful_shutdown_timeout;
    const bool wf_drained = workflow_gate_.Drain(deadline);
    const bool act_drained = activity_gate_.Drain(deadline);
    const bool sess_drained = session_gate_.Drain(deadline);
    if (!wf_drained || !act_drained || !sess_drained) {
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

void WorkerImpl::WorkflowPollLoop(bool sticky) {
  auto* metrics = options_.metrics_handler.get();
  int empty_streak = 0;
  while (!stop_.load()) {
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
      if (stop_.load()) {
        break;
      }
      // An empty token is a poll timeout (no task); a non-empty one is a hit.
      if (resp.task_token().empty()) {
        ++empty_streak;
        if (metrics) {
          metrics->Counter("temporal_workflow_poll_timeout", 1, {});
        }
        if (options_.enable_poller_autoscaling) {
          AutoscaleBackoff(empty_streak, options_.autoscaling_idle_polls_before_park,
                           options_.min_concurrent_pollers, wf_hot_pollers_, stop_);
        }
        continue;
      }
      empty_streak = 0;
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
      }
      const auto start = std::chrono::steady_clock::now();
      workflow_handler_.Handle(resp);
      if (metrics) {
        metrics->Timer("temporal_workflow_task_execution_latency",
                       std::chrono::steady_clock::now() - start, {});
        metrics->Counter("temporal_workflow_task_handled", 1, {});
      }
    } catch (const std::exception& e) {
      if (stop_.load()) {
        break;
      }
      logger_->Error("workflow poll loop error", {log::F("error", e.what())});
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

void WorkerImpl::ActivityPollLoop(bool session) {
  auto* metrics = options_.metrics_handler.get();
  // Session activities draw permits from a dedicated gate so they can be capped
  // independently of normal activity executions.
  ConcurrencyGate& gate = session ? session_gate_ : activity_gate_;
  int empty_streak = 0;
  while (!stop_.load()) {
    try {
      wsv::PollActivityTaskQueueRequest req;
      req.set_namespace_(grpc_->ns());
      req.mutable_task_queue()->set_name(session ? session_queue_ : task_queue_);
      req.mutable_task_queue()->set_kind(enums::TASK_QUEUE_KIND_NORMAL);
      req.set_identity(grpc_->identity());
      const auto resp = grpc_->PollActivityTaskQueue(req);
      if (stop_.load()) {
        break;
      }
      if (resp.task_token().empty()) {
        ++empty_streak;
        if (metrics) {
          metrics->Counter("temporal_activity_poll_timeout", 1, {});
        }
        if (options_.enable_poller_autoscaling) {
          AutoscaleBackoff(empty_streak, options_.autoscaling_idle_polls_before_park,
                           options_.min_concurrent_pollers, act_hot_pollers_, stop_);
        }
        continue;
      }
      empty_streak = 0;
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
      }
      const auto start = std::chrono::steady_clock::now();
      activity_handler_.Handle(resp);
      if (metrics) {
        metrics->Timer("temporal_activity_task_execution_latency",
                       std::chrono::steady_clock::now() - start, {});
        metrics->Counter("temporal_activity_task_handled", 1, {});
      }
    } catch (const std::exception& e) {
      if (stop_.load()) {
        break;
      }
      logger_->Error("activity poll loop error", {log::F("error", e.what())});
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

}  // namespace temporal::internal
