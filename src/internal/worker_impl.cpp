#include "internal/worker_impl.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <thread>
#include <utility>

#include "temporal/api/enums/v1/task_queue.pb.h"

#include "internal/grpc_client.h"
#include "internal/proto_util.h"

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
      sticky_queue_("temporal-cpp-sticky-" + NewUuid()),
      options_(options),
      workflow_handler_(grpc_.get(), converter_, logger_, task_queue_, sticky_queue_),
      activity_handler_(grpc_.get(), converter_, logger_, task_queue_) {}

WorkerImpl::~WorkerImpl() { Stop(); }

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
    for (int i = 0; i < wf_pollers; ++i) {
      threads_.emplace_back([this] { WorkflowPollLoop(/*sticky=*/false); });
      threads_.emplace_back([this] { WorkflowPollLoop(/*sticky=*/true); });
    }
  }
  if (activity_handler_.has_activities()) {
    for (int i = 0; i < act_pollers; ++i) {
      threads_.emplace_back([this] { ActivityPollLoop(); });
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
  stop_.store(true);
  for (auto& t : threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  threads_.clear();
}

void WorkerImpl::WorkflowPollLoop(bool sticky) {
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
      if (resp.task_token().empty()) {
        continue;
      }
      workflow_handler_.Handle(resp);
    } catch (const std::exception& e) {
      if (stop_.load()) {
        break;
      }
      logger_->Error("workflow poll loop error", {log::F("error", e.what())});
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

void WorkerImpl::ActivityPollLoop() {
  while (!stop_.load()) {
    try {
      wsv::PollActivityTaskQueueRequest req;
      req.set_namespace_(grpc_->ns());
      req.mutable_task_queue()->set_name(task_queue_);
      req.mutable_task_queue()->set_kind(enums::TASK_QUEUE_KIND_NORMAL);
      req.set_identity(grpc_->identity());
      const auto resp = grpc_->PollActivityTaskQueue(req);
      if (stop_.load()) {
        break;
      }
      if (resp.task_token().empty()) {
        continue;
      }
      activity_handler_.Handle(resp);
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
