#include <temporal/worker/worker.h>

#include <chrono>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

#include "internal/worker_impl.h"

namespace temporal::worker {

Worker::Worker(const client::Client& client, std::string task_queue, WorkerOptions options)
    : impl_(std::make_unique<internal::WorkerImpl>(client.grpc(), client.data_converter(),
                                                   client.logger(), std::move(task_queue),
                                                   options)) {}

Worker::~Worker() = default;
// Defined here (not the header) because WorkerImpl is incomplete there.
Worker::Worker(Worker&&) noexcept = default;
Worker& Worker::operator=(Worker&&) noexcept = default;

void Worker::RegisterWorkflowFn(std::string name, WorkflowFn fn) {
  impl_->RegisterWorkflow(std::move(name), std::move(fn));
}

void Worker::RegisterActivityFn(std::string name, ActivityFn fn) {
  impl_->RegisterActivity(std::move(name), std::move(fn));
}

void Worker::Start() { impl_->Start(); }

void Worker::Run() { impl_->Run(); }

void Worker::Run(std::stop_token token) {
  impl_->Start();
  while (!token.stop_requested()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  impl_->Stop();
}

void Worker::Stop() { impl_->Stop(); }

void Worker::ReplayWorkflowHistory(const std::string& history_json) {
  impl_->ReplayWorkflowHistory(history_json);
}

long Worker::cache_hits() const { return impl_->cache_hits(); }

long Worker::replays() const { return impl_->replays(); }

}  // namespace temporal::worker
