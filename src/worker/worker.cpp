#include <temporal/worker/worker.h>

#include <memory>
#include <string>
#include <utility>

#include "internal/worker_impl.h"

namespace temporal::worker {

Worker::Worker(const client::Client& client, std::string task_queue, WorkerOptions options)
    : impl_(std::make_unique<internal::WorkerImpl>(client.grpc(), client.data_converter(),
                                                   client.logger(), std::move(task_queue),
                                                   options)) {}

Worker::~Worker() = default;

void Worker::RegisterWorkflowFn(std::string name, WorkflowFn fn) {
  impl_->RegisterWorkflow(std::move(name), std::move(fn));
}

void Worker::RegisterActivityFn(std::string name, ActivityFn fn) {
  impl_->RegisterActivity(std::move(name), std::move(fn));
}

void Worker::Start() { impl_->Start(); }

void Worker::Run() { impl_->Run(); }

void Worker::Stop() { impl_->Stop(); }

}  // namespace temporal::worker
