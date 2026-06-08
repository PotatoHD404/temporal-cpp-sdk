#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <temporal/common/options.h>
#include <temporal/converter/data_converter.h>
#include <temporal/log/logger.h>

#include "internal/activity_task_handler.h"
#include "internal/workflow_task_handler.h"

namespace temporal::internal {

class GrpcClient;

// Owns the poller threads and the two task handlers. One worker serves a single
// task queue.
class WorkerImpl {
 public:
  WorkerImpl(std::shared_ptr<GrpcClient> grpc, std::shared_ptr<DataConverter> converter,
             std::shared_ptr<log::Logger> logger, std::string task_queue, WorkerOptions options);
  ~WorkerImpl();
  WorkerImpl(const WorkerImpl&) = delete;
  WorkerImpl& operator=(const WorkerImpl&) = delete;
  WorkerImpl(WorkerImpl&&) = delete;
  WorkerImpl& operator=(WorkerImpl&&) = delete;

  void RegisterWorkflow(std::string name, worker::WorkflowFn fn);
  void RegisterActivity(std::string name, worker::ActivityFn fn);

  void Start();
  void Run();
  void Stop();

 private:
  void WorkflowPollLoop();
  void ActivityPollLoop();

  std::shared_ptr<GrpcClient> grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string task_queue_;
  WorkerOptions options_;
  WorkflowTaskHandler workflow_handler_;
  ActivityTaskHandler activity_handler_;
  std::atomic<bool> stop_{false};
  std::atomic<bool> started_{false};
  std::vector<std::thread> threads_;
};

}  // namespace temporal::internal
