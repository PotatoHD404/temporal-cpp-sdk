#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "temporal/api/workflowservice/v1/request_response.pb.h"

#include <temporal/converter/data_converter.h>
#include <temporal/log/logger.h>
#include <temporal/worker/worker.h>

namespace temporal::internal {

namespace wsv = ::temporal::api::workflowservice::v1;

class GrpcClient;

// Drives a workflow task to completion. Runs non-sticky: the server delivers the
// full history each task, and the workflow function is re-executed from the start
// against it. Operations whose results are already in history resolve
// immediately; the first unresolved `Future::Get()` parks the workflow (throws
// WorkflowBlocked) and the task is finalized with whatever commands were emitted.
// See docs/ARCHITECTURE.md for the determinism model and its limits.
class WorkflowTaskHandler {
 public:
  WorkflowTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                      std::shared_ptr<log::Logger> logger, std::string task_queue);

  void Register(std::string name, worker::WorkflowFn fn);
  bool has_workflows() const { return !workflows_.empty(); }

  void Handle(const wsv::PollWorkflowTaskQueueResponse& task);

 private:
  GrpcClient* grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string task_queue_;
  std::unordered_map<std::string, worker::WorkflowFn> workflows_;
};

}  // namespace temporal::internal
