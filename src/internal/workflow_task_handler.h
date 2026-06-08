#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "temporal/api/workflowservice/v1/request_response.pb.h"

#include <temporal/converter/data_converter.h>
#include <temporal/log/logger.h>
#include <temporal/worker/worker.h>

namespace temporal::internal {

namespace wsv = ::temporal::api::workflowservice::v1;

class GrpcClient;

// Drives workflow tasks. Uses a **sticky cache**: a running workflow's coroutine
// is kept alive between tasks, keyed by run id, and continuation tasks apply only
// the incremental history before resuming — no full re-replay. A task whose
// history does not continue the cached state (first task, or a sticky-cache miss)
// is replayed from full history and (re)cached. See docs/ARCHITECTURE.md.
class WorkflowTaskHandler {
 public:
  WorkflowTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                      std::shared_ptr<log::Logger> logger, std::string task_queue,
                      std::string sticky_queue,
                      WorkflowPanicPolicy panic_policy = WorkflowPanicPolicy::BlockWorkflow);

  void Register(std::string name, worker::WorkflowFn fn);
  bool has_workflows() const { return !workflows_.empty(); }

  void Handle(const wsv::PollWorkflowTaskQueueResponse& task);

  // Test/inspection counters: continuations served from the cache vs. full replays.
  long cache_hits() const { return cache_hits_.load(); }
  long replays() const { return replays_.load(); }

 private:
  GrpcClient* grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string task_queue_;
  std::string sticky_queue_;
  WorkflowPanicPolicy panic_policy_;
  std::unordered_map<std::string, worker::WorkflowFn> workflows_;

  std::mutex cache_mu_;
  std::unordered_map<std::string, std::shared_ptr<void>> cache_;  // run_id -> WorkflowRunner
  std::atomic<long> cache_hits_{0};
  std::atomic<long> replays_{0};
};

}  // namespace temporal::internal
