#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "temporal/api/history/v1/message.pb.h"
#include "temporal/api/workflowservice/v1/request_response.pb.h"

#include "internal/lru_cache.h"

#include <temporal/converter/data_converter.h>
#include <temporal/log/logger.h>
#include <temporal/worker/worker.h>

namespace temporal::internal {

namespace wsv = ::temporal::api::workflowservice::v1;
namespace hist = ::temporal::api::history::v1;

class GrpcClient;

// Resolves an activity type name to its registered function, so the workflow
// handler can run local activities inline. Wired by the worker from the activity
// task handler's registry.
using LocalActivityResolver = std::function<worker::ActivityFn(const std::string&)>;

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
                      WorkflowPanicPolicy panic_policy = WorkflowPanicPolicy::BlockWorkflow,
                      int max_cached_workflows = 0);

  void Register(std::string name, worker::WorkflowFn fn);
  bool has_workflows() const { return !workflows_.empty(); }

  // Provide the resolver used to run local activities inline (set by the worker).
  void SetLocalActivityResolver(LocalActivityResolver resolver) {
    local_activity_resolver_ = std::move(resolver);
  }

  void Handle(const wsv::PollWorkflowTaskQueueResponse& task);

  // Replay a recorded history against the registered workflow and return the
  // first non-determinism mismatch, or nullopt if the replay is consistent. Used
  // by the replay/test framework; makes no RPCs.
  std::optional<std::string> ReplayHistory(const hist::History& history);

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
  LocalActivityResolver local_activity_resolver_;

  std::mutex cache_mu_;
  LruCache<std::string, std::shared_ptr<void>> cache_;  // run_id -> WorkflowRunner (LRU-bounded)
  std::atomic<long> cache_hits_{0};
  std::atomic<long> replays_{0};
};

}  // namespace temporal::internal
