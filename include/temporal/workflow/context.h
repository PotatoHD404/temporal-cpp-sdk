#pragma once

#include <chrono>
#include <string>
#include <string_view>

#include <temporal/common/options.h>
#include <temporal/converter/data_converter.h>
#include <temporal/internal/workflow_outbound.h>
#include <temporal/log/logger.h>
#include <temporal/workflow/future.h>

namespace temporal::workflow {

// Immutable metadata about the currently executing workflow.
struct WorkflowInfo {
  std::string workflow_id;
  std::string run_id;
  std::string workflow_type;
  std::string task_queue;
  std::string ns;
};

// The deterministic workflow context. Like the Go SDK's `workflow.Context`, all
// orchestration (activities, timers) goes through this object so the engine can
// record commands and replay history. User workflow functions take it by
// reference as their first parameter.
class Context {
 public:
  Context(internal::WorkflowOutbound* env, const DataConverter* converter)
      : env_(env), converter_(converter) {}

  // Schedule an activity by type name; returns a Future for its typed result.
  template <class R, class... Args>
  Future<R> ExecuteActivity(const ActivityOptions& options, std::string_view activity_type,
                            const Args&... args) {
    Payloads input = converter_->ToPayloads(args...);
    return Future<R>(env_->ScheduleActivity(activity_type, input, options), converter_, env_);
  }

  // Start a timer; returns a Future that resolves when it fires.
  Future<void> NewTimer(std::chrono::nanoseconds duration) {
    return Future<void>(env_->StartTimer(duration), converter_, env_);
  }

  // Block the workflow for `duration` (NewTimer + Get).
  void Sleep(std::chrono::nanoseconds duration) { NewTimer(duration).Get(); }

  const WorkflowInfo& GetInfo() const { return env_->Info(); }
  log::Logger& GetLogger() const { return env_->Logger(); }
  bool IsReplaying() const { return env_->IsReplaying(); }

  // Used by the worker's registration adapter; also available to user code.
  const DataConverter& data_converter() const { return *converter_; }

 private:
  internal::WorkflowOutbound* env_;
  const DataConverter* converter_;
};

}  // namespace temporal::workflow
