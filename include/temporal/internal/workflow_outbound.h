#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <string_view>

#include <temporal/common/options.h>
#include <temporal/common/payload.h>

// Internal seam between the public workflow API (Future, Context) and the
// workflow task handler that drives execution. Kept free of protobuf/engine
// types so it can live in a public header without leaking the implementation.
namespace temporal {

namespace log {
class Logger;
}
namespace workflow {
struct WorkflowInfo;
}

namespace internal {

// Result slot for an in-flight workflow operation (activity or timer). Resolved
// by the workflow task handler when the matching history event is replayed.
struct FutureState {
  bool ready = false;
  bool failed = false;
  Payloads result;  // success payloads (activity: 1, timer: 0)
  std::string failure_type;
  std::string failure_message;
};

// Thrown to suspend ("park") a workflow that awaits an unresolved future.
// Deliberately NOT derived from std::exception so a stray `catch (const
// std::exception&)` in user workflow code cannot swallow it. The workflow task
// handler catches it to finalize the current task. See docs/ARCHITECTURE.md.
struct WorkflowBlocked {};

// Outbound surface a running workflow uses to emit commands and block. The
// handler implements it; methods are non-templated to keep proto out of headers.
class WorkflowOutbound {
 public:
  WorkflowOutbound() = default;
  virtual ~WorkflowOutbound() = default;
  WorkflowOutbound(const WorkflowOutbound&) = delete;
  WorkflowOutbound& operator=(const WorkflowOutbound&) = delete;
  WorkflowOutbound(WorkflowOutbound&&) = delete;
  WorkflowOutbound& operator=(WorkflowOutbound&&) = delete;

  virtual std::shared_ptr<FutureState> ScheduleActivity(std::string_view activity_type,
                                                        const Payloads& input,
                                                        const ActivityOptions& options) = 0;

  virtual std::shared_ptr<FutureState> StartTimer(std::chrono::nanoseconds duration) = 0;

  // Returns if `state` is ready; otherwise throws WorkflowBlocked to suspend.
  virtual void Block(const std::shared_ptr<FutureState>& state) = 0;

  virtual const workflow::WorkflowInfo& Info() const = 0;
  virtual log::Logger& Logger() const = 0;
  virtual bool IsReplaying() const = 0;
};

}  // namespace internal
}  // namespace temporal
