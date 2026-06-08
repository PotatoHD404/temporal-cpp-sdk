#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
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

// A registered query handler, already adapted to operate on payloads (the
// Context wraps the user's typed handler into this shape).
using QueryFn = std::function<Payloads(const Payloads& args)>;

// Thrown by ctx.ContinueAsNew(...) to restart the workflow as a fresh run with
// new input. Deliberately NOT derived from std::exception (so it isn't caught as
// a failure); the workflow task handler catches it to emit the command.
struct ContinueAsNewRequested {
  std::string workflow_type;
  Payloads input;
};

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

  virtual std::shared_ptr<FutureState> StartChildWorkflow(std::string_view workflow_type,
                                                          const Payloads& input,
                                                          const ChildWorkflowOptions& options) = 0;

  // Suspend the workflow until `state` is ready (cooperatively yields the
  // dispatcher coroutine; returns once ready, or unwinds on teardown).
  virtual void Block(const std::shared_ptr<FutureState>& state) = 0;

  // Suspend the workflow unconditionally until the next event/task (used by
  // signal channels and selectors when nothing is ready).
  virtual void Park() = 0;

  // Consume the next buffered signal for `name` into `out`, advancing a
  // deterministic per-name cursor; returns false if none remain this run.
  virtual bool TryConsumeSignal(std::string_view name, Payloads& out) = 0;

  // Whether a cancel has been requested for this workflow execution.
  virtual bool IsCancelRequested() const = 0;

  // Register a query handler (re-registered on every replay). Invoked against
  // the live, suspended workflow state when a query task arrives.
  virtual void RegisterQueryHandler(std::string name, QueryFn handler) = 0;

  // Register an update handler. Like a query, but the call is recorded
  // (accepted + completed) and may mutate workflow state.
  virtual void RegisterUpdateHandler(std::string name, QueryFn handler) = 0;

  // Register an optional read-only validator for an update. It runs before the
  // update is accepted; throwing rejects the update ephemerally (no history
  // entry), so the handler never runs and workflow state is untouched.
  virtual void RegisterUpdateValidator(std::string name, QueryFn validator) = 0;

  // SideEffect: advance the deterministic side-effect counter and return the
  // value recorded in history for this call, or nullopt if there is none yet
  // (the caller then runs the function and records the result via
  // RecordSideEffect). Lets a workflow capture a non-deterministic value once.
  virtual std::optional<Payload> ReplaySideEffect() = 0;
  virtual void RecordSideEffect(const Payload& value) = 0;

  // GetVersion: returns the version recorded in history for `change_id`, or
  // selects `max_supported` on first (live) execution and records a marker; on
  // replay of history that predates the call, returns kDefaultVersion (-1).
  virtual int GetVersion(const std::string& change_id, int min_supported, int max_supported) = 0;

  virtual const workflow::WorkflowInfo& Info() const = 0;
  virtual log::Logger& Logger() const = 0;
  virtual bool IsReplaying() const = 0;
};

}  // namespace internal
}  // namespace temporal
