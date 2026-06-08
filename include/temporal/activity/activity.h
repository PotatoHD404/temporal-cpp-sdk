#pragma once

#include <functional>
#include <string>
#include <utility>

#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>

namespace temporal {

namespace activity {

// Metadata about the activity task currently executing.
struct ActivityInfo {
  std::string activity_id;
  std::string activity_type;
  std::string workflow_id;
  std::string run_id;
  std::string task_queue;
  int attempt = 1;
};

// The activity execution context. Unlike a workflow, an activity runs in real
// time with full access to I/O. User activity functions take it by reference as
// their first parameter.
class Context {
 public:
  // `heartbeat` reports to the server and returns whether the server has asked
  // this activity to cancel.
  Context(ActivityInfo info, const DataConverter* converter,
          std::function<bool(const Payloads&)> heartbeat = {})
      : info_(std::move(info)), converter_(converter), heartbeat_(std::move(heartbeat)) {}

  const ActivityInfo& GetInfo() const { return info_; }

  // Used by the worker's registration adapter; also available to user code.
  const DataConverter& data_converter() const { return *converter_; }

  // Report liveness (and optional progress details) to the server, resetting the
  // heartbeat timeout. Long-running activities should call this periodically.
  template <class... Args>
  void RecordHeartbeat(const Args&... details) {
    if (heartbeat_) {
      cancel_requested_ = heartbeat_(converter_->ToPayloads(details...));
    }
  }

  // True once the server has requested this activity cancel, as observed by the
  // most recent RecordHeartbeat (cancellation is delivered through heartbeats, so
  // only a heartbeating activity can see it). A long-running activity should poll
  // this and return promptly when it becomes true.
  bool IsCancelled() const { return cancel_requested_; }

 private:
  ActivityInfo info_;
  const DataConverter* converter_;
  std::function<bool(const Payloads&)> heartbeat_;
  bool cancel_requested_ = false;
};

}  // namespace activity
}  // namespace temporal
