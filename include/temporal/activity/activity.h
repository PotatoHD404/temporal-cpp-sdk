#pragma once

#include <chrono>
#include <functional>
#include <map>
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
  std::string task_token;  // opaque; pass to Client::CompleteActivity for async completion
  std::map<std::string, Payload> headers;  // context-propagation headers from the workflow
  int attempt = 1;
};

// The activity execution context. Unlike a workflow, an activity runs in real
// time with full access to I/O. User activity functions take it by reference as
// their first parameter.
class Context {
 public:
  // `heartbeat` reports to the server and returns whether the server has asked
  // this activity to cancel. `throttle_interval` rate-limits the ACTUAL server
  // reports (see RecordHeartbeat); 0 (the default) disables throttling so direct
  // constructions report on every call. The worker derives it from the activity's
  // heartbeat timeout.
  Context(ActivityInfo info, const DataConverter* converter,
          std::function<bool(const Payloads&)> heartbeat = {},
          std::chrono::steady_clock::duration throttle_interval = {},
          // Clock backing the heartbeat throttle. Defaults to the real steady clock;
          // tests inject a controllable clock to exercise throttling deterministically
          // without depending on real-time sleeps (and the scheduler's accuracy).
          std::function<std::chrono::steady_clock::time_point()> clock =
              std::chrono::steady_clock::now)
      : info_(std::move(info)),
        converter_(converter),
        heartbeat_(std::move(heartbeat)),
        throttle_interval_(throttle_interval),
        clock_(std::move(clock)) {}

  const ActivityInfo& GetInfo() const { return info_; }

  // Used by the worker's registration adapter; also available to user code.
  const DataConverter& data_converter() const { return *converter_; }

  // Report liveness (and optional progress details) to the server, resetting the
  // heartbeat timeout. Long-running activities should call this periodically; a
  // tight loop is fine because reports are throttled. We invoke the underlying
  // server call only when at least `throttle_interval_` has elapsed since the last
  // ACTUAL report (the first call always reports); otherwise we skip the round-trip
  // and keep the cached cancel state. The cached state is refreshed only on an
  // actual report. Mirrors the Go SDK's heartbeat throttling.
  template <class... Args>
  void RecordHeartbeat(const Args&... details) {
    if (!heartbeat_) {
      return;
    }
    const auto now = clock_();
    if (reported_ && now - last_report_ < throttle_interval_) {
      return;  // too soon; keep the cached cancel state
    }
    last_report_ = now;
    reported_ = true;
    cancel_requested_ = heartbeat_(converter_->ToPayloads(details...));
  }

  // True once the server has requested this activity cancel, as observed by the
  // most recent ACTUAL heartbeat report (cancellation is delivered through
  // heartbeats, so only a heartbeating activity can see it). Throttled calls do
  // not refresh this, so it may lag the server by up to one throttle interval —
  // acceptable, since the next un-throttled heartbeat will surface it. A
  // long-running activity should poll this and return promptly when it becomes true.
  bool IsCancelled() const { return cancel_requested_; }

  // Defer completion: the worker will NOT report a result when the function
  // returns. The activity stays open until someone calls Client::CompleteActivity
  // (or FailActivity) with this context's GetInfo().task_token. The function's
  // return value is ignored in that case. Mirrors Go's activity.ErrResultPending.
  void SetWillCompleteAsync() { will_complete_async_ = true; }
  bool WillCompleteAsync() const { return will_complete_async_; }

  // Defer completion and hand back the task token to finish it with later (via
  // Client::CompleteActivity / FailActivity). One call instead of
  // SetWillCompleteAsync() + GetInfo().task_token:
  //   const auto token = ctx.defer_completion();
  [[nodiscard]] std::string defer_completion() {
    will_complete_async_ = true;
    return info_.task_token;
  }

 private:
  ActivityInfo info_;
  const DataConverter* converter_;
  std::function<bool(const Payloads&)> heartbeat_;
  std::chrono::steady_clock::duration throttle_interval_{};  // 0 => report every call
  std::function<std::chrono::steady_clock::time_point()> clock_;  // throttle clock (injectable)
  std::chrono::steady_clock::time_point last_report_{};      // valid only once reported_
  bool reported_ = false;  // whether any actual report has happened (first call always reports)
  bool cancel_requested_ = false;
  bool will_complete_async_ = false;
};

}  // namespace activity
}  // namespace temporal
