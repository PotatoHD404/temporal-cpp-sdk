#pragma once

#include <string>
#include <utility>

#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>
#include <temporal/internal/workflow_outbound.h>

namespace temporal::workflow {

// Receive side of a workflow signal channel, à la the Go SDK's
// `workflow.ReceiveChannel`. `Receive()` blocks (parks the workflow) until a
// signal is buffered; `ReceiveAsync()` is non-blocking. Buffered signals are
// reconstructed deterministically from history on every replay, so the Nth
// `Receive()` always returns the Nth signal sent to this name.
template <class T>
class ReceiveChannel {
 public:
  ReceiveChannel(std::string name, const DataConverter* converter, internal::WorkflowOutbound* env)
      : name_(std::move(name)), converter_(converter), env_(env) {}

  T Receive() {
    Payloads payloads;
    while (!env_->TryConsumeSignal(name_, payloads)) {
      env_->Park();  // suspend until a signal/event arrives (or teardown)
    }
    return Decode(payloads);
  }

  bool ReceiveAsync(T& out) {
    Payloads payloads;
    if (!env_->TryConsumeSignal(name_, payloads)) {
      return false;
    }
    out = Decode(payloads);
    return true;
  }

  // Whether a buffered signal is available to Receive() without blocking. Used by
  // Selector channel cases.
  bool HasPending() const { return env_->HasSignal(name_); }

 private:
  T Decode(const Payloads& payloads) const {
    return payloads.empty() ? T{} : converter_->template FromPayload<T>(payloads.at(0));
  }

  std::string name_;
  const DataConverter* converter_;
  internal::WorkflowOutbound* env_;
};

}  // namespace temporal::workflow
