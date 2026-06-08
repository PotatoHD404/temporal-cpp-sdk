#pragma once

#include <memory>
#include <type_traits>
#include <utility>

#include <temporal/common/errors.h>
#include <temporal/converter/data_converter.h>
#include <temporal/internal/workflow_outbound.h>

namespace temporal::workflow {

// Handle to the eventual result of a workflow operation (activity, timer).
// `Get()` blocks the workflow until the result is available — matching the Go
// SDK's `future.Get(ctx, &out)` ergonomics — by parking the workflow task when
// the value is not yet known and resuming on a later task once it is.
template <class T>
class Future {
 public:
  Future(std::shared_ptr<internal::FutureState> state, const DataConverter* converter,
         internal::WorkflowOutbound* env)
      : state_(std::move(state)), converter_(converter), env_(env) {}

  bool IsReady() const { return state_->ready; }

  T Get() {
    env_->Block(state_);  // throws internal::WorkflowBlocked if not yet ready
    if (state_->failed) {
      throw ActivityError(state_->failure_type, state_->failure_message);
    }
    if constexpr (std::is_void_v<T>) {
      return;
    } else {
      return converter_->template FromPayload<T>(state_->result.at(0));
    }
  }

 private:
  std::shared_ptr<internal::FutureState> state_;
  const DataConverter* converter_;
  internal::WorkflowOutbound* env_;
};

}  // namespace temporal::workflow
