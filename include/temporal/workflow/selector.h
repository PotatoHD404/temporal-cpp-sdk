#pragma once

#include <functional>
#include <utility>
#include <vector>

#include <temporal/internal/workflow_outbound.h>
#include <temporal/workflow/context.h>
#include <temporal/workflow/future.h>

namespace temporal::workflow {

// Waits on multiple futures, proceeding when any one is ready — the C++ analogue
// of the Go SDK's `workflow.Selector`. The canonical use is "activity OR timeout".
// `Select()` runs the matching case's handler; add a default to make it
// non-blocking. (Only futures are supported for now; channel cases are roadmap.)
class Selector {
 public:
  explicit Selector(Context& ctx) : env_(ctx.env_) {}

  template <class T>
  Selector& AddFuture(Future<T> future, std::function<void(T)> handler) {
    cases_.push_back(Case{[future]() { return future.IsReady(); },
                          [future, handler = std::move(handler)]() mutable {
                            handler(future.Get());
                          }});
    return *this;
  }

  // Overload for void futures (e.g. timers).
  Selector& AddFuture(Future<void> future, std::function<void()> handler) {
    cases_.push_back(Case{[future]() { return future.IsReady(); },
                          [future, handler = std::move(handler)]() mutable {
                            future.Get();
                            handler();
                          }});
    return *this;
  }

  Selector& AddDefault(std::function<void()> handler) {
    default_ = std::move(handler);
    return *this;
  }

  // Block until a case is ready (or run the default), then execute its handler.
  void Select() {
    for (;;) {
      for (auto& c : cases_) {
        if (c.ready()) {
          c.exec();
          return;
        }
      }
      if (default_) {
        default_();
        return;
      }
      env_->Park();  // suspend until a future resolves (or teardown)
    }
  }

 private:
  struct Case {
    std::function<bool()> ready;
    std::function<void()> exec;
  };

  internal::WorkflowOutbound* env_;
  std::vector<Case> cases_;
  std::function<void()> default_;
};

}  // namespace temporal::workflow
