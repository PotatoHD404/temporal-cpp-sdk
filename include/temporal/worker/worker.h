#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include <temporal/activity/activity.h>
#include <temporal/client/client.h>
#include <temporal/common/options.h>
#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>
#include <temporal/internal/callable_traits.h>
#include <temporal/typed_handles.h>
#include <temporal/workflow/context.h>

namespace temporal {

namespace internal {
class WorkerImpl;
}

namespace worker {

// Type-erased registered functions, operating on payloads. The templated
// Register* methods adapt ordinary user functions into these.
using WorkflowFn = std::function<Payloads(workflow::Context&, const Payloads&)>;
using ActivityFn = std::function<Payloads(activity::Context&, const Payloads&)>;

// A worker polls a task queue and dispatches workflow/activity tasks to the
// registered functions. Mirrors the Go SDK's `worker.Worker`.
class Worker {
 public:
  Worker(const client::Client& client, std::string task_queue, WorkerOptions options = {});
  ~Worker();
  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;
  Worker(Worker&&) = delete;
  Worker& operator=(Worker&&) = delete;

  // Register a workflow: `Ret Fn(workflow::Context&, Args...)`.
  template <class Fn>
  void RegisterWorkflow(std::string name, Fn fn) {
    using Sig = internal::fn_sig<std::decay_t<Fn>>;
    static_assert(std::tuple_size_v<typename Sig::args> >= 1,
                  "workflow function must take workflow::Context& as its first parameter");
    RegisterWorkflowFn(std::move(name),
                       MakeWorkflowFn<typename Sig::ret, typename Sig::args>(std::move(fn)));
  }

  // Register an activity: `Ret Fn(activity::Context&, Args...)`.
  template <class Fn>
  void RegisterActivity(std::string name, Fn fn) {
    using Sig = internal::fn_sig<std::decay_t<Fn>>;
    static_assert(std::tuple_size_v<typename Sig::args> >= 1,
                  "activity function must take activity::Context& as its first parameter");
    RegisterActivityFn(std::move(name),
                       MakeActivityFn<typename Sig::ret, typename Sig::args>(std::move(fn)));
  }

  // Register an activity from a typed handle (TEMPORAL_ACTIVITY): the type name and
  // signature come from the handle, so they can't drift from the call site.
  template <auto Fn>
  void Register(const ActivityRef<Fn>& ref) {
    RegisterActivity(std::string(ref.name), Fn);
  }

  void Start();  // start pollers (non-blocking)
  void Run();    // start pollers and block until SIGINT or Stop()
  void Stop();   // signal pollers to stop and join

  // Replay a recorded workflow history (Temporal's JSON, e.g. from
  // `temporal workflow show -o json` or WorkflowHandle::FetchHistoryJson) against
  // this worker's registered workflow code, WITHOUT contacting a server. Throws
  // if the workflow's replayed commands diverge from the recorded history — the
  // way to catch non-deterministic changes to a workflow in a unit test.
  void ReplayWorkflowHistory(const std::string& history_json);

  // Observability: workflow tasks served as sticky-cache continuations vs. full
  // replays since the worker started.
  [[nodiscard]] long cache_hits() const;
  [[nodiscard]] long replays() const;

 private:
  void RegisterWorkflowFn(std::string name, WorkflowFn fn);
  void RegisterActivityFn(std::string name, ActivityFn fn);

  template <class Ret, class AllArgs, class Fn>
  static WorkflowFn MakeWorkflowFn(Fn fn) {
    return [fn = std::move(fn)](workflow::Context& ctx, const Payloads& in) -> Payloads {
      using UserArgs = typename internal::tuple_tail<AllArgs>::type;
      const DataConverter& dc = ctx.data_converter();
      auto args = internal::DecodeArgs<UserArgs>(
          dc, in, std::make_index_sequence<std::tuple_size_v<UserArgs>>{});
      if constexpr (std::is_void_v<Ret>) {
        std::apply([&](auto&... a) { fn(ctx, a...); }, args);
        return Payloads{};
      } else {
        Ret result = std::apply([&](auto&... a) { return fn(ctx, a...); }, args);
        return Payloads{dc.ToPayload(result)};
      }
    };
  }

  template <class Ret, class AllArgs, class Fn>
  static ActivityFn MakeActivityFn(Fn fn) {
    return [fn = std::move(fn)](activity::Context& ctx, const Payloads& in) -> Payloads {
      using UserArgs = typename internal::tuple_tail<AllArgs>::type;
      const DataConverter& dc = ctx.data_converter();
      auto args = internal::DecodeArgs<UserArgs>(
          dc, in, std::make_index_sequence<std::tuple_size_v<UserArgs>>{});
      if constexpr (std::is_void_v<Ret>) {
        std::apply([&](auto&... a) { fn(ctx, a...); }, args);
        return Payloads{};
      } else {
        Ret result = std::apply([&](auto&... a) { return fn(ctx, a...); }, args);
        return Payloads{dc.ToPayload(result)};
      }
    };
  }

  std::unique_ptr<internal::WorkerImpl> impl_;
};

}  // namespace worker
}  // namespace temporal
