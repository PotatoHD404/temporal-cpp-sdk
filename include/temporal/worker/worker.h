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

namespace detail {

// Extract the return type and (decayed) parameter types of a callable.
template <class T>
struct fn_sig : fn_sig<decltype(&std::decay_t<T>::operator())> {};
template <class R, class... A>
struct fn_sig<R (*)(A...)> {
  using ret = R;
  using args = std::tuple<std::decay_t<A>...>;
};
template <class C, class R, class... A>
struct fn_sig<R (C::*)(A...) const> {
  using ret = R;
  using args = std::tuple<std::decay_t<A>...>;
};
template <class C, class R, class... A>
struct fn_sig<R (C::*)(A...)> {
  using ret = R;
  using args = std::tuple<std::decay_t<A>...>;
};

// Drop the first tuple element (the Context& parameter).
template <class Tuple>
struct tuple_tail;
template <class Head, class... Tail>
struct tuple_tail<std::tuple<Head, Tail...>> {
  using type = std::tuple<Tail...>;
};

// Decode payloads[0..N-1] into a tuple of user argument types.
template <class Tuple, std::size_t... I>
Tuple DecodeArgs(const DataConverter& dc, const Payloads& in, std::index_sequence<I...>) {
  return Tuple{dc.FromPayload<std::tuple_element_t<I, Tuple>>(in.at(I))...};
}

}  // namespace detail

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
    using Sig = detail::fn_sig<std::decay_t<Fn>>;
    static_assert(std::tuple_size_v<typename Sig::args> >= 1,
                  "workflow function must take workflow::Context& as its first parameter");
    RegisterWorkflowFn(std::move(name),
                       MakeWorkflowFn<typename Sig::ret, typename Sig::args>(std::move(fn)));
  }

  // Register an activity: `Ret Fn(activity::Context&, Args...)`.
  template <class Fn>
  void RegisterActivity(std::string name, Fn fn) {
    using Sig = detail::fn_sig<std::decay_t<Fn>>;
    static_assert(std::tuple_size_v<typename Sig::args> >= 1,
                  "activity function must take activity::Context& as its first parameter");
    RegisterActivityFn(std::move(name),
                       MakeActivityFn<typename Sig::ret, typename Sig::args>(std::move(fn)));
  }

  void Start();  // start pollers (non-blocking)
  void Run();    // start pollers and block until SIGINT or Stop()
  void Stop();   // signal pollers to stop and join

 private:
  void RegisterWorkflowFn(std::string name, WorkflowFn fn);
  void RegisterActivityFn(std::string name, ActivityFn fn);

  template <class Ret, class AllArgs, class Fn>
  static WorkflowFn MakeWorkflowFn(Fn fn) {
    return [fn = std::move(fn)](workflow::Context& ctx, const Payloads& in) -> Payloads {
      using UserArgs = typename detail::tuple_tail<AllArgs>::type;
      const DataConverter& dc = ctx.data_converter();
      auto args = detail::DecodeArgs<UserArgs>(
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
      using UserArgs = typename detail::tuple_tail<AllArgs>::type;
      const DataConverter& dc = ctx.data_converter();
      auto args = detail::DecodeArgs<UserArgs>(
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
