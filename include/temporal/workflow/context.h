#pragma once

#include <chrono>
#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include <temporal/common/options.h>
#include <temporal/common/session.h>
#include <temporal/converter/data_converter.h>
#include <temporal/internal/callable_traits.h>
#include <temporal/typed_handles.h>
#include <temporal/internal/workflow_outbound.h>
#include <temporal/log/logger.h>
#include <temporal/workflow/channel.h>
#include <temporal/workflow/coro.h>
#include <temporal/workflow/future.h>

namespace temporal::workflow {

class Selector;

// Returned by Context::GetVersion when replaying history recorded before the
// GetVersion call was added (mirrors the Go SDK's workflow.DefaultVersion).
inline constexpr int kDefaultVersion = -1;

// Immutable metadata about the currently executing workflow.
struct WorkflowInfo {
  std::string workflow_id;
  std::string run_id;
  std::string workflow_type;
  std::string task_queue;
  std::string ns;
  std::map<std::string, Payload> headers;  // context-propagation headers
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

  // Schedule an activity from a typed handle (TEMPORAL_ACTIVITY): the type name and
  // result type come from the handle, so no string and no explicit `<R>` are needed
  // and a wrong argument type is a compile error.
  template <auto Fn, class... Args>
  Future<typename ActivityRef<Fn>::result_type> ExecuteActivity(const ActivityOptions& options,
                                                                const ActivityRef<Fn>& ref,
                                                                const Args&... args) {
    return ExecuteActivity<typename ActivityRef<Fn>::result_type>(options, ref.name, args...);
  }

  // Start a timer; returns a Future that resolves when it fires.
  Future<void> NewTimer(std::chrono::nanoseconds duration) {
    return Future<void>(env_->StartTimer(duration), converter_, env_);
  }

  // Start a child workflow by type name; returns a Future for its typed result.
  template <class R, class... Args>
  Future<R> ExecuteChildWorkflow(const ChildWorkflowOptions& options, std::string_view workflow_type,
                                 const Args&... args) {
    Payloads input = converter_->ToPayloads(args...);
    return Future<R>(env_->StartChildWorkflow(workflow_type, input, options), converter_, env_);
  }

  // Call a Nexus operation on `endpoint`'s `service`/`operation`; returns a Future
  // for its typed result. Unlike an activity, a Nexus operation's input and result
  // are each a SINGLE value (one Payload), so `input` is a single argument encoded
  // to one Payload and `R` is decoded from one Payload. `schedule_to_close` bounds
  // the whole call (0 = the server default). Mirrors the Go SDK's
  // `workflow.ExecuteNexusOperation`.
  template <class R, class Arg>
  Future<R> ExecuteNexusOperation(const std::string& endpoint, const std::string& service,
                                  const std::string& operation, const Arg& input,
                                  std::chrono::nanoseconds schedule_to_close = {}) {
    Payload encoded = converter_->ToPayload(input);
    return Future<R>(
        env_->ScheduleNexusOperation(endpoint, service, operation, encoded, schedule_to_close),
        converter_, env_);
  }

  // Create a host-pinned worker session. Runs a built-in creation activity on the
  // base task queue; the session-enabled worker that handles it returns its
  // host-unique session queue and reserves a session slot (bounded by the
  // worker's max_concurrent_sessions). Schedule subsequent activities with
  // ActivityOptions.task_queue == the returned task_queue to pin them to that one
  // host — e.g. so a sequence of activities can share host-local files. Pair with
  // CompleteSession to release the slot. Throws if no worker accepts the creation
  // activity within creation_timeout (all at capacity / none session-enabled).
  SessionInfo CreateSession(const SessionOptions& options = {}) {
    ActivityOptions opts;
    opts.schedule_to_close_timeout = options.creation_timeout;
    opts.start_to_close_timeout = options.creation_timeout;
    const std::string queue =
        ExecuteActivity<std::string>(opts, kSessionCreationActivityType).Get();
    return SessionInfo{queue, queue};
  }

  // End a host-pinned session: runs a built-in completion activity on the
  // session's host queue, releasing that worker's session slot. Best-effort —
  // swallows failures (the slot is also freed if the worker restarts).
  void CompleteSession(const SessionInfo& session) {
    if (session.task_queue.empty()) {
      return;
    }
    ActivityOptions opts;
    opts.task_queue = session.task_queue;  // route to the owning host
    opts.start_to_close_timeout = std::chrono::seconds(10);
    try {
      ExecuteActivity<void>(opts, kSessionCompletionActivityType).Get();
    } catch (...) {  // NOLINT(bugprone-empty-catch): completion is best-effort
    }
  }

  // Block the workflow for `duration` (NewTimer + Get).
  void Sleep(std::chrono::nanoseconds duration) { NewTimer(duration).Get(); }

  // Restart the workflow as a fresh run with new input. Terminal: never returns.
  template <class... Args>
  [[noreturn]] void ContinueAsNew(std::string_view workflow_type, const Args&... args) {
    throw internal::ContinueAsNewRequested{std::string(workflow_type),
                                           converter_->ToPayloads(args...)};
  }

  // Receive signals sent to this workflow under `name`. Mirrors the Go SDK's
  // `workflow.GetSignalChannel`.
  template <class T>
  ReceiveChannel<T> GetSignalChannel(std::string name) {
    return ReceiveChannel<T>(std::move(name), converter_, env_);
  }

  // Typed-handle overload: the channel's payload type is deduced from the SignalRef.
  template <class T>
  ReceiveChannel<T> GetSignalChannel(const SignalRef<T>& ref) {
    return GetSignalChannel<T>(std::string(ref.name));
  }

  // True once a cancel has been requested for this workflow execution. The
  // workflow decides how to react (finish, clean up, etc.).
  bool IsCancelled() const { return env_->IsCancelRequested(); }

  // A future that completes when this workflow is cancelled. Use it as a Selector
  // case to race work against cancellation, then clean up (e.g. cancel a timer):
  //   Selector s(ctx);
  //   s.AddFuture(work, ...);
  //   s.AddFuture(ctx.AwaitCancellation(), [&]{ work.Cancel(); });
  //   s.Select();
  Future<void> AwaitCancellation() {
    return Future<void>(env_->AwaitCancellation(), converter_, env_);
  }

  // Request cancellation of another running workflow by id (fire-and-forget).
  // Mirrors `workflow.RequestCancelExternalWorkflow`.
  void CancelExternalWorkflow(const std::string& workflow_id) {
    env_->CancelExternalWorkflow(workflow_id);
  }

  // Signal another running workflow by id (fire-and-forget), encoding `args`
  // through the data converter. Mirrors `workflow.SignalExternalWorkflow`.
  template <class... Args>
  void SignalExternalWorkflow(const std::string& workflow_id, std::string_view signal_name,
                              const Args&... args) {
    env_->SignalExternalWorkflow(workflow_id, signal_name, converter_->ToPayloads(args...));
  }

  // Upsert indexed search attributes on this workflow. Build typed values with
  // the `temporal::sa::` helpers. Mirrors `workflow.UpsertSearchAttributes`.
  void UpsertSearchAttributes(const std::map<std::string, Payload>& attributes) {
    env_->UpsertSearchAttributes(attributes);
  }

  // Register a query handler `R Fn(Args...)`, à la `workflow.SetQueryHandler`.
  // Handlers must be read-only (no activities/timers): they run against live
  // workflow state when a query arrives. Re-registering replaces the handler.
  template <class Fn>
  void SetQueryHandler(std::string name, Fn handler) {
    using Sig = internal::fn_sig<std::decay_t<Fn>>;
    env_->RegisterQueryHandler(
        std::move(name), MakeQueryFn<typename Sig::ret, typename Sig::args>(std::move(handler)));
  }

  // Typed-handle overload: ties the handler's result type to the QueryRef (a
  // mismatch is a compile error) and registers under the ref's name.
  template <class R, class Fn>
  void SetQueryHandler(const QueryRef<R>& ref, Fn handler) {
    static_assert(std::is_same_v<typename internal::fn_sig<std::decay_t<Fn>>::ret, R>,
                  "query handler must return the QueryRef's result type");
    SetQueryHandler(std::string(ref.name), std::move(handler));
  }

  // Register an update handler `R Fn(Args...)`, à la `workflow.SetUpdateHandler`.
  // Unlike a query it may mutate workflow state, and the call is recorded.
  template <class Fn>
  void SetUpdateHandler(std::string name, Fn handler) {
    using Sig = internal::fn_sig<std::decay_t<Fn>>;
    env_->RegisterUpdateHandler(
        std::move(name), MakeQueryFn<typename Sig::ret, typename Sig::args>(std::move(handler)));
  }

  // Typed-handle overload: ties the handler's result type to the UpdateRef.
  template <class R, class Fn>
  void SetUpdateHandler(const UpdateRef<R>& ref, Fn handler) {
    static_assert(std::is_same_v<typename internal::fn_sig<std::decay_t<Fn>>::ret, R>,
                  "update handler must return the UpdateRef's result type");
    SetUpdateHandler(std::string(ref.name), std::move(handler));
  }

  // Register an update handler with a read-only validator. The validator takes
  // the same arguments as the handler and is run first; if it throws, the update
  // is rejected before acceptance (nothing is written to history and the handler
  // never runs). Validators must not mutate state or schedule activities/timers.
  template <class Fn, class ValidatorFn>
  void SetUpdateHandler(std::string name, Fn handler, ValidatorFn validator) {
    using Sig = internal::fn_sig<std::decay_t<Fn>>;
    env_->RegisterUpdateValidator(name, MakeValidatorFn<typename Sig::args>(std::move(validator)));
    env_->RegisterUpdateHandler(
        std::move(name), MakeQueryFn<typename Sig::ret, typename Sig::args>(std::move(handler)));
  }

  // Capture the result of a non-deterministic operation exactly once. The first
  // time it runs, `fn` executes and its result is recorded to history; on every
  // replay the recorded value is returned without running `fn` again. Mirrors
  // the Go SDK's `workflow.SideEffect`. Use for ids, randomness, reading a clock
  // — never for anything with externally-visible effects.
  template <class R, class Fn>
  R SideEffect(Fn fn) {
    static_assert(!std::is_void_v<R>, "SideEffect's function must return a value");
    if (const auto recorded = env_->ReplaySideEffect()) {
      return converter_->FromPayload<R>(*recorded);
    }
    R value = fn();
    env_->RecordSideEffect(converter_->ToPayload(value));
    return value;
  }

  // Returns a version number for the named change, recorded the first time it
  // runs so the workflow can branch on code changes safely across replays.
  // Mirrors `workflow.GetVersion`; returns `kDefaultVersion` (-1) when replaying
  // history recorded before this call existed. Pass `kDefaultVersion` as
  // `min_supported` to keep supporting the un-versioned (pre-call) branch.
  int GetVersion(const std::string& change_id, int min_supported, int max_supported) {
    return env_->GetVersion(change_id, min_supported, max_supported);
  }

  // Like SideEffect, but keyed by `id` and only records a new marker when the
  // value changes (compared with `equals`). On replay the recorded value is
  // returned without running `fn`. Mirrors the Go SDK's
  // `workflow.MutableSideEffect`: use it to capture a value that usually stays
  // the same across many calls without writing a marker every time.
  template <class Fn, class Equals>
  auto MutableSideEffect(const std::string& id, Fn fn, Equals equals) -> std::invoke_result_t<Fn> {
    using R = std::invoke_result_t<Fn>;
    static_assert(!std::is_void_v<R>, "MutableSideEffect's function must return a value");
    Payload value_out;
    bool has_current = false;
    const auto step = env_->BeginMutableSideEffect(id, value_out, has_current);
    if (step != internal::MutableSideEffectStep::Live) {
      return converter_->FromPayload<R>(value_out);  // replayed change or current value
    }
    R value = fn();
    if (has_current) {
      R current = converter_->FromPayload<R>(value_out);
      if (equals(current, value)) {
        env_->RecordMutableSideEffect(id, /*changed=*/false, Payload{});
        return current;
      }
    }
    Payload encoded = converter_->ToPayload(value);
    env_->RecordMutableSideEffect(id, /*changed=*/true, encoded);
    return value;
  }

  // MutableSideEffect using operator== to compare values.
  template <class Fn>
  auto MutableSideEffect(const std::string& id, Fn fn) -> std::invoke_result_t<Fn> {
    using R = std::invoke_result_t<Fn>;
    return MutableSideEffect(id, std::move(fn), std::equal_to<R>{});
  }

  // Run a registered activity inline in the workflow worker — no activity-task
  // round-trip — recording its result as a marker; on replay the recorded result
  // is returned without re-running. Retries happen inline per the retry policy.
  // Best for short, idempotent steps. Mirrors the Go SDK's
  // `workflow.ExecuteLocalActivity` (resolves within the call here).
  template <class R, class... Args>
  R ExecuteLocalActivity(const LocalActivityOptions& options, std::string_view activity_type,
                         const Args&... args) {
    Payloads input = converter_->ToPayloads(args...);
    Payloads result = env_->ExecuteLocalActivity(std::string(activity_type), input, options);
    if constexpr (std::is_void_v<R>) {
      (void)result;
    } else {
      return converter_->FromPayload<R>(result.empty() ? Payload{} : result.front());
    }
  }

  const WorkflowInfo& GetInfo() const { return env_->Info(); }
  log::Logger& GetLogger() const { return env_->Logger(); }
  bool IsReplaying() const { return env_->IsReplaying(); }

  // Used by the worker's registration adapter; also available to user code.
  const DataConverter& data_converter() const { return *converter_; }

 private:
  friend class Selector;

  template <class Ret, class Args, class Fn>
  internal::QueryFn MakeQueryFn(Fn handler) {
    const DataConverter* converter = converter_;
    return [handler = std::move(handler), converter](const Payloads& in) -> Payloads {
      auto args = internal::DecodeArgs<Args>(*converter, in,
                                             std::make_index_sequence<std::tuple_size_v<Args>>{});
      if constexpr (std::is_void_v<Ret>) {
        std::apply([&](auto&... a) { handler(a...); }, args);
        return Payloads{};
      } else {
        Ret result = std::apply([&](auto&... a) { return handler(a...); }, args);
        return Payloads{converter->ToPayload(result)};
      }
    };
  }

  // Wraps a read-only update validator: decodes the same args as the handler and
  // invokes the validator (whose return value is ignored). Throwing rejects.
  template <class Args, class Fn>
  internal::QueryFn MakeValidatorFn(Fn validator) {
    const DataConverter* converter = converter_;
    return [validator = std::move(validator), converter](const Payloads& in) -> Payloads {
      auto args = internal::DecodeArgs<Args>(*converter, in,
                                             std::make_index_sequence<std::tuple_size_v<Args>>{});
      std::apply([&](auto&... a) { validator(a...); }, args);
      return Payloads{};
    };
  }

  internal::WorkflowOutbound* env_;
  const DataConverter* converter_;
};

}  // namespace temporal::workflow
