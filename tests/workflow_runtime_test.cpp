#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>
#include <temporal/internal/workflow_outbound.h>
#include <temporal/workflow/context.h>
#include <temporal/workflow/selector.h>

namespace {

using namespace temporal;

// Sentinel the fake throws where the real coroutine engine would yield.
struct FakeSuspend {};

// A fake WorkflowOutbound that lets tests preset whether scheduled operations are
// already resolved, exercising the Future/Context determinism seam without a
// server or coroutine.
class FakeEnv : public internal::WorkflowOutbound {
 public:
  std::shared_ptr<internal::FutureState> ScheduleActivity(std::string_view, const Payloads&,
                                                          const ActivityOptions&) override {
    ++scheduled;
    auto st = std::make_shared<internal::FutureState>();
    st->ready = ready;
    st->result = result;
    return st;
  }

  std::shared_ptr<internal::FutureState> StartTimer(std::chrono::nanoseconds) override {
    ++timers;
    auto st = std::make_shared<internal::FutureState>();
    st->ready = ready;
    return st;
  }

  std::shared_ptr<internal::FutureState> StartChildWorkflow(std::string_view, const Payloads&,
                                                           const ChildWorkflowOptions&) override {
    ++children;
    auto st = std::make_shared<internal::FutureState>();
    st->ready = ready;
    st->result = result;
    return st;
  }

  void Block(const std::shared_ptr<internal::FutureState>& st) override {
    if (!st->ready) {
      throw FakeSuspend{};
    }
  }

  void Park() override { throw FakeSuspend{}; }

  void Cancel(const std::shared_ptr<internal::FutureState>& state) override {
    if (state) {
      state->ready = true;
      state->cancelled = true;
    }
  }

  void CancelExternalWorkflow(std::string_view) override {}

  void SignalExternalWorkflow(std::string_view, std::string_view, const Payloads&) override {}

  void RegisterQueryHandler(std::string, internal::QueryFn) override {}

  void RegisterUpdateHandler(std::string, internal::QueryFn) override {}

  void RegisterUpdateValidator(std::string, internal::QueryFn v) override {
    update_validator = std::move(v);
  }

  std::optional<Payload> ReplaySideEffect() override {
    if (side_effect_cursor < recorded_side_effects.size()) {
      return recorded_side_effects[side_effect_cursor++];
    }
    ++side_effect_cursor;
    return std::nullopt;
  }

  void RecordSideEffect(const Payload& value) override { emitted_side_effects.push_back(value); }

  int GetVersion(const std::string& change_id, int min_supported, int max_supported) override {
    (void)min_supported;
    const auto it = recorded_versions.find(change_id);
    if (it != recorded_versions.end()) {
      return it->second;
    }
    if (replaying) {
      return workflow::kDefaultVersion;
    }
    emitted_versions[change_id] = max_supported;
    return max_supported;
  }

  bool TryConsumeSignal(std::string_view name, Payloads& out) override {
    if (std::string(name) != signal_name || signal_cursor >= signal_queue.size()) {
      return false;
    }
    out = signal_queue[signal_cursor++];
    return true;
  }

  bool HasSignal(std::string_view name) const override {
    return std::string(name) == signal_name && signal_cursor < signal_queue.size();
  }

  bool IsCancelRequested() const override { return cancel_requested; }

  std::shared_ptr<internal::FutureState> AwaitCancellation() override {
    auto st = std::make_shared<internal::FutureState>();
    st->ready = cancel_requested;
    return st;
  }

  const workflow::WorkflowInfo& Info() const override { return info; }
  log::Logger& Logger() const override { return *log::DefaultLogger(); }
  bool IsReplaying() const override { return replaying; }

  int scheduled = 0;
  int timers = 0;
  int children = 0;
  bool ready = false;
  Payloads result;
  workflow::WorkflowInfo info;
  std::string signal_name = "sig";
  std::vector<Payloads> signal_queue;
  std::size_t signal_cursor = 0;
  bool cancel_requested = false;
  bool replaying = false;
  std::vector<Payload> recorded_side_effects;  // preset to simulate replay
  std::size_t side_effect_cursor = 0;
  std::vector<Payload> emitted_side_effects;  // captured on the live path
  std::unordered_map<std::string, int> recorded_versions;  // preset to simulate replay
  std::unordered_map<std::string, int> emitted_versions;   // captured on the live path
  internal::QueryFn update_validator;                      // captured registered validator
};

TEST(WorkflowRuntime, ExecuteActivityReturnsResultWhenReady) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  env.ready = true;
  env.result = dc->ToPayloads(std::string("hi"));

  workflow::Context ctx(&env, dc.get());
  const ActivityOptions opts;
  auto future = ctx.ExecuteActivity<std::string>(opts, "ComposeGreeting", std::string("x"));

  EXPECT_EQ(env.scheduled, 1);
  EXPECT_EQ(future.Get(), "hi");
}

TEST(WorkflowRuntime, GetParksWorkflowWhenNotReady) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  env.ready = false;

  workflow::Context ctx(&env, dc.get());
  const ActivityOptions opts;
  auto future = ctx.ExecuteActivity<std::string>(opts, "ComposeGreeting", std::string("x"));

  EXPECT_THROW(future.Get(), FakeSuspend);
}

TEST(WorkflowRuntime, FailedActivitySurfacesActivityError) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  // Simulate a resolved-but-failed activity by readying a state we mark failed.
  workflow::Context ctx(&env, dc.get());

  auto st = std::make_shared<internal::FutureState>();
  st->ready = true;
  st->failed = true;
  st->failure_type = "BoomError";
  st->failure_message = "boom";
  workflow::Future<std::string> future(st, dc.get(), &env);

  EXPECT_THROW(future.Get(), ActivityError);
}

TEST(WorkflowRuntime, SignalChannelReturnsBufferedSignal) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  env.signal_name = "greet";
  env.signal_queue = {dc->ToPayloads(std::string("World"))};

  workflow::Context ctx(&env, dc.get());
  auto channel = ctx.GetSignalChannel<std::string>("greet");
  EXPECT_EQ(channel.Receive(), "World");
}

TEST(WorkflowRuntime, SignalChannelParksWhenEmpty) {
  const auto dc = DataConverter::Default();
  FakeEnv env;  // no buffered signals

  workflow::Context ctx(&env, dc.get());
  auto channel = ctx.GetSignalChannel<std::string>("greet");
  EXPECT_THROW(channel.Receive(), FakeSuspend);

  std::string out;
  EXPECT_FALSE(channel.ReceiveAsync(out));
}

TEST(WorkflowRuntime, CancellationFlagSurfaces) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  env.cancel_requested = true;

  workflow::Context ctx(&env, dc.get());
  EXPECT_TRUE(ctx.IsCancelled());
}

TEST(WorkflowRuntime, SelectorPicksTheReadyCase) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  workflow::Context ctx(&env, dc.get());

  auto ready_state = std::make_shared<internal::FutureState>();
  ready_state->ready = true;
  ready_state->result = dc->ToPayloads(std::string("R"));
  const workflow::Future<std::string> ready(ready_state, dc.get(), &env);

  const workflow::Future<std::string> pending(std::make_shared<internal::FutureState>(), dc.get(),
                                              &env);

  std::string picked;
  workflow::Selector selector(ctx);
  selector.AddFuture<std::string>(pending, [&](std::string v) { picked = "pending:" + v; });
  selector.AddFuture<std::string>(ready, [&](std::string v) { picked = "ready:" + v; });
  selector.Select();
  EXPECT_EQ(picked, "ready:R");
}

TEST(WorkflowRuntime, SelectorRunsDefaultWhenNothingReady) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  workflow::Context ctx(&env, dc.get());

  const workflow::Future<std::string> pending(std::make_shared<internal::FutureState>(), dc.get(),
                                              &env);
  bool ran_default = false;
  workflow::Selector selector(ctx);
  selector.AddFuture<std::string>(pending, [](std::string) {});
  selector.AddDefault([&] { ran_default = true; });
  selector.Select();
  EXPECT_TRUE(ran_default);
}

TEST(WorkflowRuntime, SelectorParksWhenNothingReadyAndNoDefault) {
  const auto dc = DataConverter::Default();
  FakeEnv env;
  workflow::Context ctx(&env, dc.get());

  const workflow::Future<std::string> pending(std::make_shared<internal::FutureState>(), dc.get(),
                                              &env);
  workflow::Selector selector(ctx);
  selector.AddFuture<std::string>(pending, [](std::string) {});
  EXPECT_THROW(selector.Select(), FakeSuspend);
}

TEST(WorkflowRuntime, SideEffectRunsFunctionLiveAndRecordsResult) {
  FakeEnv env;
  const auto dc = DataConverter::Default();
  workflow::Context ctx(&env, dc.get());
  int calls = 0;
  const int v = ctx.SideEffect<int>([&] {
    ++calls;
    return 42;
  });
  EXPECT_EQ(v, 42);
  EXPECT_EQ(calls, 1);
  ASSERT_EQ(env.emitted_side_effects.size(), 1U);
  EXPECT_EQ(dc->FromPayload<int>(env.emitted_side_effects[0]), 42);
}

TEST(WorkflowRuntime, SideEffectReplaysRecordedValueWithoutRunningFunction) {
  FakeEnv env;
  const auto dc = DataConverter::Default();
  env.recorded_side_effects = {dc->ToPayload(7)};
  workflow::Context ctx(&env, dc.get());
  int calls = 0;
  const int v = ctx.SideEffect<int>([&] {
    ++calls;
    return 42;
  });
  EXPECT_EQ(v, 7);      // recorded value, not 42
  EXPECT_EQ(calls, 0);  // the function never ran on replay
  EXPECT_TRUE(env.emitted_side_effects.empty());
}

TEST(WorkflowRuntime, GetVersionChoosesMaxOnLivePathAndRecords) {
  FakeEnv env;
  const auto dc = DataConverter::Default();
  workflow::Context ctx(&env, dc.get());
  EXPECT_EQ(ctx.GetVersion("change-a", workflow::kDefaultVersion, 3), 3);
  EXPECT_EQ(env.emitted_versions["change-a"], 3);
}

TEST(WorkflowRuntime, GetVersionReturnsRecordedVersionOnReplay) {
  FakeEnv env;
  env.replaying = true;
  env.recorded_versions["change-a"] = 1;
  const auto dc = DataConverter::Default();
  workflow::Context ctx(&env, dc.get());
  EXPECT_EQ(ctx.GetVersion("change-a", workflow::kDefaultVersion, 3), 1);
}

TEST(WorkflowRuntime, GetVersionReturnsDefaultWhenReplayingPreVersionHistory) {
  FakeEnv env;
  env.replaying = true;  // no recorded marker for this change id
  const auto dc = DataConverter::Default();
  workflow::Context ctx(&env, dc.get());
  EXPECT_EQ(ctx.GetVersion("new-change", workflow::kDefaultVersion, 3), workflow::kDefaultVersion);
}

TEST(WorkflowRuntime, UpdateValidatorWrapperRejectsViaThrow) {
  // SetUpdateHandler-with-validator wires a validator that decodes the same args
  // as the handler and propagates a throw (which the engine turns into a
  // rejection). Valid input passes; invalid input throws.
  FakeEnv env;
  const auto dc = DataConverter::Default();
  workflow::Context ctx(&env, dc.get());
  ctx.SetUpdateHandler(
      "add", [](int n) { return n; },
      [](int n) {
        if (n <= 0) {
          throw temporal::ApplicationError("must be positive", "InvalidUpdate");
        }
      });
  ASSERT_TRUE(env.update_validator != nullptr);
  EXPECT_NO_THROW(env.update_validator(dc->ToPayloads(5)));
  EXPECT_THROW(env.update_validator(dc->ToPayloads(-1)), std::exception);
}

TEST(WorkflowRuntime, FutureCancelResolvesTheUnderlyingState) {
  FakeEnv env;
  const auto dc = DataConverter::Default();
  workflow::Context ctx(&env, dc.get());
  auto timer = ctx.NewTimer(std::chrono::seconds(60));
  EXPECT_FALSE(timer.IsReady());
  timer.Cancel();
  EXPECT_TRUE(timer.IsReady());  // Future::Cancel routed through env->Cancel()
}

TEST(WorkflowRuntime, AwaitCancellationReadyOnlyWhenCancelled) {
  FakeEnv env;
  const auto dc = DataConverter::Default();
  workflow::Context ctx(&env, dc.get());
  EXPECT_FALSE(ctx.AwaitCancellation().IsReady());  // not cancelled
  env.cancel_requested = true;
  EXPECT_TRUE(ctx.AwaitCancellation().IsReady());  // cancelled -> ready
}

}  // namespace
