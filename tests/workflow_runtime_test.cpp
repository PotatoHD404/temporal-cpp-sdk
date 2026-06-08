#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>
#include <temporal/internal/workflow_outbound.h>
#include <temporal/workflow/context.h>

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

  void Block(const std::shared_ptr<internal::FutureState>& st) override {
    if (!st->ready) {
      throw FakeSuspend{};
    }
  }

  void Park() override { throw FakeSuspend{}; }

  void RegisterQueryHandler(std::string, internal::QueryFn) override {}

  bool TryConsumeSignal(std::string_view name, Payloads& out) override {
    if (std::string(name) != signal_name || signal_cursor >= signal_queue.size()) {
      return false;
    }
    out = signal_queue[signal_cursor++];
    return true;
  }

  bool IsCancelRequested() const override { return cancel_requested; }

  const workflow::WorkflowInfo& Info() const override { return info; }
  log::Logger& Logger() const override { return *log::DefaultLogger(); }
  bool IsReplaying() const override { return false; }

  int scheduled = 0;
  int timers = 0;
  bool ready = false;
  Payloads result;
  workflow::WorkflowInfo info;
  std::string signal_name = "sig";
  std::vector<Payloads> signal_queue;
  std::size_t signal_cursor = 0;
  bool cancel_requested = false;
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

}  // namespace
