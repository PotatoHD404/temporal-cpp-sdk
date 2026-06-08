#include <chrono>
#include <memory>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>
#include <temporal/internal/workflow_outbound.h>
#include <temporal/workflow/context.h>

namespace {

using namespace temporal;

// A fake WorkflowOutbound that lets tests preset whether scheduled operations are
// already resolved, exercising the Future/Context determinism seam without a
// server.
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
      throw internal::WorkflowBlocked{};
    }
  }

  const workflow::WorkflowInfo& Info() const override { return info; }
  log::Logger& Logger() const override { return *log::DefaultLogger(); }
  bool IsReplaying() const override { return false; }

  int scheduled = 0;
  int timers = 0;
  bool ready = false;
  Payloads result;
  workflow::WorkflowInfo info;
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

  EXPECT_THROW(future.Get(), internal::WorkflowBlocked);
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

}  // namespace
