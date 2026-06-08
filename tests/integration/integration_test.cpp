// Integration tests: exercise the implemented surface end-to-end against a real
// Temporal server. Gated behind TEMPORAL_INTEGRATION=1 (and TEMPORAL_ADDRESS,
// default localhost:7233) so the default `ctest` run needs no server.
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

namespace {

using namespace std::chrono_literals;

// ---- activities ----------------------------------------------------------
std::string EchoActivity(temporal::activity::Context&, std::string s) { return s; }

int AddOneActivity(temporal::activity::Context&, int n) { return n + 1; }

std::string BoomActivity(temporal::activity::Context&, std::string) {
  throw temporal::ApplicationError("boom", "BoomError");
}

// ---- workflows -----------------------------------------------------------
std::string SleepWorkflow(temporal::workflow::Context& ctx, int millis) {
  ctx.Sleep(std::chrono::milliseconds(millis));  // exercises the timer path
  return "slept";
}

std::string EchoWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  return ctx.ExecuteActivity<std::string>(o, "Echo", s).Get();
}

int ParallelWorkflow(temporal::workflow::Context& ctx, int base) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  auto f1 = ctx.ExecuteActivity<int>(o, "AddOne", base);        // all three scheduled
  auto f2 = ctx.ExecuteActivity<int>(o, "AddOne", base + 10);   // before any Get()
  auto f3 = ctx.ExecuteActivity<int>(o, "AddOne", base + 100);
  return f1.Get() + f2.Get() + f3.Get();
}

std::string FailWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  o.retry_policy.maximum_attempts = 1;  // fail fast: proves RetryPolicy is wired
  o.retry_policy_set = true;
  return ctx.ExecuteActivity<std::string>(o, "Boom", s).Get();
}

std::string LongSleepWorkflow(temporal::workflow::Context& ctx, int) {
  ctx.Sleep(60s);
  return "done";
}

// Waits for a "setName" signal, then greets.
std::string GreetBySignalWorkflow(temporal::workflow::Context& ctx) {
  return "Hello, " + ctx.GetSignalChannel<std::string>("setName").Receive();
}

// Counts "input" signals until a "done" signal arrives.
int CountSignalsWorkflow(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("input");
  int count = 0;
  while (signals.Receive() != "done") {
    ++count;
  }
  return count;
}

// Waits for signals but bails out if the workflow is cancelled.
std::string CancellableWorkflow(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("go");
  while (true) {
    if (ctx.IsCancelled()) {
      return "cancelled";
    }
    if (signals.Receive() == "stop") {
      return "stopped";
    }
  }
}

// Maintains a running sum of "add" signals; answers a "sum" query with the total
// (exercising a query against live, suspended workflow state).
int QueryableWorkflow(temporal::workflow::Context& ctx) {
  int sum = 0;
  ctx.SetQueryHandler("sum", [&] { return sum; });
  auto signals = ctx.GetSignalChannel<int>("add");
  while (true) {
    const int v = signals.Receive();
    if (v < 0) {
      return sum;
    }
    sum += v;
  }
}

std::string SleepActivity(temporal::activity::Context&, int ms) {
  if (ms > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  }
  return "done";
}

// Selects between an activity result and a timeout timer (the canonical pattern).
std::string SelectorWorkflow(temporal::workflow::Context& ctx, int activity_ms, int timeout_ms) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 30s;
  auto activity = ctx.ExecuteActivity<std::string>(o, "Sleep", activity_ms);
  auto timeout = ctx.NewTimer(std::chrono::milliseconds(timeout_ms));

  std::string out;
  temporal::workflow::Selector selector(ctx);
  selector.AddFuture<std::string>(activity, [&](std::string r) { out = "activity:" + r; });
  selector.AddFuture(timeout, [&]() { out = "timeout"; });
  selector.Select();
  return out;
}

// Runs N sequential activities. Under the sticky cache each activity completion
// is a continuation (apply event + resume) rather than a full replay.
int ChainWorkflow(temporal::workflow::Context& ctx, int n) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  int value = 0;
  for (int i = 0; i < n; ++i) {
    value = ctx.ExecuteActivity<int>(o, "AddOne", value).Get();
  }
  return value;
}

std::string GreetChildWorkflow(temporal::workflow::Context&, std::string name) {
  return "child:" + name;
}

// Runs a child workflow on the same task queue and returns its result.
std::string ParentWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ChildWorkflowOptions o;
  o.task_queue = ctx.GetInfo().task_queue;
  return ctx.ExecuteChildWorkflow<std::string>(o, "GreetChildWorkflow", name).Get();
}

// ---- harness -------------------------------------------------------------
std::atomic<int> g_seq{0};

std::string UniqueTaskQueue(const std::string& base) {
  return "itest-" + base + "-" + std::to_string(g_seq.fetch_add(1));
}

class IntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (std::getenv("TEMPORAL_INTEGRATION") == nullptr) {
      GTEST_SKIP() << "set TEMPORAL_INTEGRATION=1 and run `temporal server start-dev` to enable";
    }
    const char* addr = std::getenv("TEMPORAL_ADDRESS");
    temporal::ClientOptions opt;
    opt.target = (addr != nullptr) ? addr : "localhost:7233";
    client_ = std::make_unique<temporal::client::Client>(temporal::client::Client::Connect(opt));
  }

  std::unique_ptr<temporal::client::Client> client_;
};

TEST_F(IntegrationTest, TimerWorkflowCompletes) {
  const auto tq = UniqueTaskQueue("timer");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("SleepWorkflow", SleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "SleepWorkflow", 500);
  EXPECT_EQ(handle.Result<std::string>(), "slept");
  worker.Stop();
}

TEST_F(IntegrationTest, SingleActivityRoundTrip) {
  const auto tq = UniqueTaskQueue("echo");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "EchoWorkflow", std::string("ping"));
  EXPECT_EQ(handle.Result<std::string>(), "ping");
  worker.Stop();
}

TEST_F(IntegrationTest, ParallelActivitiesAllResolve) {
  const auto tq = UniqueTaskQueue("parallel");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ParallelWorkflow", ParallelWorkflow);
  worker.RegisterActivity("AddOne", AddOneActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ParallelWorkflow", 0);
  EXPECT_EQ(handle.Result<int>(), (0 + 1) + (10 + 1) + (100 + 1));  // 113
  worker.Stop();
}

TEST_F(IntegrationTest, ActivityFailurePropagatesWithMaxOneAttempt) {
  const auto tq = UniqueTaskQueue("fail");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("FailWorkflow", FailWorkflow);
  worker.RegisterActivity("Boom", BoomActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "FailWorkflow", std::string("x"));
  // Without RetryPolicy wiring the default policy would retry forever and this
  // would hang until the ctest timeout; maximum_attempts=1 makes it fail fast.
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  worker.Stop();
}

TEST_F(IntegrationTest, TerminateMakesResultThrow) {
  const auto tq = UniqueTaskQueue("terminate");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("LongSleepWorkflow", LongSleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "LongSleepWorkflow", 0);
  handle.Terminate("integration test");
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  worker.Stop();
}

TEST_F(IntegrationTest, SignalDeliveredToWorkflow) {
  const auto tq = UniqueTaskQueue("signal-greet");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("GreetBySignalWorkflow", GreetBySignalWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "GreetBySignalWorkflow");
  const auto dc = temporal::DataConverter::Default();
  handle.Signal("setName", dc->ToPayloads(std::string("World")));
  EXPECT_EQ(handle.Result<std::string>(), "Hello, World");
  worker.Stop();
}

TEST_F(IntegrationTest, MultipleSignalsCountedInOrder) {
  const auto tq = UniqueTaskQueue("signal-count");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CountSignalsWorkflow", CountSignalsWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CountSignalsWorkflow");
  const auto dc = temporal::DataConverter::Default();
  handle.Signal("input", dc->ToPayloads(std::string("a")));
  handle.Signal("input", dc->ToPayloads(std::string("b")));
  handle.Signal("input", dc->ToPayloads(std::string("done")));
  EXPECT_EQ(handle.Result<int>(), 2);  // "a" and "b" counted, "done" terminates
  worker.Stop();
}

TEST_F(IntegrationTest, CancellationObservedByWorkflow) {
  const auto tq = UniqueTaskQueue("cancel");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CancellableWorkflow", CancellableWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CancellableWorkflow");
  handle.Cancel();
  EXPECT_EQ(handle.Result<std::string>(), "cancelled");
  worker.Stop();
}

TEST_F(IntegrationTest, QueryReadsLiveWorkflowState) {
  const auto tq = UniqueTaskQueue("query");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("QueryableWorkflow", QueryableWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "QueryableWorkflow");
  const auto dc = temporal::DataConverter::Default();
  handle.Signal("add", dc->ToPayloads(5));
  handle.Signal("add", dc->ToPayloads(7));
  EXPECT_EQ(handle.Query<int>("sum"), 12);  // query the live, parked workflow
  handle.Terminate("done");
  worker.Stop();
}

TEST_F(IntegrationTest, SelectorPicksActivityWhenItWinsTheRace) {
  const auto tq = UniqueTaskQueue("select-activity");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("SelectorWorkflow", SelectorWorkflow);
  worker.RegisterActivity("Sleep", SleepActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "SelectorWorkflow", 0, 10000);  // fast activity
  EXPECT_EQ(handle.Result<std::string>(), "activity:done");
  worker.Stop();
}

TEST_F(IntegrationTest, SelectorPicksTimeoutWhenActivityIsSlow) {
  const auto tq = UniqueTaskQueue("select-timeout");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("SelectorWorkflow", SelectorWorkflow);
  worker.RegisterActivity("Sleep", SleepActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "SelectorWorkflow", 3000, 500);  // 0.5s timer wins
  EXPECT_EQ(handle.Result<std::string>(), "timeout");
  worker.Stop();
}

TEST_F(IntegrationTest, StickyCacheServesContinuations) {
  const auto tq = UniqueTaskQueue("sticky-chain");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ChainWorkflow", ChainWorkflow);
  worker.RegisterActivity("AddOne", AddOneActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ChainWorkflow", 5);
  EXPECT_EQ(handle.Result<int>(), 5);  // 0 -> +1 five times
  // Several of those workflow tasks were served as sticky-cache continuations
  // rather than full replays; if the sticky path were broken this would be 0.
  EXPECT_GT(worker.cache_hits(), 0);
  worker.Stop();
}

TEST_F(IntegrationTest, ChildWorkflowReturnsResultToParent) {
  const auto tq = UniqueTaskQueue("child");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ParentWorkflow", ParentWorkflow);
  worker.RegisterWorkflow("GreetChildWorkflow", GreetChildWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ParentWorkflow", std::string("World"));
  EXPECT_EQ(handle.Result<std::string>(), "child:World");
  worker.Stop();
}

// LongSleepWorkflow ignores signals/cancel, so this only asserts the client RPCs
// succeed (the reacting cases are covered by the tests above).
TEST_F(IntegrationTest, SignalAndCancelRpcsSucceed) {
  const auto tq = UniqueTaskQueue("signal");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("LongSleepWorkflow", LongSleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "LongSleepWorkflow", 0);
  const auto dc = temporal::DataConverter::Default();
  EXPECT_NO_THROW(handle.Signal("ping", dc->ToPayloads(std::string("hi"))));
  EXPECT_NO_THROW(handle.Cancel());
  handle.Terminate("cleanup");
  worker.Stop();
}

}  // namespace
