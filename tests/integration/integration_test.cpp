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

// Holds a running total; an "add" update adds to it and returns the new total.
// The body blocks on a signal so the workflow stays running for updates.
int UpdatableWorkflow(temporal::workflow::Context& ctx) {
  int total = 0;
  ctx.SetUpdateHandler("add", [&](int n) {
    total += n;
    return total;
  });
  ctx.GetSignalChannel<std::string>("stop").Receive();
  return total;
}

// Like UpdatableWorkflow, but a read-only validator rejects non-positive inputs
// before acceptance, so they never reach the handler or get written to history.
int ValidatedUpdateWorkflow(temporal::workflow::Context& ctx) {
  int total = 0;
  ctx.SetUpdateHandler(
      "add", [&](int n) { total += n; return total; },
      [](int n) {
        if (n <= 0) {
          throw temporal::ApplicationError("must be positive", "InvalidUpdate");
        }
      });
  ctx.GetSignalChannel<std::string>("stop").Receive();
  return total;
}

// Two versions of one workflow type for the replay framework: V1 is what ran and
// produced the recorded history; V2 is an incompatible "code change" (a different
// activity) that a replay must flag as non-deterministic.
int ReplayFwV1(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  return ctx.ExecuteActivity<int>(o, "AddOne", 41).Get();
}
int ReplayFwV2(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  return ctx.ExecuteActivity<int>(o, "Multiply", 41).Get();  // different activity type
}

// Starts a long timer then immediately cancels it. Future::Cancel resolves the
// timer so the workflow finishes at once instead of waiting it out.
std::string TimerCancelWorkflow(temporal::workflow::Context& ctx) {
  auto timer = ctx.NewTimer(60s);
  timer.Cancel();
  timer.Get();  // returns immediately (cancelled), not after 60s
  return "cancelled";
}

// Runs longer than its heartbeat timeout but heartbeats to stay alive.
std::string HeartbeatActivity(temporal::activity::Context& ctx, int) {
  for (int i = 0; i < 5; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ctx.RecordHeartbeat(i);
  }
  return "alive";
}

std::string HeartbeatWorkflow(temporal::workflow::Context& ctx, int n) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 30s;
  o.heartbeat_timeout = 2s;  // shorter than the activity's ~2.5s runtime
  o.retry_policy.maximum_attempts = 2;
  o.retry_policy_set = true;
  return ctx.ExecuteActivity<std::string>(o, "Heartbeat", n).Get();
}

// Counts down via continue-as-new until n reaches 0.
int CountdownWorkflow(temporal::workflow::Context& ctx, int n) {
  if (n <= 0) {
    return 0;
  }
  ctx.ContinueAsNew("CountdownWorkflow", n - 1);
}

// Schedules an activity (recorded in history), then parks on a "bump" signal.
// Used to force a from-scratch full-history replay after a worker restart and
// verify a correct, deterministic workflow is NOT flagged as non-deterministic.
int ReplayProbeWorkflow(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  const int base = ctx.ExecuteActivity<int>(o, "AddOne", 41).Get();  // -> 42, in history
  const int bump = ctx.GetSignalChannel<int>("bump").Receive();      // arrives post-restart
  return base + bump;
}

// SideEffect/GetVersion must resolve exactly once: recorded the first time, then
// replayed. This global counts SideEffect function invocations across replays.
std::atomic<int> g_side_effect_calls{0};

// Records a Version marker and a SideEffect marker, then parks on a signal so a
// worker restart forces a full replay of both markers.
int MarkerWorkflow(temporal::workflow::Context& ctx) {
  const int ver = ctx.GetVersion("v1", temporal::workflow::kDefaultVersion, 2);  // -> 2
  const int v = ctx.SideEffect<int>([] { return 100 + g_side_effect_calls.fetch_add(1); });
  ctx.GetSignalChannel<std::string>("go").Receive();  // park until after the restart
  return v + (ver * 1000);
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
  // Queries are read-after-write eventually consistent vs. just-sent signals
  // (and a query right after start may briefly be unanswerable), so poll the
  // live, parked workflow until both signals have been applied. The window is
  // generous because CI runners are slow.
  int sum = -1;
  for (int i = 0; i < 150 && sum != 12; ++i) {
    try {
      sum = handle.Query<int>("sum");
    } catch (const std::exception&) {
      // not yet answerable (handler not registered / eventual consistency)
    }
    if (sum != 12) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  EXPECT_EQ(sum, 12);
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

TEST_F(IntegrationTest, UpdateMutatesStateAndReturnsResult) {
  const auto tq = UniqueTaskQueue("update");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("UpdatableWorkflow", UpdatableWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "UpdatableWorkflow");
  EXPECT_EQ(handle.Update<int>("add", 5), 5);  // accumulates across updates
  EXPECT_EQ(handle.Update<int>("add", 3), 8);
  handle.Terminate("done");
  worker.Stop();
}

TEST_F(IntegrationTest, ActivityHeartbeatKeepsActivityAlive) {
  const auto tq = UniqueTaskQueue("heartbeat");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("HeartbeatWorkflow", HeartbeatWorkflow);
  worker.RegisterActivity("Heartbeat", HeartbeatActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "HeartbeatWorkflow", 0);
  // Without heartbeating the activity would time out (2s) before finishing (~2.5s).
  EXPECT_EQ(handle.Result<std::string>(), "alive");
  worker.Stop();
}

TEST_F(IntegrationTest, ContinueAsNewChainsToCompletion) {
  const auto tq = UniqueTaskQueue("continue");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CountdownWorkflow", CountdownWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CountdownWorkflow", 3);
  EXPECT_EQ(handle.Result<int>(), 0);  // client follows the continue-as-new chain
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

// A correct, deterministic workflow must survive a from-scratch full-history
// replay (after a sticky-cache loss) without tripping non-determinism detection.
TEST_F(IntegrationTest, FullReplayAfterWorkerRestartStaysDeterministic) {
  const auto tq = UniqueTaskQueue("replay");
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ReplayProbe");
  const auto dc = temporal::DataConverter::Default();

  // Worker A runs the activity; the workflow then parks on the "bump" signal,
  // resident in A's sticky cache. Stopping A discards that cache.
  {
    temporal::worker::Worker worker_a(*client_, tq);
    worker_a.RegisterWorkflow("ReplayProbe", ReplayProbeWorkflow);
    worker_a.RegisterActivity("AddOne", AddOneActivity);
    worker_a.Start();
    std::this_thread::sleep_for(6s);  // activity completes; workflow parks on the signal
    worker_a.Stop();
  }

  // Worker B has a cold cache. Once the bump signal's task reschedules onto the
  // normal queue (A's sticky queue times out), B replays the full history from
  // scratch and must reproduce the recorded AddOne command. A non-determinism
  // false-positive would fail the task and the workflow would never complete.
  temporal::worker::Worker worker_b(*client_, tq);
  worker_b.RegisterWorkflow("ReplayProbe", ReplayProbeWorkflow);
  worker_b.RegisterActivity("AddOne", AddOneActivity);
  worker_b.Start();

  handle.Signal("bump", dc->ToPayloads(100));
  EXPECT_EQ(handle.Result<int>(), 142);  // 42 + 100: the full replay completed cleanly
  EXPECT_GE(worker_b.replays(), 1);      // prove B actually replayed from scratch
  worker_b.Stop();
}

// SideEffect and GetVersion record a marker on first execution and replay it
// verbatim afterwards. The side-effect function must run exactly once, even
// across a from-scratch replay on a fresh worker.
TEST_F(IntegrationTest, MarkersRecordOnceAndReplayAcrossRestart) {
  g_side_effect_calls.store(0);
  const auto tq = UniqueTaskQueue("markers");
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "MarkerWorkflow");
  const auto dc = temporal::DataConverter::Default();

  {
    temporal::worker::Worker worker_a(*client_, tq);
    worker_a.RegisterWorkflow("MarkerWorkflow", MarkerWorkflow);
    worker_a.Start();
    std::this_thread::sleep_for(6s);  // markers recorded; workflow parks on "go"
    worker_a.Stop();
  }
  EXPECT_EQ(g_side_effect_calls.load(), 1);  // SideEffect's function ran once on worker A

  temporal::worker::Worker worker_b(*client_, tq);
  worker_b.RegisterWorkflow("MarkerWorkflow", MarkerWorkflow);
  worker_b.Start();
  handle.Signal("go", dc->ToPayloads(std::string("now")));
  EXPECT_EQ(handle.Result<int>(), 2100);     // GetVersion->2 (x1000) + SideEffect->100
  EXPECT_EQ(g_side_effect_calls.load(), 1);  // replay did NOT re-run the side-effect function
  worker_b.Stop();
}

// An update validator rejects invalid input before acceptance: the client's
// Update throws, the handler never runs, and (because a rejection is not written
// to history) workflow state is unaffected.
TEST_F(IntegrationTest, UpdateValidatorRejectsInvalidInput) {
  const auto tq = UniqueTaskQueue("update-validate");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ValidatedUpdateWorkflow", ValidatedUpdateWorkflow);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ValidatedUpdateWorkflow");
  const auto dc = temporal::DataConverter::Default();

  // Valid updates are accepted and return the new running total.
  EXPECT_EQ(handle.Update<int>("add", 5), 5);
  EXPECT_EQ(handle.Update<int>("add", 7), 12);

  // An invalid update is rejected by the validator: Update throws.
  EXPECT_THROW(handle.Update<int>("add", -3), std::exception);

  // A subsequent valid update proves the rejection left state untouched (12 + 1).
  EXPECT_EQ(handle.Update<int>("add", 1), 13);

  handle.Signal("stop", dc->ToPayloads(std::string("done")));
  EXPECT_EQ(handle.Result<int>(), 13);
  worker.Stop();
}

// The replay framework: a recorded history replayed against the SAME workflow
// code is deterministic; replayed against incompatibly-changed code it is caught
// as non-determinism — all without contacting a server.
TEST_F(IntegrationTest, ReplayFrameworkDetectsNonDeterministicChange) {
  const auto tq = UniqueTaskQueue("replay-fw");
  std::string history_json;
  {
    temporal::worker::Worker worker(*client_, tq);
    worker.RegisterWorkflow("ReplayFwWorkflow", ReplayFwV1);
    worker.RegisterActivity("AddOne", AddOneActivity);
    worker.Start();
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    auto handle = client_->StartWorkflow(o, "ReplayFwWorkflow");
    EXPECT_EQ(handle.Result<int>(), 42);
    history_json = handle.FetchHistoryJson();  // export the real history
    worker.Stop();
  }
  ASSERT_FALSE(history_json.empty());

  // Same code -> deterministic replay (no RPCs; the worker is never started).
  {
    temporal::worker::Worker replayer(*client_, tq);
    replayer.RegisterWorkflow("ReplayFwWorkflow", ReplayFwV1);
    EXPECT_NO_THROW(replayer.ReplayWorkflowHistory(history_json));
  }
  // Changed code (a different activity) -> non-determinism is detected.
  {
    temporal::worker::Worker replayer(*client_, tq);
    replayer.RegisterWorkflow("ReplayFwWorkflow", ReplayFwV2);
    EXPECT_THROW(replayer.ReplayWorkflowHistory(history_json), std::exception);
  }
}

// Cancelling a timer resolves it immediately: a workflow that starts a 60s timer
// and cancels it completes in seconds, and its StartTimer+CancelTimer history
// replays deterministically.
TEST_F(IntegrationTest, TimerCancellationCompletesFastAndReplays) {
  const auto tq = UniqueTaskQueue("timer-cancel");
  std::string history_json;
  {
    temporal::worker::Worker worker(*client_, tq);
    worker.RegisterWorkflow("TimerCancelWorkflow", TimerCancelWorkflow);
    worker.Start();
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    const auto t0 = std::chrono::steady_clock::now();
    auto handle = client_->StartWorkflow(o, "TimerCancelWorkflow");
    EXPECT_EQ(handle.Result<std::string>(), "cancelled");
    // A non-cancelled 60s timer would block Result ~60s; cancellation finishes fast.
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(20));
    history_json = handle.FetchHistoryJson();
    worker.Stop();
  }
  // The recorded StartTimer + CancelTimer history replays deterministically.
  temporal::worker::Worker replayer(*client_, tq);
  replayer.RegisterWorkflow("TimerCancelWorkflow", TimerCancelWorkflow);
  EXPECT_NO_THROW(replayer.ReplayWorkflowHistory(history_json));
}

// Memo set at start is returned verbatim by Describe, along with the run status.
TEST_F(IntegrationTest, MemoIsAttachedAndReturnedByDescribe) {
  const auto tq = UniqueTaskQueue("memo");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("LongSleepWorkflow", LongSleepWorkflow);
  worker.Start();
  const auto dc = temporal::DataConverter::Default();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  o.memo["owner"] = dc->ToPayload(std::string("alice"));
  o.memo["priority"] = dc->ToPayload(7);
  auto handle = client_->StartWorkflow(o, "LongSleepWorkflow", 0);

  const auto desc = handle.Describe();
  EXPECT_EQ(desc.status, "RUNNING");
  ASSERT_EQ(desc.memo.count("owner"), 1U);
  EXPECT_EQ(dc->FromPayload<std::string>(desc.memo.at("owner")), "alice");
  EXPECT_EQ(dc->FromPayload<int>(desc.memo.at("priority")), 7);

  handle.Terminate("done");
  worker.Stop();
}

// Signal-with-start creates a not-yet-running workflow and delivers a signal to
// it atomically.
TEST_F(IntegrationTest, SignalWithStartCreatesAndSignals) {
  const auto tq = UniqueTaskQueue("sigwithstart");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("GreetBySignalWorkflow", GreetBySignalWorkflow);  // waits for "setName"
  worker.Start();
  const auto dc = temporal::DataConverter::Default();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->SignalWithStartWorkflow(o, "GreetBySignalWorkflow", "setName",
                                                 dc->ToPayloads(std::string("Ada")));
  EXPECT_EQ(handle.Result<std::string>(), "Hello, Ada");
  worker.Stop();
}

// With a bounded sticky cache (capacity 1), starting a second workflow evicts the
// first; the first's next task then triggers a from-scratch replay. Both still
// complete correctly, proving eviction + replay-recovery.
TEST_F(IntegrationTest, BoundedCacheEvictsAndStillCompletes) {
  const auto tq = UniqueTaskQueue("lru");
  temporal::WorkerOptions wo;
  wo.max_cached_workflows = 1;  // only one resident workflow at a time
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("GreetBySignalWorkflow", GreetBySignalWorkflow);
  worker.Start();
  const auto dc = temporal::DataConverter::Default();

  temporal::StartWorkflowOptions oa;
  oa.task_queue = tq;
  auto a = client_->StartWorkflow(oa, "GreetBySignalWorkflow");
  std::this_thread::sleep_for(3s);  // A is processed and cached
  temporal::StartWorkflowOptions ob;
  ob.task_queue = tq;
  auto b = client_->StartWorkflow(ob, "GreetBySignalWorkflow");
  std::this_thread::sleep_for(3s);  // B is cached -> A evicted (capacity 1)

  a.Signal("setName", dc->ToPayloads(std::string("Ada")));
  b.Signal("setName", dc->ToPayloads(std::string("Linus")));
  EXPECT_EQ(a.Result<std::string>(), "Hello, Ada");      // recovered via replay
  EXPECT_EQ(b.Result<std::string>(), "Hello, Linus");
  EXPECT_GE(worker.replays(), 3);  // 2 first-tasks + at least one eviction-replay
  worker.Stop();
}

}  // namespace
