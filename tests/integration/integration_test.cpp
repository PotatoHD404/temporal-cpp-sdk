// Integration tests: exercise the implemented surface end-to-end against a real
// Temporal server. Gated behind TEMPORAL_INTEGRATION=1 (and TEMPORAL_ADDRESS,
// default localhost:7233) so the default `ctest` run needs no server.
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <temporal/interceptor/interceptor.h>
#include <temporal/interceptor/tracing.h>
#include <temporal/temporal.h>

#include "temporal/api/failure/v1/message.pb.h"

#include "integration_fixture.h"  // IntegrationTest, UniqueTaskQueue, g_seq

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

// Creates two host-pinned sessions back to back, running an Echo activity in each
// on the session host. With a worker capacity of 1 the second CreateSession can
// only succeed if the first was released by CompleteSession — so a passing result
// proves both host pinning (the activity ran on the session queue) and the
// create/complete slot bookkeeping. Returns "<host queue>|<a>|<b>".
std::string SessionLifecycleWorkflow(temporal::workflow::Context& ctx) {
  auto s1 = ctx.CreateSession();
  temporal::ActivityOptions o1;
  o1.task_queue = s1.task_queue;
  o1.start_to_close_timeout = 10s;
  const std::string a = ctx.ExecuteActivity<std::string>(o1, "Echo", std::string("a")).Get();
  ctx.CompleteSession(s1);

  auto s2 = ctx.CreateSession();
  temporal::ActivityOptions o2;
  o2.task_queue = s2.task_queue;
  o2.start_to_close_timeout = 10s;
  const std::string b = ctx.ExecuteActivity<std::string>(o2, "Echo", std::string("b")).Get();
  ctx.CompleteSession(s2);
  return s1.task_queue + "|" + a + "|" + b;
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
  // fail fast: proves RetryPolicy is wired.
  o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 1};
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

// Waits on a long timer, racing it against cancellation. When the workflow is
// cancelled it wakes immediately, cancels the timer, and returns "cancelled".
std::string CancelAwareWorkflow(temporal::workflow::Context& ctx) {
  auto timer = ctx.NewTimer(60s);
  auto cancelled = ctx.AwaitCancellation();
  std::string out;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture(timer, [&]() { out = "timer-fired"; });
  sel.AddFuture(cancelled, [&]() {
    timer.Cancel();
    out = "cancelled";
  });
  sel.Select();
  return out;
}

// Starts a cancel-aware child, lets it start, then cancels it and returns the
// child's result. The child observes the cancel and finishes "cancelled".
// (Temporal forbids Start + RequestCancel of a child in the same workflow task,
// so the timer forces the cancel into a later task.)
std::string CancelChildWorkflow(temporal::workflow::Context& ctx) {
  temporal::ChildWorkflowOptions o;
  auto child = ctx.ExecuteChildWorkflow<std::string>(o, "CancelAwareWorkflow");
  ctx.Sleep(std::chrono::seconds(1));
  child.Cancel();
  return child.Get();
}

// Cancels an unrelated workflow by id (external cancellation), then completes.
// Stays alive briefly so the cancel is delivered before this workflow closes.
std::string CancelExternalWf(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.CancelExternalWorkflow(target_id);
  ctx.Sleep(std::chrono::seconds(3));
  return "done";
}

// Signals an unrelated workflow by id (external signal), then completes. Stays
// alive briefly so the signal is delivered before this workflow closes.
std::string SignalExternalWf(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));
  ctx.Sleep(std::chrono::seconds(3));
  return "done";
}

// Upserts an indexed search attribute (value passed as arg), then returns.
std::string UpsertSaWorkflow(temporal::workflow::Context& ctx, std::string val) {
  ctx.UpsertSearchAttributes({{"ItestKeyword", temporal::sa::Keyword(val)}});
  return "upserted";
}

std::atomic<bool> g_async_captured{false};
std::string g_async_token;  // NOLINT: test handoff for async completion

// Defers completion: captures its task token, signals async, returns (ignored).
std::string AsyncCaptureActivity(temporal::activity::Context& ctx, int) {
  g_async_token = ctx.defer_completion();  // sets async + returns the task token
  g_async_captured.store(true);
  return "";
}

std::string AsyncActivityWorkflow(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 30s;
  return ctx.ExecuteActivity<std::string>(o, "AsyncCaptureActivity", 0).Get();
}

// Decodes and returns the named auto-propagated header value (or "MISSING").
std::string HeaderEchoActivity(temporal::activity::Context& ctx, std::string key) {
  const auto& headers = ctx.GetInfo().headers;
  const auto it = headers.find(key);
  if (it == headers.end()) {
    return "MISSING";
  }
  return ctx.data_converter().FromPayload<std::string>(it->second);
}

// Confirms it received the start header, then has it auto-propagated to an activity.
std::string HeaderPropagationWorkflow(temporal::workflow::Context& ctx) {
  const bool wf_has = ctx.GetInfo().headers.count("trace") > 0;
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  const std::string act =
      ctx.ExecuteActivity<std::string>(o, "HeaderEchoActivity", std::string("trace")).Get();
  return std::string(wf_has ? "wf-has|" : "wf-missing|") + act;
}

// Heartbeats until it observes a server cancel request, then returns "cancelled".
std::string CancellableActivity(temporal::activity::Context& ctx, int) {
  for (int i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ctx.RecordHeartbeat(i);
    if (ctx.IsCancelled()) {
      return "cancelled";
    }
  }
  return "finished";
}

// Runs a heartbeating activity; if the workflow is cancelled it cancels the
// activity (RequestCancelActivityTask) and returns the activity's result.
std::string ActivityCancelWorkflow(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 60s;
  o.heartbeat_timeout = 5s;
  auto act = ctx.ExecuteActivity<std::string>(o, "CancellableActivity", 0);
  auto cancelled = ctx.AwaitCancellation();
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(act, [](std::string) {});  // activity finished on its own
  sel.AddFuture(cancelled, [&]() { act.Cancel(); });    // workflow cancel -> cancel the activity
  sel.Select();
  return act.Get();  // the activity's result ("cancelled" after it observes the request)
}

// Selects between a "go" signal and a timeout timer ("signal OR timeout"),
// exercising a Selector signal-channel receive case.
std::string SignalSelectorWorkflow(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("go");
  auto timeout = ctx.NewTimer(30s);
  std::string out;
  temporal::workflow::Selector sel(ctx);
  sel.AddReceive<std::string>(signals, [&](std::string s) { out = "signal:" + s; });
  sel.AddFuture(timeout, [&]() { out = "timeout"; });
  sel.Select();
  return out;
}

// Runs longer than its heartbeat timeout but heartbeats to stay alive. Heartbeats
// finely (every 100ms) so that even with server-report throttling (~80% of the
// heartbeat timeout) the gap between actual reports stays safely under the timeout.
std::string HeartbeatActivity(temporal::activity::Context& ctx, int) {
  for (int i = 0; i < 25; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ctx.RecordHeartbeat(i);
  }
  return "alive";
}

std::string HeartbeatWorkflow(temporal::workflow::Context& ctx, int n) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 30s;
  o.heartbeat_timeout = 2s;  // shorter than the activity's ~2.5s runtime
  o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 2};
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
  handle.Signal("setName", std::string("World"));  // variadic: encodes for us
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
  handle.Signal("input", std::string("a"));
  handle.Signal("input", std::string("b"));
  handle.Signal("input", std::string("done"));
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

// A workflow can wait on cancellation (AwaitCancellation) as a Selector case and
// react: here it cancels its timer and returns promptly instead of waiting 60s.
TEST_F(IntegrationTest, WorkflowReactsToCancellationViaSelector) {
  const auto tq = UniqueTaskQueue("wf-cancel");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CancelAwareWorkflow", CancelAwareWorkflow);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  const auto t0 = std::chrono::steady_clock::now();
  auto handle = client_->StartWorkflow(o, "CancelAwareWorkflow");
  std::this_thread::sleep_for(3s);  // let the workflow park on the selector
  handle.Cancel();
  EXPECT_EQ(handle.Result<std::string>(), "cancelled");  // woke via AwaitCancellation
  EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(20));
  worker.Stop();
}

// End-to-end activity cancellation: cancelling the workflow makes it cancel its
// in-flight activity (RequestCancelActivityTask); the activity observes the
// request via its heartbeat (Context::IsCancelled) and returns "cancelled".
TEST_F(IntegrationTest, ActivityCancellationViaWorkflowCancel) {
  const auto tq = UniqueTaskQueue("act-cancel");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ActivityCancelWorkflow", ActivityCancelWorkflow);
  worker.RegisterActivity("CancellableActivity", CancellableActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ActivityCancelWorkflow");
  std::this_thread::sleep_for(3s);  // let the activity start and heartbeat
  handle.Cancel();                  // workflow cancel -> the workflow cancels its activity
  EXPECT_EQ(handle.Result<std::string>(), "cancelled");  // activity observed it and returned
  worker.Stop();
}

// A Selector can wait on a signal channel: the signal arrives before the 30s
// timeout, so the channel case wins.
TEST_F(IntegrationTest, SelectorPicksSignalOverTimeout) {
  const auto tq = UniqueTaskQueue("select-signal");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("SignalSelectorWorkflow", SignalSelectorWorkflow);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "SignalSelectorWorkflow");
  const auto dc = temporal::DataConverter::Default();
  std::this_thread::sleep_for(2s);  // let the workflow park on the selector
  handle.Signal("go", dc->ToPayloads(std::string("hi")));
  EXPECT_EQ(handle.Result<std::string>(), "signal:hi");  // the channel case won
  worker.Stop();
}

// A schedule can be created (interval + start-workflow action), described, and
// deleted via the client. A 1-hour interval ensures it doesn't fire during the
// test, so no workflows are spawned.
TEST_F(IntegrationTest, ScheduleCreateDescribeDelete) {
  const std::string schedule_id = UniqueTaskQueue("schedule");
  temporal::ScheduleOptions opts;
  opts.interval = std::chrono::hours(1);
  opts.workflow_type = "EchoWorkflow";
  opts.task_queue = schedule_id + "-tq";
  client_->CreateSchedule(schedule_id, opts);
  EXPECT_TRUE(client_->DescribeSchedule(schedule_id));  // it exists
  EXPECT_NO_THROW(client_->DeleteSchedule(schedule_id));
}

// The fuller schedule lifecycle: list finds it, and update/pause/unpause/trigger
// all succeed.
TEST_F(IntegrationTest, ScheduleUpdateListTriggerPause) {
  const std::string sid = UniqueTaskQueue("schedule2");
  temporal::ScheduleOptions opts;
  opts.interval = std::chrono::hours(1);  // long -> never auto-fires during the test
  opts.workflow_type = "EchoWorkflow";
  opts.task_queue = sid + "-tq";
  client_->CreateSchedule(sid, opts);

  bool found = false;
  for (int i = 0; i < 20 && !found; ++i) {  // listing is eventually consistent
    for (const auto& id : client_->ListSchedules()) {
      if (id == sid) {
        found = true;
        break;
      }
    }
    if (!found) {
      std::this_thread::sleep_for(250ms);
    }
  }
  EXPECT_TRUE(found);

  EXPECT_NO_THROW(client_->PauseSchedule(sid, "test"));
  EXPECT_NO_THROW(client_->UnpauseSchedule(sid));
  opts.interval = std::chrono::hours(2);
  EXPECT_NO_THROW(client_->UpdateSchedule(sid, opts));
  EXPECT_NO_THROW(client_->TriggerSchedule(sid));
  EXPECT_NO_THROW(client_->DeleteSchedule(sid));
}

// ListWorkflows + CountWorkflows query the visibility store. Two runs of a
// unique workflow type are started; a WorkflowType filter then finds exactly
// those two (visibility is eventually consistent, so we poll briefly).
TEST_F(IntegrationTest, ListAndCountWorkflows) {
  const auto tq = UniqueTaskQueue("list");
  // Process-unique type: g_seq resets each run but a dev server persists prior
  // runs' workflows, so a per-process seed keeps the visibility query exact.
  const std::string wf_type = "ListCountWf" + std::to_string(std::random_device{}());
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow(wf_type, SleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h1 = client_->StartWorkflow(o, wf_type, 0);
  auto h2 = client_->StartWorkflow(o, wf_type, 0);
  EXPECT_EQ(h1.Result<std::string>(), "slept");
  EXPECT_EQ(h2.Result<std::string>(), "slept");

  const std::string query = "WorkflowType = '" + wf_type + "'";
  std::vector<temporal::client::WorkflowDescription> listed;
  for (int i = 0; i < 40; ++i) {  // wait for the visibility index to catch up
    listed = client_->ListWorkflows(query);
    if (listed.size() == 2) {
      break;
    }
    std::this_thread::sleep_for(250ms);
  }
  ASSERT_EQ(listed.size(), 2U);
  EXPECT_EQ(listed[0].workflow_type, wf_type);
  EXPECT_FALSE(listed[0].run_id.empty());
  EXPECT_EQ(client_->CountWorkflows(query), 2);
  worker.Stop();
}

// A parent can cancel a running child workflow: the child observes the cancel
// (AwaitCancellation) and finishes "cancelled", which the parent receives.
TEST_F(IntegrationTest, ChildWorkflowCancellation) {
  const auto tq = UniqueTaskQueue("child-cancel");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CancelChildWorkflow", CancelChildWorkflow);
  worker.RegisterWorkflow("CancelAwareWorkflow", CancelAwareWorkflow);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CancelChildWorkflow");
  EXPECT_EQ(handle.Result<std::string>(), "cancelled");
  worker.Stop();
}

// One workflow cancels another, unrelated workflow by id (external cancellation).
// The target awaits cancellation and finishes "cancelled".
TEST_F(IntegrationTest, ExternalWorkflowCancellation) {
  const auto tq = UniqueTaskQueue("ext-cancel");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CancelAwareWorkflow", CancelAwareWorkflow);
  worker.RegisterWorkflow("CancelExternalWf", CancelExternalWf);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto target = client_->StartWorkflow(o, "CancelAwareWorkflow");
  std::this_thread::sleep_for(1s);  // let the target start + park on cancellation
  auto canceller = client_->StartWorkflow(o, "CancelExternalWf", target.id());
  EXPECT_EQ(canceller.Result<std::string>(), "done");      // canceller emitted the cancel
  EXPECT_EQ(target.Result<std::string>(), "cancelled");    // target observed it
  worker.Stop();
}

// One workflow signals another, unrelated workflow by id (external signal). The
// target receives the "setName" signal and greets it.
TEST_F(IntegrationTest, ExternalWorkflowSignal) {
  const auto tq = UniqueTaskQueue("ext-signal");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("GreetBySignalWorkflow", GreetBySignalWorkflow);
  worker.RegisterWorkflow("SignalExternalWf", SignalExternalWf);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto target = client_->StartWorkflow(o, "GreetBySignalWorkflow");
  std::this_thread::sleep_for(1s);  // let the target start + park on the signal
  auto signaller = client_->StartWorkflow(o, "SignalExternalWf", target.id());
  EXPECT_EQ(signaller.Result<std::string>(), "done");          // signaller emitted the signal
  EXPECT_EQ(target.Result<std::string>(), "Hello, World");     // target received it
  worker.Stop();
}

// A start-time search attribute is indexed and found by a visibility query.
TEST_F(IntegrationTest, StartTimeSearchAttributeIsQueryable) {
  // The SDK has no operator API, so register the custom attribute via the CLI
  // (idempotent); give it a moment to propagate before first use.
  std::system("temporal operator search-attribute create --name ItestKeyword --type Keyword "
              ">/dev/null 2>&1");
  std::this_thread::sleep_for(1s);
  const auto tq = UniqueTaskQueue("sa");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("SleepWorkflow", SleepWorkflow);
  worker.Start();
  const std::string val = "sa-" + std::to_string(std::random_device{}());
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  o.search_attributes["ItestKeyword"] = temporal::sa::Keyword(val);
  auto handle = client_->StartWorkflow(o, "SleepWorkflow", 0);
  EXPECT_EQ(handle.Result<std::string>(), "slept");

  const std::string query = "ItestKeyword = '" + val + "'";
  std::vector<temporal::client::WorkflowDescription> listed;
  for (int i = 0; i < 40; ++i) {  // visibility is eventually consistent
    listed = client_->ListWorkflows(query);
    if (!listed.empty()) {
      break;
    }
    std::this_thread::sleep_for(250ms);
  }
  ASSERT_EQ(listed.size(), 1U);
  EXPECT_EQ(listed[0].run_id, handle.run_id());
  worker.Stop();
}

// An activity defers completion (SetWillCompleteAsync) and is completed
// out-of-band via Client::CompleteActivity; the workflow receives that result.
TEST_F(IntegrationTest, AsyncActivityCompletion) {
  g_async_captured.store(false);
  g_async_token.clear();
  const auto tq = UniqueTaskQueue("async-act");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("AsyncActivityWorkflow", AsyncActivityWorkflow);
  worker.RegisterActivity("AsyncCaptureActivity", AsyncCaptureActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "AsyncActivityWorkflow");
  for (int i = 0; i < 100 && !g_async_captured.load(); ++i) {
    std::this_thread::sleep_for(100ms);
  }
  ASSERT_TRUE(g_async_captured.load());  // activity ran and deferred
  client_->CompleteActivity(g_async_token, std::string("async-result"));
  EXPECT_EQ(handle.Result<std::string>(), "async-result");
  worker.Stop();
}

// A start header is visible to the workflow and auto-propagated to its activities.
TEST_F(IntegrationTest, HeaderContextPropagation) {
  const auto tq = UniqueTaskQueue("header");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("HeaderPropagationWorkflow", HeaderPropagationWorkflow);
  worker.RegisterActivity("HeaderEchoActivity", HeaderEchoActivity);
  worker.Start();
  const auto dc = temporal::DataConverter::Default();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  o.headers["trace"] = dc->ToPayload(std::string("abc123"));
  auto handle = client_->StartWorkflow(o, "HeaderPropagationWorkflow");
  EXPECT_EQ(handle.Result<std::string>(), "wf-has|abc123");
  worker.Stop();
}

// Tallies the worker's task counters via the MetricsHandler interface.
class CountingMetrics : public temporal::MetricsHandler {
 public:
  void Counter(const std::string& name, std::int64_t value, const Tags&) override {
    if (name == "temporal_workflow_task_handled") {
      wf += value;
    } else if (name == "temporal_activity_task_handled") {
      act += value;
    }
  }
  void Gauge(const std::string&, double, const Tags&) override {}
  void Timer(const std::string&, std::chrono::nanoseconds, const Tags&) override {}
  std::atomic<std::int64_t> wf{0};
  std::atomic<std::int64_t> act{0};
};

// A worker emits task counters to a configured MetricsHandler.
TEST_F(IntegrationTest, MetricsHandlerReceivesCounters) {
  const auto tq = UniqueTaskQueue("metrics");
  auto metrics = std::make_shared<CountingMetrics>();
  temporal::WorkerOptions wo;
  wo.metrics_handler = metrics;
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "EchoWorkflow", std::string("hi"));
  EXPECT_EQ(handle.Result<std::string>(), "hi");
  EXPECT_GT(metrics->wf.load(), 0);   // at least one workflow task handled
  EXPECT_GT(metrics->act.load(), 0);  // at least one activity task handled
  worker.Stop();
}

// A workflow upserts an indexed search attribute, which a visibility query finds.
TEST_F(IntegrationTest, WorkflowUpsertsSearchAttribute) {
  std::system("temporal operator search-attribute create --name ItestKeyword --type Keyword "
              ">/dev/null 2>&1");
  std::this_thread::sleep_for(1s);
  const auto tq = UniqueTaskQueue("upsert-sa");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("UpsertSaWorkflow", UpsertSaWorkflow);
  worker.Start();
  const std::string val = "up-" + std::to_string(std::random_device{}());
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "UpsertSaWorkflow", val);
  EXPECT_EQ(handle.Result<std::string>(), "upserted");

  const std::string query = "ItestKeyword = '" + val + "'";
  std::vector<temporal::client::WorkflowDescription> listed;
  for (int i = 0; i < 40; ++i) {  // visibility is eventually consistent
    listed = client_->ListWorkflows(query);
    if (!listed.empty()) {
      break;
    }
    std::this_thread::sleep_for(250ms);
  }
  ASSERT_EQ(listed.size(), 1U);
  EXPECT_EQ(listed[0].run_id, handle.run_id());
  worker.Stop();
}

// ===========================================================================
// Wave-1 parity additions: client reset / build-id, worker concurrency /
// graceful drain / expanded metrics.
// ===========================================================================

// Tracks peak observed activity parallelism so a concurrency cap can be asserted.
std::atomic<int> g_parallel_now{0};
std::atomic<int> g_parallel_max{0};
std::string TrackingSleepActivity(temporal::activity::Context&, int ms) {
  const int now = ++g_parallel_now;
  int prev = g_parallel_max.load();
  while (now > prev && !g_parallel_max.compare_exchange_weak(prev, now)) {
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  --g_parallel_now;
  return "done";
}

// Fans out N activities so several are schedulable at once.
int FanOutWorkflow(temporal::workflow::Context& ctx, int n) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 30s;
  std::vector<temporal::workflow::Future<std::string>> fs;
  for (int i = 0; i < n; ++i) {
    fs.push_back(ctx.ExecuteActivity<std::string>(o, "TrackSleep", 400));
  }
  for (auto& f : fs) {
    f.Get();
  }
  return n;
}

// Flips a flag at its end so a graceful-drain test can assert it ran to completion.
std::atomic<bool> g_drain_activity_done{false};
std::string DrainSleepActivity(temporal::activity::Context&, int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  g_drain_activity_done = true;
  return "done";
}
std::string OneActivityWorkflow(temporal::workflow::Context& ctx, int ms) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 30s;
  return ctx.ExecuteActivity<std::string>(o, "DrainSleep", ms).Get();
}

// Captures timers/gauges/poll counters to assert the expanded metric set fires.
class RichMetrics : public temporal::MetricsHandler {
 public:
  void Counter(const std::string& n, std::int64_t v, const Tags&) override {
    if (n == "temporal_activity_poll_success") {
      act_poll_ok += v;
    }
    if (n == "temporal_workflow_poll_timeout" || n == "temporal_activity_poll_timeout") {
      timeouts += v;
    }
  }
  void Gauge(const std::string& n, double v, const Tags&) override {
    if (n == "temporal_activity_tasks_in_flight" && v >= 1.0) {
      saw_inflight = true;
    }
  }
  void Timer(const std::string& n, std::chrono::nanoseconds d, const Tags&) override {
    if (n == "temporal_activity_task_execution_latency" && d.count() > 0) {
      act_timer = true;
    }
    if (n == "temporal_workflow_task_execution_latency" && d.count() > 0) {
      wf_timer = true;
    }
  }
  std::atomic<std::int64_t> act_poll_ok{0};
  std::atomic<std::int64_t> timeouts{0};
  std::atomic<bool> act_timer{false};
  std::atomic<bool> wf_timer{false};
  std::atomic<bool> saw_inflight{false};
};

// POSITIVE: complete a workflow, reset it to the first completed workflow task,
// and verify a new run id comes back (and differs from the original).
TEST_F(IntegrationTest, ResetWorkflowStartsNewRun) {
  const auto tq = UniqueTaskQueue("reset");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("SleepWorkflow", SleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "SleepWorkflow", 200);
  EXPECT_EQ(handle.Result<std::string>(), "slept");

  // Event 4 = first WorkflowTaskCompleted (Started=1, TaskScheduled=2,
  // TaskStarted=3, TaskCompleted=4) — a valid WorkflowTaskFinishEventId.
  const std::string new_run_id =
      client_->ResetWorkflow(handle.id(), handle.run_id(), "integration test reset", 4);
  EXPECT_FALSE(new_run_id.empty());
  EXPECT_NE(new_run_id, handle.run_id());
  worker.Stop();
}

// NEGATIVE: resetting a workflow that was never started fails.
TEST_F(IntegrationTest, ResetNonexistentWorkflowThrows) {
  const std::string missing = "no-such-wf-" + std::to_string(std::random_device{}());
  EXPECT_THROW(client_->ResetWorkflow(missing, "", "reset missing", 4), temporal::RpcError);
}

// POSITIVE: add a build id as a new default set, then read it back.
TEST_F(IntegrationTest, BuildIdUpdateThenGet) {
  // Process-unique task queue: the dev server persists build-id sets across runs,
  // so a g_seq-only name would accumulate sets and break the exact-count assert.
  const auto tq = UniqueTaskQueue("buildid") + "-" + std::to_string(std::random_device{}());
  const std::string build_id = "v1-" + std::to_string(std::random_device{}());
  client_->UpdateWorkerBuildIdCompatibility(tq, build_id);

  std::vector<std::vector<std::string>> sets;
  for (int i = 0; i < 80; ++i) {  // build-id state is eventually consistent
    sets = client_->GetWorkerBuildIdCompatibility(tq);
    if (!sets.empty()) {
      break;
    }
    std::this_thread::sleep_for(250ms);
  }
  ASSERT_EQ(sets.size(), 1U);
  ASSERT_EQ(sets[0].size(), 1U);
  EXPECT_EQ(sets[0][0], build_id);
}

// NEGATIVE: Get on a task queue that never registered a build id returns empty
// (the dev server reports no versioning data rather than erroring).
TEST_F(IntegrationTest, BuildIdGetOnUnusedTaskQueueIsEmpty) {
  const auto tq = UniqueTaskQueue("buildid-unused");
  EXPECT_TRUE(client_->GetWorkerBuildIdCompatibility(tq).empty());
}

// POSITIVE: cap of 1 => observed max activity parallelism is exactly 1.
TEST_F(IntegrationTest, ActivityConcurrencyCapSerializesExecutions) {
  const auto tq = UniqueTaskQueue("conc-cap");
  g_parallel_now = 0;
  g_parallel_max = 0;
  temporal::WorkerOptions wo;
  wo.max_concurrent_activity_executions = 1;
  wo.activity_task_pollers = 4;  // pollers > cap: proves the gate, not poll count, limits
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("FanOutWorkflow", FanOutWorkflow);
  worker.RegisterActivity("TrackSleep", TrackingSleepActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "FanOutWorkflow", 5);
  EXPECT_EQ(h.Result<int>(), 5);
  EXPECT_EQ(g_parallel_max.load(), 1);  // never two activities at once
  worker.Stop();
}

// CONTROL: cap of 3 => more than one runs concurrently, but never exceeds the cap.
TEST_F(IntegrationTest, ActivityConcurrencyAllowsParallelismUpToCap) {
  const auto tq = UniqueTaskQueue("conc-par");
  g_parallel_now = 0;
  g_parallel_max = 0;
  temporal::WorkerOptions wo;
  wo.max_concurrent_activity_executions = 3;
  wo.activity_task_pollers = 4;
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("FanOutWorkflow", FanOutWorkflow);
  worker.RegisterActivity("TrackSleep", TrackingSleepActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "FanOutWorkflow", 5);
  EXPECT_EQ(h.Result<int>(), 5);
  EXPECT_GT(g_parallel_max.load(), 1);  // some overlap occurred
  EXPECT_LE(g_parallel_max.load(), 3);  // but never exceeded the cap
  worker.Stop();
}

// A long activity already running when Stop() is called still completes (drain).
TEST_F(IntegrationTest, GracefulShutdownDrainsInFlightActivity) {
  const auto tq = UniqueTaskQueue("drain");
  g_drain_activity_done = false;
  temporal::WorkerOptions wo;
  wo.graceful_shutdown_timeout = 10s;
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("OneActivityWorkflow", OneActivityWorkflow);
  worker.RegisterActivity("DrainSleep", DrainSleepActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "OneActivityWorkflow", 1500);
  std::this_thread::sleep_for(500ms);  // let the activity start
  worker.Stop();                       // must wait for the in-flight activity
  EXPECT_TRUE(g_drain_activity_done.load());
  (void)h;
}

// Timers, in-flight gauge, and poll counters all fire on the metrics handler.
TEST_F(IntegrationTest, MetricsHandlerReceivesTimersGaugesAndPollCounters) {
  const auto tq = UniqueTaskQueue("metrics-rich");
  auto m = std::make_shared<RichMetrics>();
  temporal::WorkerOptions wo;
  wo.metrics_handler = m;
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "EchoWorkflow", std::string("hi"));
  EXPECT_EQ(h.Result<std::string>(), "hi");
  EXPECT_TRUE(m->act_timer.load());     // activity execution timed
  EXPECT_TRUE(m->wf_timer.load());      // workflow task timed
  EXPECT_GT(m->act_poll_ok.load(), 0);  // at least one successful poll
  EXPECT_TRUE(m->saw_inflight.load());  // in-flight gauge emitted >= 1
  worker.Stop();
}

// ===========================================================================
// Wave-2 parity additions: MutableSideEffect (engine), batch operations,
// worker versioning rules.
// ===========================================================================

// Uses MutableSideEffect across timers (multiple tasks). The "config" changes at
// i==1 then stays the same, so only the first two calls record a marker; the
// recorded history must then replay deterministically.
std::string MutableSideEffectWorkflow(temporal::workflow::Context& ctx, int n) {
  int sum = 0;
  for (int i = 0; i < n; ++i) {
    const int v = ctx.MutableSideEffect("cfg", [&] { return i == 0 ? 1 : 2; });
    sum += v;
    ctx.Sleep(50ms);  // task boundary -> exercises sticky/replay between calls
  }
  return std::to_string(sum);
}

// POSITIVE: MutableSideEffect records only on change, and the recorded history
// replays deterministically (real marker record/replay engine path).
TEST_F(IntegrationTest, MutableSideEffectRecordsChangesAndReplays) {
  const auto tq = UniqueTaskQueue("mse");
  std::string history_json;
  {
    temporal::worker::Worker worker(*client_, tq);
    worker.RegisterWorkflow("MseWorkflow", MutableSideEffectWorkflow);
    worker.Start();
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    auto handle = client_->StartWorkflow(o, "MseWorkflow", 3);
    EXPECT_EQ(handle.Result<std::string>(), "5");  // 1 + 2 + 2
    history_json = handle.FetchHistoryJson();
    worker.Stop();
  }
  ASSERT_FALSE(history_json.empty());
  temporal::worker::Worker replayer(*client_, tq);
  replayer.RegisterWorkflow("MseWorkflow", MutableSideEffectWorkflow);
  EXPECT_NO_THROW(replayer.ReplayWorkflowHistory(history_json));  // deterministic
}

// POSITIVE: batch-terminate two running workflows matched by a visibility query.
TEST_F(IntegrationTest, BatchTerminateByQuery) {
  std::system("temporal operator search-attribute create --name ItestKeyword --type Keyword "
              ">/dev/null 2>&1");
  std::this_thread::sleep_for(1s);
  const auto tq = UniqueTaskQueue("batch");
  const std::string marker = "batch-" + std::to_string(std::random_device{}());
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("LongSleepWorkflow", LongSleepWorkflow);
  worker.Start();
  std::vector<temporal::client::WorkflowHandle> handles;
  for (int i = 0; i < 2; ++i) {
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    o.search_attributes["ItestKeyword"] = temporal::sa::Keyword(marker);
    handles.push_back(client_->StartWorkflow(o, "LongSleepWorkflow", 0));
  }
  const std::string query = "ItestKeyword = '" + marker + "'";
  for (int i = 0; i < 40; ++i) {  // visibility is eventually consistent
    if (client_->ListWorkflows(query).size() >= 2) {
      break;
    }
    std::this_thread::sleep_for(250ms);
  }
  const std::string job_id = "job-" + std::to_string(std::random_device{}());
  client_->StartBatchTerminate(job_id, query, "integration batch test");

  temporal::client::BatchOperationDescription desc;
  for (int i = 0; i < 60; ++i) {
    desc = client_->DescribeBatchOperation(job_id);
    // State names are the prefix-stripped proto enums, i.e. UPPERCASE.
    if (desc.state == "COMPLETED" || desc.state == "FAILED") {
      break;
    }
    std::this_thread::sleep_for(500ms);
  }
  EXPECT_EQ(desc.state, "COMPLETED");
  for (auto& h : handles) {
    EXPECT_EQ(h.Describe().status, "TERMINATED");
  }
  worker.Stop();
}

// NEGATIVE: describing an unknown batch job fails.
TEST_F(IntegrationTest, DescribeUnknownBatchOperationThrows) {
  const std::string job = "no-such-job-" + std::to_string(std::random_device{}());
  EXPECT_THROW(client_->DescribeBatchOperation(job), temporal::RpcError);
}

// POSITIVE: insert a worker assignment rule and read it back (rules-based
// versioning). Requires the dev server with frontend.workerVersioningRuleAPIs=true.
TEST_F(IntegrationTest, InsertAndReadWorkerAssignmentRule) {
  const auto tq = UniqueTaskQueue("vrules");
  const std::string build_id = "build-" + std::to_string(std::random_device{}());
  client_->InsertWorkerAssignmentRule(tq, build_id);
  const auto rules = client_->GetWorkerVersioningRules(tq);
  bool found = false;
  for (const auto& b : rules.assignment_rule_target_build_ids) {
    if (b == build_id) {
      found = true;
    }
  }
  EXPECT_TRUE(found);
  EXPECT_FALSE(rules.conflict_token.empty());
}

// ===========================================================================
// Wave-3 parity additions: local activities (engine), operator service.
// ===========================================================================

// A local activity: runs inline in the workflow worker (no activity-task poll).
int LocalAddActivity(temporal::activity::Context&, int a, int b) { return a + b; }

// Fails on attempt 1, succeeds on attempt 2 — exercises inline retry.
int FlakyLocalActivity(temporal::activity::Context& ctx, int n) {
  if (ctx.GetInfo().attempt < 2) {
    throw temporal::ApplicationError("flaky local activity", "Flaky");
  }
  return n;
}

// Runs two local activities across a timer (task boundary), so their markers
// must replay deterministically.
int LocalActivityWorkflow(temporal::workflow::Context& ctx, int base) {
  temporal::LocalActivityOptions o;
  const int x = ctx.ExecuteLocalActivity<int>(o, "LocalAdd", base, 5);
  ctx.Sleep(50ms);
  return ctx.ExecuteLocalActivity<int>(o, "LocalAdd", x, 100);
}

int RetryLocalWorkflow(temporal::workflow::Context& ctx, int n) {
  temporal::LocalActivityOptions o;
  o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};
  return ctx.ExecuteLocalActivity<int>(o, "FlakyLocal", n);
}

// POSITIVE: local activities run inline + their markers replay deterministically.
TEST_F(IntegrationTest, LocalActivityRunsInlineAndReplays) {
  const auto tq = UniqueTaskQueue("la");
  std::string history_json;
  {
    temporal::worker::Worker worker(*client_, tq);
    worker.RegisterWorkflow("LaWorkflow", LocalActivityWorkflow);
    worker.RegisterActivity("LocalAdd", LocalAddActivity);
    worker.Start();
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    auto handle = client_->StartWorkflow(o, "LaWorkflow", 10);
    EXPECT_EQ(handle.Result<int>(), 115);  // (10+5)=15, then 15+100=115
    history_json = handle.FetchHistoryJson();
    worker.Stop();
  }
  ASSERT_FALSE(history_json.empty());
  temporal::worker::Worker replayer(*client_, tq);
  replayer.RegisterWorkflow("LaWorkflow", LocalActivityWorkflow);
  replayer.RegisterActivity("LocalAdd", LocalAddActivity);
  EXPECT_NO_THROW(replayer.ReplayWorkflowHistory(history_json));  // deterministic
}

// POSITIVE: a local activity that fails on attempt 1 is retried inline and succeeds.
TEST_F(IntegrationTest, LocalActivityRetriesInline) {
  const auto tq = UniqueTaskQueue("la-retry");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("RetryLocalWorkflow", RetryLocalWorkflow);
  worker.RegisterActivity("FlakyLocal", FlakyLocalActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "RetryLocalWorkflow", 7);
  EXPECT_EQ(handle.Result<int>(), 7);  // succeeds on attempt 2
  worker.Stop();
}

// POSITIVE: register a custom search attribute via the operator service, then list it.
TEST_F(IntegrationTest, OperatorAddAndListSearchAttribute) {
  std::string attr;
  for (char ch : UniqueTaskQueue("sa")) {
    attr.push_back(ch == '-' ? '_' : ch);  // SA names allow only [A-Za-z0-9_]
  }
  EXPECT_NO_THROW(client_->AddSearchAttributes({{attr, "Keyword"}}));
  bool found = false;
  std::string type;
  for (int i = 0; i < 50 && !found; ++i) {  // registration is eventually consistent
    const auto sa = client_->ListSearchAttributes();
    const auto it = sa.custom.find(attr);
    if (it != sa.custom.end()) {
      found = true;
      type = it->second;
      break;
    }
    std::this_thread::sleep_for(300ms);
  }
  EXPECT_TRUE(found);
  EXPECT_EQ(type, "Keyword");
  EXPECT_NO_THROW(client_->RemoveSearchAttributes({attr}));
}

// NEGATIVE: an unknown type string is rejected client-side before any RPC.
TEST_F(IntegrationTest, OperatorAddSearchAttributeRejectsUnknownType) {
  EXPECT_THROW(client_->AddSearchAttributes({{"BogusAttr", "NotAType"}}), std::invalid_argument);
}

// ===========================================================================
// Wave-4 parity additions: per-second activity rate limiting.
// ===========================================================================

std::string InstantActivity(temporal::activity::Context&, int) { return "ok"; }

int RateFanOutWorkflow(temporal::workflow::Context& ctx, int n) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 30s;
  std::vector<temporal::workflow::Future<std::string>> fs;
  for (int i = 0; i < n; ++i) {
    fs.push_back(ctx.ExecuteActivity<std::string>(o, "Instant", i));
  }
  for (auto& f : fs) {
    f.Get();
  }
  return n;
}

// POSITIVE: a 2/sec rate limit paces 6 instant activity starts to take >= ~2s
// (without it, 6 instant activities finish in well under a second).
TEST_F(IntegrationTest, ActivityRateLimitPacesStarts) {
  const auto tq = UniqueTaskQueue("rate");
  temporal::WorkerOptions wo;
  wo.max_activities_per_second = 2.0;
  wo.activity_task_pollers = 4;  // pollers share one limiter -> 2/sec total
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("RateFanOutWorkflow", RateFanOutWorkflow);
  worker.RegisterActivity("Instant", InstantActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  const auto start = std::chrono::steady_clock::now();
  auto h = client_->StartWorkflow(o, "RateFanOutWorkflow", 6);
  EXPECT_EQ(h.Result<int>(), 6);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_GE(elapsed, 2s);  // 6 starts at 2/sec are spaced ~0.5s apart
  worker.Stop();
}

// POSITIVE: DescribeCluster (backed by GetClusterInfo) returns server info.
TEST_F(IntegrationTest, DescribeClusterReturnsServerInfo) {
  const auto desc = client_->DescribeCluster();
  EXPECT_FALSE(desc.server_version.empty());
  EXPECT_GT(desc.history_shard_count, 0);
}

// POSITIVE: ListClusters includes the dev server's "active" cluster.
TEST_F(IntegrationTest, ListClustersIncludesActiveCluster) {
  const auto names = client_->ListClusters();
  ASSERT_FALSE(names.empty());
  bool found_active = false;
  for (const auto& n : names) {
    if (n == "active") {
      found_active = true;
    }
  }
  EXPECT_TRUE(found_active);
}

// ===========================================================================
// Wave-5 parity additions: interceptor wiring (activity-inbound).
// ===========================================================================

std::atomic<int> g_intercepted_activities{0};

// An activity-inbound interceptor that records each wrapped execution, then runs
// the real activity via the next link.
class RecordingActivityInbound : public temporal::interceptor::ActivityInboundInterceptor {
 public:
  explicit RecordingActivityInbound(temporal::interceptor::ActivityInboundInterceptor* next)
      : temporal::interceptor::ActivityInboundInterceptor(next) {}
  temporal::Payloads ExecuteActivity(temporal::activity::Context& ctx,
                                     temporal::interceptor::ExecuteActivityInput& in,
                                     const temporal::interceptor::Header& header) override {
    ++g_intercepted_activities;
    return next_->ExecuteActivity(ctx, in, header);
  }
};

std::atomic<int> g_intercepted_workflows{0};

// A workflow-inbound interceptor that counts live (non-replay) workflow runs.
class RecordingWorkflowInbound : public temporal::interceptor::WorkflowInboundInterceptor {
 public:
  explicit RecordingWorkflowInbound(temporal::interceptor::WorkflowInboundInterceptor* next)
      : temporal::interceptor::WorkflowInboundInterceptor(next) {}
  temporal::Payloads ExecuteWorkflow(temporal::workflow::Context& ctx,
                                     temporal::interceptor::ExecuteWorkflowInput& in,
                                     const temporal::interceptor::Header& header) override {
    if (!ctx.IsReplaying()) {
      ++g_intercepted_workflows;
    }
    return next_->ExecuteWorkflow(ctx, in, header);
  }
};

class RecordingInterceptor : public temporal::interceptor::Interceptor {
 public:
  std::unique_ptr<temporal::interceptor::ActivityInboundInterceptor> InterceptActivity(
      temporal::interceptor::ActivityInboundInterceptor* next) override {
    return std::make_unique<RecordingActivityInbound>(next);
  }
  std::unique_ptr<temporal::interceptor::WorkflowInboundInterceptor> InterceptWorkflow(
      temporal::interceptor::WorkflowInboundInterceptor* next) override {
    return std::make_unique<RecordingWorkflowInbound>(next);
  }
};

// POSITIVE: an activity-inbound interceptor (installed via WorkerOptions) wraps
// the real activity execution.
TEST_F(IntegrationTest, ActivityInboundInterceptorWrapsExecution) {
  const auto tq = UniqueTaskQueue("interceptor");
  g_intercepted_activities = 0;
  temporal::WorkerOptions wo;
  wo.interceptors.push_back(std::make_shared<RecordingInterceptor>());
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "EchoWorkflow", std::string("hi"));
  EXPECT_EQ(h.Result<std::string>(), "hi");
  EXPECT_GT(g_intercepted_activities.load(), 0);  // the Echo activity was intercepted
  worker.Stop();
}

// POSITIVE + SAFETY: a workflow-inbound interceptor wraps the real workflow
// execution AND the recorded history still replays deterministically (the
// wrapping must not perturb command emission).
TEST_F(IntegrationTest, WorkflowInboundInterceptorWrapsExecutionAndReplays) {
  const auto tq = UniqueTaskQueue("wf-interceptor");
  g_intercepted_workflows = 0;
  temporal::WorkerOptions wo;
  wo.interceptors.push_back(std::make_shared<RecordingInterceptor>());
  std::string history_json;
  {
    temporal::worker::Worker worker(*client_, tq, wo);
    worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
    worker.RegisterActivity("Echo", EchoActivity);
    worker.Start();
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    auto h = client_->StartWorkflow(o, "EchoWorkflow", std::string("hi"));
    EXPECT_EQ(h.Result<std::string>(), "hi");
    EXPECT_GT(g_intercepted_workflows.load(), 0);  // the workflow execution was intercepted
    history_json = h.FetchHistoryJson();
    worker.Stop();
  }
  ASSERT_FALSE(history_json.empty());
  // Replay through the same interceptor must remain deterministic.
  temporal::worker::Worker replayer(*client_, tq, wo);
  replayer.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  replayer.RegisterActivity("Echo", EchoActivity);
  EXPECT_NO_THROW(replayer.ReplayWorkflowHistory(history_json));
}

// A workflow-outbound interceptor that injects a header on every ExecuteActivity.
class HeaderInjectingOutbound : public temporal::interceptor::WorkflowOutboundInterceptor {
 public:
  explicit HeaderInjectingOutbound(temporal::interceptor::WorkflowOutboundInterceptor* next)
      : temporal::interceptor::WorkflowOutboundInterceptor(next) {}
  void ExecuteActivity(temporal::workflow::Context& ctx,
                       temporal::interceptor::ExecuteActivityOutboundInput& in,
                       temporal::interceptor::Header& header) override {
    header["x-trace"] = ctx.data_converter().ToPayload(std::string("traced"));
    next_->ExecuteActivity(ctx, in, header);
  }
};

// An inbound interceptor whose Init wraps the outbound with the header injector.
class OutboundInjectInbound : public temporal::interceptor::WorkflowInboundInterceptor {
 public:
  explicit OutboundInjectInbound(temporal::interceptor::WorkflowInboundInterceptor* next)
      : temporal::interceptor::WorkflowInboundInterceptor(next) {}
  void Init(temporal::interceptor::WorkflowOutboundInterceptor* outbound) override {
    wrapped_ = std::make_unique<HeaderInjectingOutbound>(outbound);
    next_->Init(wrapped_.get());
  }

 private:
  std::unique_ptr<HeaderInjectingOutbound> wrapped_;
};

class OutboundInjectInterceptor : public temporal::interceptor::Interceptor {
 public:
  std::unique_ptr<temporal::interceptor::WorkflowInboundInterceptor> InterceptWorkflow(
      temporal::interceptor::WorkflowInboundInterceptor* next) override {
    return std::make_unique<OutboundInjectInbound>(next);
  }
};

std::atomic<bool> g_activity_saw_trace_header{false};
std::string HeaderCheckActivity(temporal::activity::Context& ctx, std::string) {
  g_activity_saw_trace_header = ctx.GetInfo().headers.count("x-trace") > 0;
  return "ok";
}
std::string HeaderCheckWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  return ctx.ExecuteActivity<std::string>(o, "HeaderCheck", s).Get();
}

// POSITIVE: a workflow-outbound interceptor injects a header that the scheduled
// activity then receives (the tracing-propagation use case).
TEST_F(IntegrationTest, WorkflowOutboundInterceptorInjectsActivityHeader) {
  const auto tq = UniqueTaskQueue("wf-outbound");
  g_activity_saw_trace_header = false;
  temporal::WorkerOptions wo;
  wo.interceptors.push_back(std::make_shared<OutboundInjectInterceptor>());
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("HeaderCheckWorkflow", HeaderCheckWorkflow);
  worker.RegisterActivity("HeaderCheck", HeaderCheckActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "HeaderCheckWorkflow", std::string("x"));
  EXPECT_EQ(h.Result<std::string>(), "ok");
  EXPECT_TRUE(g_activity_saw_trace_header.load());  // outbound header reached the activity
  worker.Stop();
}

// POSITIVE: the bundled TracingInterceptor (with an InMemoryTracer) creates spans
// around the workflow + activity and propagates one trace from workflow to
// activity through the header — i.e. tracing works end-to-end via the wired
// interceptor paths (no OpenTelemetry exporter; the Tracer is bring-your-own).
TEST_F(IntegrationTest, TracingInterceptorPropagatesTraceWorkflowToActivity) {
  auto tracer = std::make_shared<temporal::interceptor::InMemoryTracer>();
  auto tracing = std::make_shared<temporal::interceptor::TracingInterceptor>(tracer.get());
  const auto tq = UniqueTaskQueue("tracing");
  temporal::WorkerOptions wo;
  wo.interceptors.push_back(tracing);
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "EchoWorkflow", std::string("hi"));
  EXPECT_EQ(h.Result<std::string>(), "hi");
  worker.Stop();  // joins worker threads before reading the recorded spans

  const auto& recs = tracer->records();
  ASSERT_GE(recs.size(), 2U);  // at least a workflow span + an activity span
  // The activity span shares the workflow span's trace (propagated via header).
  const std::string trace = recs.front().trace_id;
  int linked = 0;
  for (const auto& r : recs) {
    if (r.trace_id == trace) {
      ++linked;
    }
  }
  EXPECT_GE(linked, 2);  // workflow + activity spans in one trace
}

// A failure converter that tags every error message, to prove the activity
// failure path uses the configured converter.
class WrappingFailureConverter : public temporal::FailureConverter {
 public:
  void ErrorToFailure(const std::exception& error,
                      temporal::api::failure::v1::Failure& out) const override {
    out.set_message(std::string("WRAPPED:") + error.what());
    out.mutable_application_failure_info()->set_type("Wrapped");
  }
  std::exception_ptr FailureToError(
      const temporal::api::failure::v1::Failure& failure) const override {
    return std::make_exception_ptr(temporal::ApplicationError(failure.message(), "Wrapped"));
  }
};

// POSITIVE: a custom failure converter on the worker's data converter encodes
// activity failures (the wrapped message propagates to the workflow result).
TEST_F(IntegrationTest, CustomFailureConverterEncodesActivityFailure) {
  const char* addr = std::getenv("TEMPORAL_ADDRESS");
  temporal::ClientOptions opt;
  opt.target = (addr != nullptr) ? addr : "localhost:7233";
  auto converter = temporal::DataConverter::Default();
  converter->WithFailureConverter(std::make_shared<WrappingFailureConverter>());
  opt.data_converter = converter;
  auto fc_client = temporal::client::Client::Connect(opt);

  const auto tq = UniqueTaskQueue("failconv");
  temporal::worker::Worker worker(fc_client, tq);
  worker.RegisterWorkflow("FailWorkflow", FailWorkflow);
  worker.RegisterActivity("Boom", BoomActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = fc_client.StartWorkflow(o, "FailWorkflow", std::string("x"));
  try {
    handle.Result<std::string>();
    FAIL() << "expected the workflow to fail";
  } catch (const temporal::WorkflowFailedError& e) {
    EXPECT_NE(std::string(e.what()).find("WRAPPED:boom"), std::string::npos);
  }
  worker.Stop();
}

std::string ThrowingWorkflow(temporal::workflow::Context&, std::string) {
  throw temporal::ApplicationError("wf-boom", "WfError");
}

// POSITIVE: the custom failure converter encodes a WORKFLOW's own failure (the
// workflow-failure path, not just activity failures).
TEST_F(IntegrationTest, CustomFailureConverterEncodesWorkflowFailure) {
  const char* addr = std::getenv("TEMPORAL_ADDRESS");
  temporal::ClientOptions opt;
  opt.target = (addr != nullptr) ? addr : "localhost:7233";
  auto converter = temporal::DataConverter::Default();
  converter->WithFailureConverter(std::make_shared<WrappingFailureConverter>());
  opt.data_converter = converter;
  auto fc_client = temporal::client::Client::Connect(opt);

  const auto tq = UniqueTaskQueue("failconv-wf");
  temporal::worker::Worker worker(fc_client, tq);
  worker.RegisterWorkflow("ThrowingWorkflow", ThrowingWorkflow);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = fc_client.StartWorkflow(o, "ThrowingWorkflow", std::string("x"));
  try {
    handle.Result<std::string>();
    FAIL() << "expected the workflow to fail";
  } catch (const temporal::WorkflowFailedError& e) {
    EXPECT_NE(std::string(e.what()).find("WRAPPED:wf-boom"), std::string::npos);
  }
  worker.Stop();
}

std::atomic<int> g_deadlock_metric{0};
class DeadlockMetrics : public temporal::MetricsHandler {
 public:
  void Counter(const std::string& n, std::int64_t v, const Tags&) override {
    if (n == "temporal_workflow_task_deadlock") {
      g_deadlock_metric += static_cast<int>(v);
    }
  }
  void Gauge(const std::string&, double, const Tags&) override {}
  void Timer(const std::string&, std::chrono::nanoseconds, const Tags&) override {}
};

// Blocks the workflow thread (a real sleep — forbidden in real workflows, used
// here to simulate a deadlock/blocking call).
std::string BlockingWorkflow(temporal::workflow::Context&, int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  return "done";
}

// POSITIVE: a workflow task that overruns the deadlock deadline is detected on
// its resume, reported via the deadlock metric, and ABORTED (the task is failed
// so the server reschedules it). A workflow that deterministically blocks its
// thread is aborted on every attempt and never completes, but the metric fires.
TEST_F(IntegrationTest, DeadlockDetectionReportsOverrunningTask) {
  const auto tq = UniqueTaskQueue("deadlock");
  g_deadlock_metric = 0;
  temporal::WorkerOptions wo;
  wo.deadlock_detection_timeout = 300ms;
  wo.metrics_handler = std::make_shared<DeadlockMetrics>();
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("BlockingWorkflow", BlockingWorkflow);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "BlockingWorkflow", 1500);  // 1.5s >> 300ms deadline
  // The overrun is detected + reported on every aborted attempt; wait for it.
  for (int i = 0; i < 100 && g_deadlock_metric.load() == 0; ++i) {
    std::this_thread::sleep_for(100ms);
  }
  EXPECT_GT(g_deadlock_metric.load(), 0);  // the watchdog flagged + aborted the overrun
  h.Terminate("test cleanup");  // never completes on its own (aborted on every retry)
  worker.Stop();
}

// POSITIVE: register a Nexus endpoint (worker target), describe it, and list it.
// Endpoint management only — not Nexus operation calls or a worker Nexus handler.
TEST_F(IntegrationTest, NexusEndpointManagement) {
  const std::string name = "itest-nexus-" + std::to_string(std::random_device{}());
  std::string id;
  try {
    id = client_->CreateNexusEndpoint(name, "nexus-handler-tq");
  } catch (const temporal::RpcError& e) {
    GTEST_SKIP() << "Nexus appears disabled on the dev server: " << e.what();
  }
  ASSERT_FALSE(id.empty());
  const auto desc = client_->GetNexusEndpoint(id);
  EXPECT_EQ(desc.name, name);
  EXPECT_EQ(desc.target_task_queue, "nexus-handler-tq");
  bool found = false;
  for (int i = 0; i < 20 && !found; ++i) {  // creation is eventually consistent
    for (const auto& n : client_->ListNexusEndpoints()) {
      if (n == name) {
        found = true;
        break;
      }
    }
    if (!found) {
      std::this_thread::sleep_for(200ms);
    }
  }
  EXPECT_TRUE(found);
}

std::atomic<int> g_client_start_intercepts{0};

class RecordingClientOutbound : public temporal::interceptor::ClientOutboundInterceptor {
 public:
  explicit RecordingClientOutbound(temporal::interceptor::ClientOutboundInterceptor* next)
      : temporal::interceptor::ClientOutboundInterceptor(next) {}
  std::string StartWorkflow(temporal::interceptor::StartWorkflowInput& in,
                            temporal::interceptor::Header& header) override {
    ++g_client_start_intercepts;
    return next_->StartWorkflow(in, header);
  }
};

class ClientRecordingInterceptor : public temporal::interceptor::Interceptor {
 public:
  std::unique_ptr<temporal::interceptor::ClientOutboundInterceptor> InterceptClient(
      temporal::interceptor::ClientOutboundInterceptor* next) override {
    return std::make_unique<RecordingClientOutbound>(next);
  }
};

// POSITIVE: a client-outbound interceptor (installed via ClientOptions) wraps the
// real StartWorkflow call.
TEST_F(IntegrationTest, ClientOutboundInterceptorWrapsStartWorkflow) {
  const char* addr = std::getenv("TEMPORAL_ADDRESS");
  temporal::ClientOptions opt;
  opt.target = (addr != nullptr) ? addr : "localhost:7233";
  opt.interceptors.push_back(std::make_shared<ClientRecordingInterceptor>());
  auto ic_client = temporal::client::Client::Connect(opt);

  const auto tq = UniqueTaskQueue("client-interceptor");
  g_client_start_intercepts = 0;
  temporal::worker::Worker worker(ic_client, tq);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = ic_client.StartWorkflow(o, "EchoWorkflow", std::string("hi"));
  EXPECT_EQ(h.Result<std::string>(), "hi");
  EXPECT_EQ(g_client_start_intercepts.load(), 1);  // StartWorkflow intercepted once
  worker.Stop();
}

// POSITIVE: add a compatible-redirect rule (source build id -> target) and read it
// back (gradual build-id rollout, rules-based worker versioning).
TEST_F(IntegrationTest, AddAndReadWorkerRedirectRule) {
  const auto tq = UniqueTaskQueue("vredirect") + "-" + std::to_string(std::random_device{}());
  const std::string target = "vt-" + std::to_string(std::random_device{}());
  const std::string source = "vs-" + std::to_string(std::random_device{}());
  client_->InsertWorkerAssignmentRule(tq, target);     // target = current default
  client_->AddWorkerRedirectRule(tq, source, target);  // redirect old `source` -> `target`
  const auto rules = client_->GetWorkerVersioningRules(tq);
  bool found = false;
  for (const auto& [s, t] : rules.redirect_rules) {
    if (s == source && t == target) {
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

// Captures the broadened worker metric set to assert the new names fire.
class BroadMetrics : public temporal::MetricsHandler {
 public:
  void Counter(const std::string& n, std::int64_t v, const Tags& t) override {
    if (n == "temporal_poller_start") {
      poller_starts += v;
      if (t.count("poller_type")) {
        saw_poller_type = true;
      }
    }
    if (n == "temporal_sticky_cache_hit" || n == "temporal_sticky_cache_miss") {
      cache_events += v;
    }
  }
  void Gauge(const std::string& n, double v, const Tags&) override {
    if (n == "temporal_pollers_in_flight" && v >= 1.0) {
      saw_pollers_in_flight = true;
    }
    if (n == "temporal_worker_task_slots_available") {
      saw_slots = true;
    }
  }
  void Timer(const std::string& n, std::chrono::nanoseconds d, const Tags&) override {
    if (n == "temporal_workflow_task_end_to_end_latency" && d.count() > 0) {
      saw_wf_e2e = true;
    }
    if (n == "temporal_activity_task_end_to_end_latency" && d.count() > 0) {
      saw_act_e2e = true;
    }
  }
  std::atomic<std::int64_t> poller_starts{0};
  std::atomic<std::int64_t> cache_events{0};
  std::atomic<bool> saw_poller_type{false};
  std::atomic<bool> saw_pollers_in_flight{false};
  std::atomic<bool> saw_slots{false};
  std::atomic<bool> saw_wf_e2e{false};
  std::atomic<bool> saw_act_e2e{false};
};

// POSITIVE: the broadened worker metric set fires on a simple Echo round-trip.
TEST_F(IntegrationTest, MetricsHandlerReceivesBroadenedSet) {
  const auto tq = UniqueTaskQueue("metrics-broad");
  auto m = std::make_shared<BroadMetrics>();
  temporal::WorkerOptions wo;
  wo.metrics_handler = m;
  wo.max_concurrent_activity_executions = 4;  // bound caps so slots-available emits
  wo.max_concurrent_workflow_task_executions = 4;
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "EchoWorkflow", std::string("hi"));
  EXPECT_EQ(h.Result<std::string>(), "hi");
  EXPECT_GT(m->poller_starts.load(), 0);
  EXPECT_TRUE(m->saw_poller_type.load());
  EXPECT_TRUE(m->saw_pollers_in_flight.load());
  EXPECT_TRUE(m->saw_slots.load());
  EXPECT_TRUE(m->saw_wf_e2e.load());
  EXPECT_TRUE(m->saw_act_e2e.load());
  EXPECT_GT(m->cache_events.load(), 0);
  worker.Stop();
}

// POSITIVE: a fresh dev server has no worker deployments; ListWorkerDeployments
// pages cleanly and returns an (empty) vector.
TEST_F(IntegrationTest, ListWorkerDeploymentsReturnsVector) {
  const auto names = client_->ListWorkerDeployments();
  EXPECT_TRUE(names.empty());
}

// NEGATIVE: deleting a namespace that doesn't exist exercises the real
// DeleteNamespace RPC path and surfaces a server error.
TEST_F(IntegrationTest, DeleteNamespaceUnknownThrows) {
  const auto missing = "ns-nope-" + std::to_string(std::random_device{}());
  EXPECT_THROW(client_->DeleteNamespace(missing), temporal::RpcError);
}

// POSITIVE: with demand-driven poller autoscaling (min 1, max 4) a burst of
// workflows all complete — the elastic pool scales up under load without
// dropping work, then drains cleanly on Stop.
TEST_F(IntegrationTest, PollerAutoscalingHandlesBurst) {
  const auto tq = UniqueTaskQueue("autoscale");
  temporal::WorkerOptions wo;
  wo.enable_poller_autoscaling = true;
  wo.min_concurrent_pollers = 1;
  wo.max_concurrent_pollers = 4;
  wo.autoscaling_idle_polls_before_park = 1;  // react quickly within the test
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  std::vector<temporal::client::WorkflowHandle> handles;
  for (int i = 0; i < 8; ++i) {
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    handles.push_back(client_->StartWorkflow(o, "EchoWorkflow", std::string("x")));
  }
  for (auto& h : handles) {
    EXPECT_EQ(h.Result<std::string>(), "x");
  }
  worker.Stop();
}

// POSITIVE: full session lifecycle on one worker with capacity 1 — CreateSession
// pins to the host, an Echo runs on the session queue, CompleteSession releases
// the slot so a second session can be created and used. Proves host pinning +
// create/complete bookkeeping.
TEST_F(IntegrationTest, SessionLifecyclePinsAndReleases) {
  const auto tq = UniqueTaskQueue("session");
  temporal::WorkerOptions wo;
  wo.enable_sessions = true;
  wo.max_concurrent_sessions = 1;  // 2nd session only works if the 1st is released
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("SessionLifecycleWorkflow", SessionLifecycleWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "SessionLifecycleWorkflow");
  const std::string result = h.Result<std::string>();
  worker.Stop();
  // "<host queue>|a|b": the host queue is the worker's session queue, and both
  // in-session activities ran (on that pinned host).
  EXPECT_NE(result.find("-session-"), std::string::npos);
  EXPECT_TRUE(result.ends_with("|a|b")) << "got: " << result;
}

// A type-safe activity handle (TEMPORAL_ACTIVITY): the type name "TypedIncrement"
// and the int result are deduced from the handle — no string, no explicit <R>.
int TypedIncrement(temporal::activity::Context&, int n) { return n + 1; }
TEMPORAL_ACTIVITY(TypedIncrement);

std::string TypedHandleWorkflow(temporal::workflow::Context& ctx, int base) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  const int a = ctx.ExecuteActivity(o, TypedIncrement_activity, base).Get();  // R deduced
  const int b = ctx.ExecuteActivity(o, TypedIncrement_activity, a).Get();
  return std::to_string(b);
}

// POSITIVE: registering + invoking an activity through its typed handle works
// end-to-end and is wire-identical to the string form.
TEST_F(IntegrationTest, TypedActivityHandleRoundTrip) {
  const auto tq = UniqueTaskQueue("typed-handle");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("TypedHandleWorkflow", TypedHandleWorkflow);
  worker.Register(TypedIncrement_activity);  // typed registration: name from the handle
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "TypedHandleWorkflow", 40);
  EXPECT_EQ(h.Result<std::string>(), "42");
  worker.Stop();
}

// Typed signal/query/update handles: name + type are bound in one place, so the
// channel type, sent value, and result type are checked/deduced (no strings, no
// explicit <R>).
inline constexpr temporal::SignalRef<bool> kStopSignal{"stop"};
inline constexpr temporal::QueryRef<int> kSumQuery{"sum"};
inline constexpr temporal::UpdateRef<int> kBumpUpdate{"bump"};

int TypedSquWorkflow(temporal::workflow::Context& ctx) {
  int sum = 0;
  ctx.SetQueryHandler(kSumQuery, [&sum]() -> int { return sum; });
  ctx.SetUpdateHandler(kBumpUpdate, [&sum](int by) -> int {
    sum += by;
    return sum;
  });
  ctx.GetSignalChannel(kStopSignal).Receive();  // ReceiveChannel<bool>, deduced
  return sum;
}

// POSITIVE: typed update (returns int, deduced), typed query, and a typed signal
// all round-trip end-to-end. Update/query are synchronous so the ordering is
// deterministic.
TEST_F(IntegrationTest, TypedSignalQueryUpdateHandles) {
  const auto tq = UniqueTaskQueue("typed-squ");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("TypedSquWorkflow", TypedSquWorkflow);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "TypedSquWorkflow");
  EXPECT_EQ(h.Update(kBumpUpdate, 5), 5);    // typed update; result int deduced
  EXPECT_EQ(h.Update(kBumpUpdate, 7), 12);
  EXPECT_EQ(h.Query(kSumQuery), 12);         // typed query; result int deduced
  h.Signal(kStopSignal, true);               // typed signal; bool checked
  EXPECT_EQ(h.Result<int>(), 12);
  worker.Stop();
}

// POSITIVE: replay re-application of updates. A workflow accumulates state via
// updates, then a signal completes it returning that state. After the updates are
// accepted, the original worker is stopped (discarding its sticky cache), so a
// fresh worker must replay the whole history from scratch — re-running both update
// handlers at their recorded interleaving — to reconstruct the sum. If historical
// updates were dropped on replay (the pre-fix behavior), the body would complete
// with 0 instead of 12.
TEST_F(IntegrationTest, UpdateStateReappliedOnFullReplay) {
  const auto tq = UniqueTaskQueue("update-replay");
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "TypedSquWorkflow");

  // Worker A accepts the two updates (sum -> 12) and parks the workflow on the
  // stop signal, resident in A's sticky cache. Stopping A discards that cache.
  {
    temporal::worker::Worker worker_a(*client_, tq);
    worker_a.RegisterWorkflow("TypedSquWorkflow", TypedSquWorkflow);
    worker_a.Start();
    EXPECT_EQ(h.Update(kBumpUpdate, 5), 5);   // accepted + completed in history
    EXPECT_EQ(h.Update(kBumpUpdate, 7), 12);
    std::this_thread::sleep_for(2s);          // workflow parks on the stop signal
    worker_a.Stop();
  }

  // Worker B has a cold cache. Once the stop-signal task reschedules onto the
  // normal queue (A's sticky queue times out), B replays the full history from
  // scratch: the two bump updates must be re-applied so sum is rebuilt to 12.
  temporal::worker::Worker worker_b(*client_, tq);
  worker_b.RegisterWorkflow("TypedSquWorkflow", TypedSquWorkflow);
  worker_b.Start();
  h.Signal(kStopSignal, true);
  EXPECT_EQ(h.Result<int>(), 12);   // updates reconstructed on replay (0 if dropped)
  EXPECT_GE(worker_b.replays(), 1);  // prove B replayed from scratch
  worker_b.Stop();
}

// POSITIVE: a Worker is movable — register on one, move it, and the moved-to
// worker runs workflows (the moved-from is left inert).
TEST_F(IntegrationTest, WorkerIsMovable) {
  const auto tq = UniqueTaskQueue("movable");
  temporal::worker::Worker w1(*client_, tq);
  w1.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  w1.RegisterActivity("Echo", EchoActivity);
  temporal::worker::Worker w2 = std::move(w1);  // move ctor
  w2.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "EchoWorkflow", std::string("moved"));
  EXPECT_EQ(h.Result<std::string>(), "moved");
  w2.Stop();
}

// POSITIVE: a worker started with Start() serves a workflow, and Stop() invoked
// from another thread cancels and drains cleanly (the cancellable lifecycle
// without a global SIGINT handler).
TEST_F(IntegrationTest, WorkerStopFromAnotherThread) {
  const auto tq = UniqueTaskQueue("stopthread");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "EchoWorkflow", std::string("via-run"));
  EXPECT_EQ(h.Result<std::string>(), "via-run");
  std::thread stopper([&] { worker.Stop(); });  // drains the pollers from off-thread
  stopper.join();
}

// A workflow written in the C++20 coroutine style: workflow_task + co_await + co_return.
temporal::workflow::workflow_task<std::string> CoAwaitWorkflow(temporal::workflow::Context& ctx,
                                                               std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  const std::string a = co_await ctx.ExecuteActivity<std::string>(o, "Echo", s);
  const std::string b = co_await ctx.ExecuteActivity<std::string>(o, "Echo", a + "!");
  co_return b;
}

// POSITIVE: the co_await authoring mode runs activities AND replays deterministically
// (it lowers to the same commands as the .Get() form).
TEST_F(IntegrationTest, CoAwaitWorkflowRunsAndReplays) {
  const auto tq = UniqueTaskQueue("coawait");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CoAwaitWorkflow", CoAwaitWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "CoAwaitWorkflow", std::string("hi"));
  EXPECT_EQ(h.Result<std::string>(), "hi!");
  const std::string history = h.FetchHistoryJson();
  worker.Stop();
  // Replay the coroutine workflow against its real history — proves replay-determinism.
  temporal::worker::Worker replayer(*client_, UniqueTaskQueue("coawait-rep"));
  replayer.RegisterWorkflow("CoAwaitWorkflow", CoAwaitWorkflow);
  EXPECT_NO_THROW(replayer.ReplayWorkflowHistory(history));
}

// POSITIVE: a schedule can be created with a calendar/cron spec (not just an
// interval) — the server accepts the cron_string and the schedule exists.
TEST_F(IntegrationTest, CronScheduleCreateDescribeDelete) {
  const auto sid = "cron-sched-" + std::to_string(std::random_device{}());
  temporal::ScheduleOptions opts;
  opts.cron_expressions = {"0 9 * * MON-FRI"};  // weekdays at 09:00
  opts.workflow_type = "EchoWorkflow";
  opts.task_queue = UniqueTaskQueue("cron");
  client_->CreateSchedule(sid, opts);
  EXPECT_TRUE(client_->DescribeSchedule(sid));
  client_->DeleteSchedule(sid);
}

// A child that sleeps long enough to still be running after its parent closes.
std::string PclpSleeperChild(temporal::workflow::Context& ctx) {
  ctx.Sleep(10s);
  return "child-done";
}

// Parent starts an Abandon child (caller-provided id) then returns WITHOUT
// awaiting it.
std::string PclpAbandonParent(temporal::workflow::Context& ctx, std::string child_id) {
  temporal::ChildWorkflowOptions co;
  co.id = child_id;
  co.parent_close_policy = temporal::ParentClosePolicy::Abandon;
  ctx.ExecuteChildWorkflow<std::string>(co, "PclpSleeperChild");  // fire-and-forget
  ctx.Sleep(2s);  // let the child actually start before the parent closes, so Abandon applies
  return "parent-done";
}

// POSITIVE: with ParentClosePolicy::Abandon the child outlives the parent — after
// the parent completes, the child is still running (vs Terminate, which kills it).
TEST_F(IntegrationTest, ChildParentClosePolicyAbandon) {
  const auto tq = UniqueTaskQueue("pclp");
  const auto child_id = "pclp-child-" + std::to_string(std::random_device{}());
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("PclpAbandonParent", PclpAbandonParent);
  worker.RegisterWorkflow("PclpSleeperChild", PclpSleeperChild);
  worker.Start();
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "PclpAbandonParent", child_id);
  EXPECT_EQ(h.Result<std::string>(), "parent-done");
  auto child = client_->GetHandle(child_id);
  bool running = false;
  std::string last_status = "<never described>";
  for (int i = 0; i < 25 && !running; ++i) {
    try {
      last_status = child.Describe().status;  // prefix-stripped UPPERCASE enum
      running = (last_status == "RUNNING");
    } catch (const temporal::TemporalError& e) {
      last_status = std::string("<describe threw: ") + e.what() + ">";
    }
    if (!running) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }
  EXPECT_TRUE(running) << "abandoned child should still be running; last status: " << last_status;
  if (running) {
    child.Terminate("test cleanup");
  }
  worker.Stop();
}

// Blocks the coroutine thread without ever yielding — simulates a deadlock (a
// blocking call or non-yielding loop in workflow code).
std::string DeadlockWorkflow(temporal::workflow::Context&) {
  std::this_thread::sleep_for(std::chrono::seconds(8));
  return "should-not-complete";
}

// POSITIVE: a workflow task that overruns the deadlock timeout is ABORTED (failed,
// its coroutine abandoned) so the worker keeps serving other workflows instead of
// hanging forever on the stuck task.
TEST_F(IntegrationTest, DeadlockAbortKeepsWorkerAlive) {
  const auto tq = UniqueTaskQueue("deadlock-abort");
  temporal::WorkerOptions wo;
  wo.deadlock_detection_timeout = std::chrono::milliseconds(2000);
  temporal::worker::Worker worker(*client_, tq, wo);
  worker.RegisterWorkflow("DeadlockWorkflow", DeadlockWorkflow);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();
  temporal::StartWorkflowOptions od;
  od.task_queue = tq;
  auto deadlocked = client_->StartWorkflow(od, "DeadlockWorkflow");  // task will be aborted + retried
  // Despite the deadlocking workflow, the worker stays alive and completes other work.
  temporal::StartWorkflowOptions oe;
  oe.task_queue = tq;
  auto echo = client_->StartWorkflow(oe, "EchoWorkflow", std::string("alive"));
  EXPECT_EQ(echo.Result<std::string>(), "alive");
  deadlocked.Terminate("test cleanup");
  worker.Stop();
}

}  // namespace
