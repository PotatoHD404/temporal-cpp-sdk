// Edge-path integration tests for child workflows, continue-as-new, and
// determinism/replay — the positive and negative boundaries that complement the
// happy-path cases in integration_test.cpp:
//   * child-workflow FAILURE propagates to the parent (vs. the success case
//     ChildWorkflowReturnsResultToParent),
//   * continue-as-new carries a TRANSFORMED input forward (vs. the
//     count-to-zero chain in ContinueAsNewChainsToCompletion),
//   * a correct multi-activity history replays clean (positive complement to
//     ReplayFrameworkDetectsNonDeterministicChange), and
//   * WorkerOptions::panic_policy is selectable without breaking a good workflow.
// Gated behind TEMPORAL_INTEGRATION=1 (TEMPORAL_ADDRESS overrides the default
// localhost:7233) like the rest of the integration suite.
#include <chrono>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

#include "integration_fixture.h"  // IntegrationTest, UniqueTaskQueue, g_seq

namespace {

using namespace std::chrono_literals;

// ---- activities ----------------------------------------------------------
// Throws so the child workflow that awaits it fails. maximum_attempts on the
// caller keeps it from retrying forever (which would hang the test).
std::string CcdBoomActivity(temporal::activity::Context&, std::string) {
  throw temporal::ApplicationError("ccd-boom", "CcdBoomError");
}

int CcdAddOneActivity(temporal::activity::Context&, int n) { return n + 1; }

// ---- workflows -----------------------------------------------------------
// A child whose single activity throws (fail-fast: one attempt) so the failure
// surfaces to whoever awaits the child.
std::string CcdFailingChildWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  // fail fast; the default policy retries forever.
  o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 1};
  return ctx.ExecuteActivity<std::string>(o, "CcdBoom", s).Get();
}

// Parent that runs the failing child and CATCHES the propagated failure, then
// returns a sentinel — proving the parent observed the child's failure instead
// of receiving a result. A failed child future surfaces in workflow code as a
// temporal::ActivityError (see workflow/future.h Get()).
std::string CcdCatchingParentWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ChildWorkflowOptions o;
  o.task_queue = ctx.GetInfo().task_queue;
  auto child = ctx.ExecuteChildWorkflow<std::string>(o, "CcdFailingChild", name);
  try {
    child.Get();
    return "no-failure";  // unreachable when the child fails
  } catch (const temporal::TemporalError&) {
    return "caught-child-failure";
  }
}

// Parent that runs the failing child and lets the failure propagate, so the
// PARENT execution itself fails (client sees WorkflowFailedError).
std::string CcdPropagatingParentWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ChildWorkflowOptions o;
  o.task_queue = ctx.GetInfo().task_queue;
  return ctx.ExecuteChildWorkflow<std::string>(o, "CcdFailingChild", name).Get();
}

// Continue-as-new that TRANSFORMS its input on the first run and returns the
// transformed value on the second. The first run is distinguished by the
// untransformed marker prefix; the next run sees the new argument and returns
// it, so the final result must reflect the carried-forward (transformed) input.
std::string CcdTransformOnceWorkflow(temporal::workflow::Context& ctx, std::string in) {
  if (in.rfind("seed:", 0) == 0) {
    ctx.ContinueAsNew("CcdTransformOnce", "carried:" + in.substr(5));
  }
  return in;  // second run: returns the value carried forward by continue-as-new
}

// Multi-activity workflow whose history is exported and replayed against the
// same code (positive determinism). Each activity is recorded in history.
int CcdMultiActivityWorkflow(temporal::workflow::Context& ctx, int start) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  int value = start;
  for (int i = 0; i < 3; ++i) {
    value = ctx.ExecuteActivity<int>(o, "CcdAddOne", value).Get();
  }
  return value;
}

// A plain, deterministic workflow used to prove a non-default panic_policy does
// not disturb a workflow that never diverges from its history.
int CcdSimpleAddWorkflow(temporal::workflow::Context& ctx, int start) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  return ctx.ExecuteActivity<int>(o, "CcdAddOne", start).Get();
}

// ---- tests ---------------------------------------------------------------

// A child workflow whose activity throws fails, and the PARENT observes that
// failure (here by catching it and returning a sentinel).
TEST_F(IntegrationTest, ChildCanDetChildFailurePropagatesToParent) {
  const auto tq = UniqueTaskQueue("ccd-childfail");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CcdCatchingParent", CcdCatchingParentWorkflow);
  worker.RegisterWorkflow("CcdFailingChild", CcdFailingChildWorkflow);
  worker.RegisterActivity("CcdBoom", CcdBoomActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CcdCatchingParent", std::string("World"));
  EXPECT_EQ(handle.Result<std::string>(), "caught-child-failure");
  worker.Stop();
}

// When the parent does NOT catch it, the child's failure propagates out and the
// parent execution itself fails (client sees WorkflowFailedError).
TEST_F(IntegrationTest, ChildCanDetUncaughtChildFailureFailsParent) {
  const auto tq = UniqueTaskQueue("ccd-childfail2");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CcdPropagatingParent", CcdPropagatingParentWorkflow);
  worker.RegisterWorkflow("CcdFailingChild", CcdFailingChildWorkflow);
  worker.RegisterActivity("CcdBoom", CcdBoomActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CcdPropagatingParent", std::string("World"));
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  worker.Stop();
}

// Continue-as-new carries a NEW (transformed) input forward: the first run
// rewrites its argument and restarts; the second run returns the carried value,
// so the final result reflects the transformed input, not the original.
TEST_F(IntegrationTest, ChildCanDetContinueAsNewCarriesNewInput) {
  const auto tq = UniqueTaskQueue("ccd-can");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("CcdTransformOnce", CcdTransformOnceWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CcdTransformOnce", std::string("seed:abc"));
  // The client follows the continue-as-new chain; the value reflects the input
  // rewritten on the first run ("seed:abc" -> "carried:abc"), proving the new
  // argument was carried forward rather than the original being returned.
  EXPECT_EQ(handle.Result<std::string>(), "carried:abc");
  worker.Stop();
}

// A correct multi-activity workflow run to completion replays clean against the
// SAME registered code: FetchHistoryJson then ReplayWorkflowHistory (no server
// contact, the replayer is never Start()ed) must not throw.
TEST_F(IntegrationTest, ChildCanDetReplayOfGoodHistorySucceeds) {
  const auto tq = UniqueTaskQueue("ccd-replay");
  std::string history_json;
  {
    temporal::worker::Worker worker(*client_, tq);
    worker.RegisterWorkflow("CcdMultiActivity", CcdMultiActivityWorkflow);
    worker.RegisterActivity("CcdAddOne", CcdAddOneActivity);
    worker.Start();
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    auto handle = client_->StartWorkflow(o, "CcdMultiActivity", 10);
    EXPECT_EQ(handle.Result<int>(), 13);     // 10 + 1 + 1 + 1
    history_json = handle.FetchHistoryJson();  // export the real history
    worker.Stop();
  }
  ASSERT_FALSE(history_json.empty());

  // Same code -> deterministic replay (no RPCs; the worker is never started).
  temporal::worker::Worker replayer(*client_, tq);
  replayer.RegisterWorkflow("CcdMultiActivity", CcdMultiActivityWorkflow);
  replayer.RegisterActivity("CcdAddOne", CcdAddOneActivity);
  EXPECT_NO_THROW(replayer.ReplayWorkflowHistory(history_json));
}

// WorkerOptions::panic_policy is selectable (here FailWorkflow): a deterministic
// workflow that never diverges from its history completes normally regardless of
// which non-determinism policy is chosen.
TEST_F(IntegrationTest, ChildCanDetFailWorkflowPanicPolicyHonoursGoodWorkflow) {
  const auto tq = UniqueTaskQueue("ccd-panic");
  temporal::WorkerOptions wopts;
  wopts.panic_policy = temporal::WorkflowPanicPolicy::FailWorkflow;
  temporal::worker::Worker worker(*client_, tq, wopts);
  worker.RegisterWorkflow("CcdSimpleAdd", CcdSimpleAddWorkflow);
  worker.RegisterActivity("CcdAddOne", CcdAddOneActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "CcdSimpleAdd", 7);
  EXPECT_EQ(handle.Result<int>(), 8);  // 7 + 1: good workflow unaffected by the policy
  worker.Stop();
}

}  // namespace
