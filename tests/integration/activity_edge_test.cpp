// Integration tests: activity retry/timeout/failure edge paths. Exercises the
// RetryPolicy wiring (retry-then-succeed, exhaustion, non-retryable short-circuit)
// and the start-to-close timeout, all end-to-end against a real Temporal server.
// Gated behind TEMPORAL_INTEGRATION=1 (and TEMPORAL_ADDRESS, default
// localhost:7233) so the default `ctest` run needs no server.
#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

#include "integration_fixture.h"  // IntegrationTest, UniqueTaskQueue, g_seq

namespace {

using namespace std::chrono_literals;

// File-scope attempt counters: incremented at the TOP of each activity body, so
// the value after the workflow settles equals the number of activity attempts
// the server actually dispatched. Each test resets its own counter first.
std::atomic<int> g_flaky_attempts{0};
std::atomic<int> g_always_attempts{0};
std::atomic<int> g_nonretry_attempts{0};
std::atomic<int> g_timeout_attempts{0};

// ---- activities ----------------------------------------------------------

// Throws a RETRYABLE ApplicationError on the first two attempts, then succeeds.
// Drives the retry-then-succeed path; the workflow should see the success value.
std::string ActEdgeFlakyActivity(temporal::activity::Context&, std::string s) {
  const int attempt = g_flaky_attempts.fetch_add(1) + 1;  // 1-based
  if (attempt < 3) {
    throw temporal::ApplicationError("flaky, retry me", "FlakyEdge");  // retryable
  }
  return s;
}

// ALWAYS throws a retryable error. With maximum_attempts=3 the server stops after
// the third attempt and the workflow fails; the counter then reads exactly 3.
std::string ActEdgeAlwaysFailsActivity(temporal::activity::Context&, std::string) {
  g_always_attempts.fetch_add(1);
  throw temporal::ApplicationError("always boom", "AlwaysEdge");  // retryable
}

// Throws a NON-RETRYABLE ApplicationError (non_retryable=true), so the server
// must not schedule a second attempt regardless of the retry policy.
std::string ActEdgeNonRetryableActivity(temporal::activity::Context&, std::string) {
  g_nonretry_attempts.fetch_add(1);
  throw temporal::ApplicationError("do not retry", "NonRetryableEdge", /*non_retryable=*/true);
}

// Sleeps well past its start_to_close_timeout so the server times the attempt
// out. Reports each attempt; with maximum_attempts=1 it is dispatched once.
std::string ActEdgeSlowActivity(temporal::activity::Context&, int) {
  g_timeout_attempts.fetch_add(1);
  std::this_thread::sleep_for(3s);  // > the 1s start_to_close_timeout below
  return "too-late";
}

// ---- workflows -----------------------------------------------------------

// Generous retry budget + tiny interval so the flaky activity's two failures are
// retried fast and attempt 3 succeeds within the test.
std::string ActEdgeRetrySucceedWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  o.retry_policy = temporal::RetryPolicy{
      .initial_interval = 10ms, .maximum_interval = 50ms, .maximum_attempts = 5};
  return ctx.ExecuteActivity<std::string>(o, "ActEdgeFlaky", s).Get();
}

// Caps retries at 3 so the always-failing activity exhausts and the workflow
// fails (instead of the default unlimited policy, which would never settle).
std::string ActEdgeRetryExhaustWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  o.retry_policy = temporal::RetryPolicy{
      .initial_interval = 10ms, .maximum_interval = 50ms, .maximum_attempts = 3};
  return ctx.ExecuteActivity<std::string>(o, "ActEdgeAlwaysFails", s).Get();
}

// A high attempt cap deliberately proves the non_retryable flag short-circuits
// retries on its own: even with room for 5 attempts, only 1 should run.
std::string ActEdgeNonRetryableWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  // room to retry; the non_retryable flag must veto it on its own.
  o.retry_policy = temporal::RetryPolicy{.initial_interval = 10ms, .maximum_attempts = 5};
  return ctx.ExecuteActivity<std::string>(o, "ActEdgeNonRetryable", s).Get();
}

// Small start_to_close_timeout vs. a long activity sleep, with no extra attempts,
// so the workflow surfaces the timeout failure promptly.
std::string ActEdgeTimeoutWorkflow(temporal::workflow::Context& ctx, int n) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 1s;  // activity sleeps ~3s -> times out
  o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 1};  // single shot
  return ctx.ExecuteActivity<std::string>(o, "ActEdgeSlow", n).Get();
}

// ---- tests ---------------------------------------------------------------

// NEGATIVE/EDGE: a retryable failure on attempts 1-2 is retried by the server and
// attempt 3 succeeds; the workflow gets the result AND the counter proves it
// retried (== 3, not 1).
TEST_F(IntegrationTest, ActivityEdgeRetryThenSucceeds) {
  g_flaky_attempts.store(0);
  const auto tq = UniqueTaskQueue("actedge-retry");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ActEdgeRetrySucceedWorkflow", ActEdgeRetrySucceedWorkflow);
  worker.RegisterActivity("ActEdgeFlaky", ActEdgeFlakyActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ActEdgeRetrySucceedWorkflow", std::string("payload"));
  EXPECT_EQ(handle.Result<std::string>(), "payload");  // succeeded on attempt 3
  EXPECT_EQ(g_flaky_attempts.load(), 3);               // and it actually retried twice
  worker.Stop();
}

// NEGATIVE/EDGE: an always-retryable failure with maximum_attempts=3 exhausts the
// budget; the workflow fails and the activity ran exactly 3 times.
TEST_F(IntegrationTest, ActivityEdgeRetryExhaustsAndFails) {
  g_always_attempts.store(0);
  const auto tq = UniqueTaskQueue("actedge-exhaust");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ActEdgeRetryExhaustWorkflow", ActEdgeRetryExhaustWorkflow);
  worker.RegisterActivity("ActEdgeAlwaysFails", ActEdgeAlwaysFailsActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ActEdgeRetryExhaustWorkflow", std::string("x"));
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  EXPECT_EQ(g_always_attempts.load(), 3);  // stopped at exactly maximum_attempts
  worker.Stop();
}

// NEGATIVE/EDGE: a non_retryable=true ApplicationError short-circuits the retry
// policy. Despite maximum_attempts=5, the activity runs exactly once and the
// workflow fails. (Distinct from ActivityFailurePropagatesWithMaxOneAttempt,
// which fails fast via maximum_attempts=1; here the 1-attempt cap comes from the
// non_retryable flag, asserted on the attempt counter.)
TEST_F(IntegrationTest, ActivityEdgeNonRetryableRunsOnce) {
  g_nonretry_attempts.store(0);
  const auto tq = UniqueTaskQueue("actedge-nonretry");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ActEdgeNonRetryableWorkflow", ActEdgeNonRetryableWorkflow);
  worker.RegisterActivity("ActEdgeNonRetryable", ActEdgeNonRetryableActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ActEdgeNonRetryableWorkflow", std::string("x"));
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  EXPECT_EQ(g_nonretry_attempts.load(), 1);  // non_retryable vetoed the 5-attempt budget
  worker.Stop();
}

// NEGATIVE/EDGE: an activity that sleeps ~3s under a 1s start_to_close_timeout is
// timed out by the server, surfacing a failure on the workflow side. Bounded:
// the single attempt times out at ~1s, so the workflow settles in a few seconds.
TEST_F(IntegrationTest, ActivityEdgeStartToCloseTimeoutFails) {
  g_timeout_attempts.store(0);
  const auto tq = UniqueTaskQueue("actedge-timeout");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ActEdgeTimeoutWorkflow", ActEdgeTimeoutWorkflow);
  worker.RegisterActivity("ActEdgeSlow", ActEdgeSlowActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ActEdgeTimeoutWorkflow", 0);
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  EXPECT_GE(g_timeout_attempts.load(), 1);  // the slow attempt was dispatched
  worker.Stop();
}

}  // namespace
