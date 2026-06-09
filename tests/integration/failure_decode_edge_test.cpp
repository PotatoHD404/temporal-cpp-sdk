// Integration test: client-side typed failure decode. When a workflow ends in
// WORKFLOW_EXECUTION_FAILED, WorkflowHandle::Result<R>() decodes the close event's
// failure into a WorkflowFailedError that preserves the application-failure type
// (WorkflowFailedError::type()) alongside the message, so callers can discriminate
// without parsing what(). Gated behind TEMPORAL_INTEGRATION=1 (TEMPORAL_ADDRESS,
// default localhost:7233) so the default `ctest` run needs no server.
#include <string>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

#include "integration_fixture.h"  // IntegrationTest, client_, UniqueTaskQueue

namespace {

using namespace std::chrono_literals;

// ---- activity ------------------------------------------------------------

// Throws a typed ApplicationError so the close failure carries a message + type.
std::string FailureDecodeBoomActivity(temporal::activity::Context&, std::string) {
  throw temporal::ApplicationError("boom", "MyFailureType");
}

// ---- workflow ------------------------------------------------------------

// Runs the failing activity with a single attempt (no retries) and lets the
// failure propagate, so the workflow itself fails fast.
std::string FailureDecodeWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 1};  // single shot, then fail
  return ctx.ExecuteActivity<std::string>(o, "FailureDecodeBoom", s).Get();
}

// ---- test ----------------------------------------------------------------

// NEGATIVE/EDGE: a workflow fails by propagating a typed activity failure. The
// client decodes the close event into a WorkflowFailedError whose message
// reflects the failure ("boom") and whose type() is populated (preserved from the
// close failure's application_failure_info), proving callers can discriminate.
TEST_F(IntegrationTest, FailureDecodePreservesTypeAndMessage) {
  const auto tq = UniqueTaskQueue("faildecode-type");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("FailureDecodeWorkflow", FailureDecodeWorkflow);
  worker.RegisterActivity("FailureDecodeBoom", FailureDecodeBoomActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto h = client_->StartWorkflow(o, "FailureDecodeWorkflow", std::string("x"));
  try {
    h.Result<std::string>();
    FAIL() << "expected the workflow to fail";
  } catch (const temporal::WorkflowFailedError& e) {
    EXPECT_NE(std::string(e.what()).find("boom"), std::string::npos);  // message preserved
    EXPECT_FALSE(e.type().empty());  // application-failure type decoded onto the typed exception
  }
  worker.Stop();
}

}  // namespace
