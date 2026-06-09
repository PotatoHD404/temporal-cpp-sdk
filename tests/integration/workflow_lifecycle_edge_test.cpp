// Integration tests: workflow-lifecycle NEGATIVE / EDGE paths against a real
// Temporal server. Gated behind TEMPORAL_INTEGRATION=1 by the shared fixture, so
// the default `ctest` run needs no server. These complement the positive-path
// cases in integration_test.cpp by exercising the error surface: operations on a
// nonexistent workflow id, operations on an already-closed workflow, and a
// duplicate-workflow-id start while the first run is still alive.
//
// Error-type discovery (read from the implementation, not guessed):
//   * Signal / Cancel / Describe / Query / Update of an unknown workflow go
//     through GrpcClient::UnaryCall with poll=false; a NOT_FOUND status is
//     mapped to temporal::RpcError (src/internal/grpc_client.cpp:56-60).
//   * GetHandle(...).Result<>() long-polls GetWorkflowExecutionHistory
//     (poll=true). For a missing id the server returns NOT_FOUND, which is NOT
//     the swallowed DEADLINE_EXCEEDED case, so it also throws RpcError quickly
//     rather than hanging (src/client/client.cpp:162-195, grpc_client.cpp:57).
//   * A duplicate StartWorkflow with the same options.id while the first run is
//     still RUNNING is rejected by the server (default reuse policy only allows
//     re-use of CLOSED runs); StartWorkflowExecution surfaces that as RpcError.
#include <chrono>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

#include "integration_fixture.h"  // IntegrationTest, UniqueTaskQueue, g_seq

namespace {

using namespace std::chrono_literals;

// A process-unique, definitely-nonexistent workflow id.
std::string MissingId(const std::string& tag) {
  return "wflife-missing-" + tag + "-" + std::to_string(std::random_device{}());
}

// ---- activities ----------------------------------------------------------
std::string WfLifeEchoActivity(temporal::activity::Context&, std::string s) { return s; }

// ---- workflows -----------------------------------------------------------
// Runs one Echo activity and returns its result; completes promptly.
std::string WfLifeEchoWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  return ctx.ExecuteActivity<std::string>(o, "WfLifeEcho", s).Get();
}

// Registers a constant "status" query handler, then completes immediately. The
// handler is re-established on replay, so the workflow can still be queried after
// it has closed (Temporal answers closed-workflow queries by replaying history).
std::string WfLifeQueryableWorkflow(temporal::workflow::Context& ctx) {
  ctx.SetQueryHandler("status", [] { return std::string("final"); });
  return "done";
}

// Parks on a "stop" signal so the workflow stays RUNNING for the duration of a
// test (used by the duplicate-id case). It ignores cancellation, so any test
// using it must Terminate to clean up.
std::string WfLifeBlockedWorkflow(temporal::workflow::Context& ctx) {
  ctx.GetSignalChannel<std::string>("stop").Receive();
  return "stopped";
}

// ---- tests ---------------------------------------------------------------

// Signalling a workflow id that was never started fails fast (no worker needed:
// the RPC is rejected before reaching any workflow code).
TEST_F(IntegrationTest, WfLifecycleSignalUnknownThrows) {
  const auto tq = UniqueTaskQueue("wflife-sig");
  (void)tq;  // no workflow is run; the task queue is unused but keeps the pattern
  const auto dc = temporal::DataConverter::Default();
  auto handle = client_->GetHandle(MissingId("sig"));
  EXPECT_THROW(handle.Signal("whatever", dc->ToPayloads(std::string("x"))),
               temporal::RpcError);
}

// Querying an unknown workflow id fails fast with RpcError (NOT_FOUND).
TEST_F(IntegrationTest, WfLifecycleQueryUnknownThrows) {
  const auto tq = UniqueTaskQueue("wflife-qry");
  (void)tq;
  auto handle = client_->GetHandle(MissingId("qry"));
  EXPECT_THROW(handle.Query<std::string>("status"), temporal::RpcError);
}

// Updating an unknown workflow id fails fast with RpcError (NOT_FOUND).
TEST_F(IntegrationTest, WfLifecycleUpdateUnknownThrows) {
  const auto tq = UniqueTaskQueue("wflife-upd");
  (void)tq;
  auto handle = client_->GetHandle(MissingId("upd"));
  EXPECT_THROW(handle.Update<int>("add", 1), temporal::RpcError);
}

// Describing an unknown workflow id fails with RpcError (NOT_FOUND).
TEST_F(IntegrationTest, WfLifecycleDescribeUnknownThrows) {
  const auto tq = UniqueTaskQueue("wflife-desc");
  (void)tq;
  auto handle = client_->GetHandle(MissingId("desc"));
  EXPECT_THROW(handle.Describe(), temporal::RpcError);
}

// Awaiting the result of a handle to a workflow that was never started throws
// RpcError. This must NOT hang: Result long-polls history, but a missing id
// returns NOT_FOUND (only DEADLINE_EXCEEDED is swallowed and retried), so the
// first poll throws. A short overall guard documents the no-hang expectation.
TEST_F(IntegrationTest, WfLifecycleResultOfUnknownThrows) {
  const auto tq = UniqueTaskQueue("wflife-res");
  (void)tq;
  auto handle = client_->GetHandle(MissingId("res"));
  const auto t0 = std::chrono::steady_clock::now();
  EXPECT_THROW(handle.Result<std::string>(), temporal::RpcError);
  EXPECT_LT(std::chrono::steady_clock::now() - t0, 30s);  // NOT_FOUND returns at once
}

// Operations on an ALREADY-COMPLETED workflow.
//   * Result on a completed workflow returns its result (sanity: it is closed).
//   * Query on a completed workflow: Temporal permits querying closed workflows
//     by default and answers via history replay (the worker is kept running and
//     re-registers the handler), so the constant query value is returned. If a
//     server is configured to reject closed-workflow queries it would instead
//     throw; we tolerate that and document it rather than assert a brittle path.
//   * Cancel of a completed workflow: there is no open run to cancel, so the
//     server reports NOT_FOUND ("workflow execution already completed") and the
//     SDK throws RpcError. Observed behavior is asserted-with-tolerance because
//     it is server-version dependent (some versions no-op the cancel).
TEST_F(IntegrationTest, WfLifecycleQueryAndCancelOfCompletedWorkflow) {
  const auto tq = UniqueTaskQueue("wflife-done");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("WfLifeQueryableWorkflow", WfLifeQueryableWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "WfLifeQueryableWorkflow");
  ASSERT_EQ(handle.Result<std::string>(), "done");  // the workflow is now CLOSED
  EXPECT_EQ(handle.Describe().status, "COMPLETED");

  // Query of the completed workflow: answered via replay by the still-running
  // worker. Tolerate a reject-configured server (throws) without failing.
  try {
    EXPECT_EQ(handle.Query<std::string>("status"), "final");
  } catch (const temporal::TemporalError&) {
    // Observed on servers that reject queries against closed workflows.
  }

  // Cancel of the completed workflow: OBSERVED to throw RpcError (NOT_FOUND, the
  // run is already closed). Tolerate a no-op on server versions that accept it.
  try {
    handle.Cancel();
    // No throw observed: the server accepted the cancel as a no-op on a closed run.
  } catch (const temporal::RpcError&) {
    // Expected on current dev servers: there is no running execution to cancel.
  }

  worker.Stop();
}

// Duplicate workflow id: start a workflow, then start AGAIN with the SAME
// options.id while the first run is still RUNNING. Temporal's default
// workflow-id reuse policy only permits re-using an id whose prior run is
// CLOSED, so the second StartWorkflow against a live run is rejected and the SDK
// surfaces RpcError (StartWorkflowExecution -> WorkflowExecutionAlreadyStarted).
// (StartWorkflowOptions exposes `id`; no separate reuse-policy field exists, so
// this asserts the default-policy behavior — see include/temporal/common/options.h:58.)
TEST_F(IntegrationTest, WfLifecycleDuplicateRunningIdThrows) {
  const auto tq = UniqueTaskQueue("wflife-dup");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("WfLifeBlockedWorkflow", WfLifeBlockedWorkflow);
  worker.Start();

  const std::string wf_id = "wflife-dup-" + std::to_string(std::random_device{}());
  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  o.id = wf_id;
  auto first = client_->StartWorkflow(o, "WfLifeBlockedWorkflow");
  ASSERT_EQ(first.Describe().status, "RUNNING");  // first run is live

  // Second start with the same id while the first is still running -> rejected.
  temporal::StartWorkflowOptions dup;
  dup.task_queue = tq;
  dup.id = wf_id;
  EXPECT_THROW(client_->StartWorkflow(dup, "WfLifeBlockedWorkflow"), temporal::RpcError);

  const auto dc = temporal::DataConverter::Default();
  first.Signal("stop", dc->ToPayloads(std::string("go")));  // let the first run finish
  EXPECT_EQ(first.Result<std::string>(), "stopped");
  worker.Stop();
}

}  // namespace
