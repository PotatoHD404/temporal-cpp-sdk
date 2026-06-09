// Integration test: Nexus operation call (workflow caller) + worker Nexus handler,
// end-to-end against a real Temporal dev server with Nexus enabled
// (--dynamic-config-value system.enableNexus=true). Creates a Nexus endpoint that
// targets the worker's own task queue, registers a synchronous Nexus operation
// handler on that worker, and runs a workflow that calls the operation via
// ExecuteNexusOperation; asserts the operation's result flows back. Gated behind
// TEMPORAL_INTEGRATION=1 (and TEMPORAL_ADDRESS, default localhost:7233) so the
// default `ctest` run needs no server. Skips cleanly if Nexus is disabled.
#include <random>
#include <string>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

#include "integration_fixture.h"  // IntegrationTest, UniqueTaskQueue, g_seq

namespace {

using namespace std::chrono_literals;

// The Nexus operation handler: a synchronous R(Arg) operation. Input/result are a
// single value each (one Payload), so it takes one argument and returns one value.
std::string NexusEdgeGreetOp(std::string name) { return "hello " + name; }

// The caller workflow: invokes the Nexus operation on `endpoint` and returns its
// result. The endpoint name is passed as the workflow input so the test can use a
// unique, server-persisted endpoint per run.
std::string NexusEdgeCallerWorkflow(temporal::workflow::Context& ctx, std::string endpoint) {
  return ctx.ExecuteNexusOperation<std::string>(endpoint, "svc", "op", std::string("world")).Get();
}

// POSITIVE/EDGE: a workflow calls a Nexus operation served by a worker on the same
// task queue; the operation's result must flow back to the workflow. Exercises the
// full path: ScheduleNexusOperation command -> NexusOperationScheduled ->
// PollNexusTaskQueue -> handler -> RespondNexusTaskCompleted -> NexusOperation
// Completed -> Future resolves. Skips if the dev server has Nexus disabled.
TEST_F(IntegrationTest, NexusOperationCallReturnsResult) {
  const auto tq = UniqueTaskQueue("nexus-op");
  const std::string endpoint = "itest-nexus-op-" + std::to_string(std::random_device{}());

  // Create the endpoint targeting this worker's task queue. If Nexus is disabled
  // on the dev server, skip cleanly rather than fail/hang.
  try {
    client_->CreateNexusEndpoint(endpoint, tq);
  } catch (const temporal::RpcError& e) {
    GTEST_SKIP() << "Nexus appears disabled on the dev server: " << e.what();
  }

  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("NexusEdgeCallerWorkflow", NexusEdgeCallerWorkflow);
  worker.RegisterNexusOperation("svc", "op", NexusEdgeGreetOp);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "NexusEdgeCallerWorkflow", endpoint);
  EXPECT_EQ(handle.Result<std::string>(), "hello world");  // result flowed back from the handler
  worker.Stop();
}

}  // namespace
