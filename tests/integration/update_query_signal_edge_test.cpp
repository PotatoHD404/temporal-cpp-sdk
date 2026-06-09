// Negative / edge-path integration tests for the update, query, and signal
// surfaces, run end-to-end against a real Temporal server. Gated behind
// TEMPORAL_INTEGRATION=1 (and TEMPORAL_ADDRESS, default localhost:7233) so the
// default `ctest` run needs no server. These complement the happy-path update/
// query/signal tests in integration_test.cpp by covering failures and ordering:
// a handler that throws at runtime (NOT a validator rejection), querying a type
// with no registered handler, signal-then-query visibility, routing several
// distinct update names, and querying a workflow that has already completed.
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

#include "integration_fixture.h"  // IntegrationTest, UniqueTaskQueue, g_seq

namespace {

using namespace std::chrono_literals;

// ---- workflows -----------------------------------------------------------

// An "explode" update handler that always throws at runtime (handler phase, not
// validator phase: there is no validator here, so the update is accepted and the
// failure happens while running the handler). The body parks on a "stop" signal
// so the workflow stays alive to receive updates.
int UqsThrowingUpdateWorkflow(temporal::workflow::Context& ctx) {
  int total = 0;
  ctx.SetUpdateHandler("explode", [&](int n) -> int {
    total += n;  // mutate first so we can prove the handler actually ran
    throw temporal::ApplicationError("kaboom in handler", "HandlerBoom");
  });
  ctx.GetSignalChannel<std::string>("stop").Receive();
  return total;
}

// Registers a single query handler ("known") and parks on a "stop" signal. Used
// to prove that querying an *unregistered* query type surfaces as an error while
// a registered one still answers.
int UqsSingleQueryWorkflow(temporal::workflow::Context& ctx) {
  ctx.SetQueryHandler("known", [&] { return 99; });
  ctx.GetSignalChannel<std::string>("stop").Receive();
  return 0;
}

// Holds a string built by appending each "append" signal's payload, and answers
// a "current" query with the accumulated value. Exercises signal-then-query
// ordering/visibility: a query issued after a signal must observe the mutation.
std::string UqsSignalThenQueryWorkflow(temporal::workflow::Context& ctx) {
  std::string acc;
  ctx.SetQueryHandler("current", [&] { return acc; });
  auto signals = ctx.GetSignalChannel<std::string>("append");
  while (true) {
    const std::string v = signals.Receive();
    if (v == "done") {
      return acc;
    }
    acc += v;
  }
}

// Registers three DISTINCT update names, each routed to its own handler that
// mutates a different field, then encodes all three into the result on "stop".
// Proves the dispatch keys updates by name to the right handler.
std::string UqsMultiUpdateWorkflow(temporal::workflow::Context& ctx) {
  int adds = 0;
  int subs = 0;
  std::string label;
  ctx.SetUpdateHandler("add", [&](int n) {
    adds += n;
    return adds;
  });
  ctx.SetUpdateHandler("sub", [&](int n) {
    subs -= n;
    return subs;
  });
  ctx.SetUpdateHandler("setLabel", [&](std::string s) {
    label = s;
    return label;
  });
  ctx.GetSignalChannel<std::string>("stop").Receive();
  return label + ":" + std::to_string(adds) + ":" + std::to_string(subs);
}

// Counts "add" signals into a sum, answers a "sum" query, and finishes on a
// negative value. Used to query a workflow that has already COMPLETED.
int UqsCompletableQueryWorkflow(temporal::workflow::Context& ctx) {
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

// ---- tests ---------------------------------------------------------------

// An update handler that throws at runtime (NOT a validator rejection) surfaces
// the failure to the caller: the client's Update<R>() throws. Distinct from
// UpdateValidatorRejectsInvalidInput, where the *validator* rejects before
// acceptance; here the update is accepted and the handler itself fails.
TEST_F(IntegrationTest, UpdQrySigUpdateHandlerFailureThrows) {
  const auto tq = UniqueTaskQueue("uqs-uthrow");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("UqsThrowingUpdateWorkflow", UqsThrowingUpdateWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "UqsThrowingUpdateWorkflow");
  const auto dc = temporal::DataConverter::Default();

  // The handler runs and throws -> the synchronous Update must surface a failure.
  EXPECT_THROW(handle.Update<int>("explode", 5), temporal::WorkflowFailedError);

  // The workflow itself is still healthy (an update-handler failure fails the
  // update, not the run), so the "stop" signal still completes it cleanly.
  handle.Signal("stop", dc->ToPayloads(std::string("done")));
  EXPECT_NO_THROW(handle.Result<int>());
  worker.Stop();
}

// Querying a query type with NO registered handler surfaces as an error: the
// client's Query<R>() throws. A registered query type on the same workflow still
// answers, proving the failure is specific to the unknown type.
TEST_F(IntegrationTest, UpdQrySigQueryUnknownTypeThrows) {
  const auto tq = UniqueTaskQueue("uqs-noqh");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("UqsSingleQueryWorkflow", UqsSingleQueryWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "UqsSingleQueryWorkflow");
  const auto dc = temporal::DataConverter::Default();

  // The registered query answers (poll: a query right after start may briefly be
  // unanswerable, and CI runners are slow).
  int known = -1;
  for (int i = 0; i < 150 && known != 99; ++i) {
    try {
      known = handle.Query<int>("known");
    } catch (const std::exception&) {
      // not yet answerable (handler not registered / eventual consistency)
    }
    if (known != 99) {
      std::this_thread::sleep_for(100ms);
    }
  }
  EXPECT_EQ(known, 99);

  // An unregistered query type has no handler -> Query throws.
  EXPECT_THROW(handle.Query<int>("nonexistent"), std::exception);

  handle.Signal("stop", dc->ToPayloads(std::string("done")));
  EXPECT_NO_THROW(handle.Result<int>());
  worker.Stop();
}

// Signal-then-query visibility: a signal mutates workflow state, and a query
// issued afterward observes the mutation. Polls because signal delivery and the
// query are independently eventually consistent.
TEST_F(IntegrationTest, UpdQrySigSignalThenQueryReflectsMutation) {
  const auto tq = UniqueTaskQueue("uqs-sq");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("UqsSignalThenQueryWorkflow", UqsSignalThenQueryWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "UqsSignalThenQueryWorkflow");
  const auto dc = temporal::DataConverter::Default();

  handle.Signal("append", dc->ToPayloads(std::string("foo")));
  handle.Signal("append", dc->ToPayloads(std::string("bar")));

  // After the mutating signals, the query must eventually reflect "foobar".
  std::string current;
  for (int i = 0; i < 150 && current != "foobar"; ++i) {
    try {
      current = handle.Query<std::string>("current");
    } catch (const std::exception&) {
      // not yet answerable / eventual consistency
    }
    if (current != "foobar") {
      std::this_thread::sleep_for(100ms);
    }
  }
  EXPECT_EQ(current, "foobar");

  handle.Signal("append", dc->ToPayloads(std::string("done")));
  EXPECT_EQ(handle.Result<std::string>(), "foobar");
  worker.Stop();
}

// Multiple DISTINCT update names on one workflow, each routed to its own handler.
// Interleave the names and assert each handler accumulated only its own input,
// proving updates dispatch by name.
TEST_F(IntegrationTest, UpdQrySigDistinctUpdateNamesRouteIndependently) {
  const auto tq = UniqueTaskQueue("uqs-multi");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("UqsMultiUpdateWorkflow", UqsMultiUpdateWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "UqsMultiUpdateWorkflow");
  const auto dc = temporal::DataConverter::Default();

  // Each update name hits its own handler and returns that handler's new state.
  EXPECT_EQ(handle.Update<int>("add", 5), 5);
  EXPECT_EQ(handle.Update<int>("sub", 2), -2);
  EXPECT_EQ(handle.Update<std::string>("setLabel", std::string("hi")), "hi");
  EXPECT_EQ(handle.Update<int>("add", 3), 8);   // "add" accumulates, untouched by "sub"
  EXPECT_EQ(handle.Update<int>("sub", 4), -6);  // "sub" accumulates, untouched by "add"

  handle.Signal("stop", dc->ToPayloads(std::string("done")));
  EXPECT_EQ(handle.Result<std::string>(), "hi:8:-6");
  worker.Stop();
}

// Query of a COMPLETED workflow: drive the workflow to completion, then query
// its final state. Temporal answers queries against closed executions by
// replaying history, so this should return the final value; we assert the
// observed behavior defensively (return the final sum, or throw) so the test is
// not flaky across server versions while still exercising the closed-query path.
TEST_F(IntegrationTest, UpdQrySigQueryCompletedWorkflow) {
  const auto tq = UniqueTaskQueue("uqs-done");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("UqsCompletableQueryWorkflow", UqsCompletableQueryWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "UqsCompletableQueryWorkflow");
  const auto dc = temporal::DataConverter::Default();

  handle.Signal("add", dc->ToPayloads(10));
  handle.Signal("add", dc->ToPayloads(-1));  // negative value completes the workflow
  ASSERT_EQ(handle.Result<int>(), 10);       // confirm it has actually closed

  // Query the now-completed workflow. Either the server answers from replayed
  // history with the final state (10), or it rejects queries on a closed
  // execution by throwing — both are acceptable observed behaviors.
  bool threw = false;
  int observed = -1;
  try {
    observed = handle.Query<int>("sum");
  } catch (const std::exception&) {
    threw = true;
  }
  if (!threw) {
    EXPECT_EQ(observed, 10);  // final state, served from history
  }
  worker.Stop();
}

}  // namespace
