// Integration tests: NEGATIVE / EDGE paths for schedules, batch operations, and
// visibility queries against a real Temporal server. These complement the
// positive flows in integration_test.cpp (ScheduleCreateDescribeDelete,
// BatchTerminateByQuery, ListAndCountWorkflows, ...). Gated behind
// TEMPORAL_INTEGRATION=1 (TEMPORAL_ADDRESS overrides localhost:7233) via the
// shared IntegrationTest fixture, so the default `ctest` run needs no server.
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

#include "integration_fixture.h"  // IntegrationTest, UniqueTaskQueue, g_seq

namespace {

using namespace std::chrono_literals;

// A workflow that sleeps long enough to stay Running while a batch operation or
// visibility query observes it. Named uniquely so it doesn't collide with the
// LongSleepWorkflow registered by integration_test.cpp.
std::string EdgeLongSleepWorkflow(temporal::workflow::Context& ctx, int) {
  ctx.Sleep(60s);
  return "done";
}

// EDGE: deleting a schedule that was never created. The SDK passes the raw
// DeleteSchedule RPC straight through (src/client/client.cpp:456) with no
// not-found swallowing, and GrpcClient::UnaryCall turns any non-OK status into
// RpcError (src/internal/grpc_client.cpp:60). The Temporal server's behavior for
// deleting a missing schedule id has varied across versions (some return
// NOT_FOUND, some treat it as an idempotent no-op), so rather than hard-pin one
// outcome we assert the only two acceptable observed behaviors: a clean no-op,
// or an RpcError (never some other exception type / crash). OBSERVED on the
// pinned dev server: it throws RpcError (NOT_FOUND).
TEST_F(IntegrationTest, SchedBatchVisDeleteUnknownSchedule) {
  const std::string sid = "no-such-schedule-" + std::to_string(std::random_device{}());
  bool threw_rpc = false;
  try {
    client_->DeleteSchedule(sid);  // either a no-op or RpcError
  } catch (const temporal::RpcError&) {
    threw_rpc = true;
  }
  // Whatever the server chose, the schedule must not exist afterwards.
  SUCCEED() << "DeleteSchedule(unknown) " << (threw_rpc ? "threw RpcError" : "was a no-op");
  EXPECT_FALSE(client_->DescribeSchedule(sid));
}

// NEGATIVE: describing a schedule id that was never created. DescribeSchedule
// returns a bool "exists" (mapping NOT_FOUND -> has_schedule()==false at
// src/client/client.cpp:453), so it must report false without throwing.
TEST_F(IntegrationTest, SchedBatchVisDescribeUnknownSchedule) {
  const std::string sid = "absent-schedule-" + std::to_string(std::random_device{}());
  bool exists = true;
  EXPECT_NO_THROW(exists = client_->DescribeSchedule(sid));
  EXPECT_FALSE(exists);
}

// EDGE: pause -> unpause lifecycle. Pausing then unpausing a live schedule must
// leave it existing and describable (a 1-hour interval guarantees it never fires
// during the test, so no workflows are spawned).
TEST_F(IntegrationTest, SchedBatchVisPauseUnpauseStillExists) {
  const std::string sid = UniqueTaskQueue("sbv-pause");
  temporal::ScheduleOptions opts;
  opts.interval = std::chrono::hours(1);  // long -> never auto-fires during the test
  opts.workflow_type = "EdgeLongSleepWorkflow";
  opts.task_queue = sid + "-tq";
  client_->CreateSchedule(sid, opts);
  ASSERT_TRUE(client_->DescribeSchedule(sid));

  EXPECT_NO_THROW(client_->PauseSchedule(sid, "edge-pause"));
  EXPECT_TRUE(client_->DescribeSchedule(sid));  // still exists while paused
  EXPECT_NO_THROW(client_->UnpauseSchedule(sid, "edge-unpause"));
  EXPECT_TRUE(client_->DescribeSchedule(sid));  // and after unpause

  EXPECT_NO_THROW(client_->DeleteSchedule(sid));  // cleanup
}

// EDGE: batch CANCEL by query (the CANCEL counterpart to BatchTerminateByQuery).
// Starts two long-running workflows tagged with a unique search-attribute marker,
// requests batch cancellation by that query, then polls DescribeBatchOperation
// for progress to a terminal state. We assert on the batch operation completing
// and its progress counts (the workflows themselves ignore cancellation, so we
// don't assert their final status here — that's covered by the dedicated
// cancellation tests).
TEST_F(IntegrationTest, SchedBatchVisBatchCancelByQuery) {
  // Register the custom attribute via the CLI (idempotent); let it propagate.
  std::system("temporal operator search-attribute create --name ItestKeyword --type Keyword "
              ">/dev/null 2>&1");
  std::this_thread::sleep_for(1s);
  const auto tq = UniqueTaskQueue("sbv-bcancel");
  const std::string marker = "bcancel-" + std::to_string(std::random_device{}());
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("EdgeLongSleepWorkflow", EdgeLongSleepWorkflow);
  worker.Start();

  std::vector<temporal::client::WorkflowHandle> handles;
  for (int i = 0; i < 2; ++i) {
    temporal::StartWorkflowOptions o;
    o.task_queue = tq;
    o.search_attributes["ItestKeyword"] = temporal::sa::Keyword(marker);
    handles.push_back(client_->StartWorkflow(o, "EdgeLongSleepWorkflow", 0));
  }
  const std::string query = "ItestKeyword = '" + marker + "'";
  for (int i = 0; i < 40; ++i) {  // visibility is eventually consistent
    if (client_->ListWorkflows(query).size() >= 2) {
      break;
    }
    std::this_thread::sleep_for(250ms);
  }

  const std::string job_id = "cancel-job-" + std::to_string(std::random_device{}());
  client_->StartBatchCancel(job_id, query, "integration batch-cancel edge test");

  temporal::client::BatchOperationDescription desc;
  for (int i = 0; i < 60; ++i) {
    desc = client_->DescribeBatchOperation(job_id);
    // State names are the prefix-stripped proto enums, i.e. UPPERCASE.
    if (desc.state == "COMPLETED" || desc.state == "FAILED") {
      break;
    }
    std::this_thread::sleep_for(500ms);
  }
  EXPECT_EQ(desc.job_id, job_id);
  EXPECT_EQ(desc.state, "COMPLETED");
  EXPECT_EQ(desc.total_operations, 2);
  EXPECT_EQ(desc.completed_operations, 2);
  EXPECT_EQ(desc.failed_operations, 0);
  worker.Stop();
}

// EDGE: a visibility query that MATCHES vs one that does NOT match. Starts one
// workflow of a process-unique type, then asserts a WorkflowType filter for that
// type finds exactly 1 (via both ListWorkflows and CountWorkflows), while a
// filter for a different, never-used type finds 0. Process-unique types keep the
// counts exact across repeated full-suite runs against a persistent dev server.
TEST_F(IntegrationTest, SchedBatchVisQueryMatchVsNoMatch) {
  const auto tq = UniqueTaskQueue("sbv-vis");
  const std::string seed = std::to_string(std::random_device{}());
  const std::string match_type = "SbvMatchWf" + seed;
  const std::string miss_type = "SbvMissWf" + seed;  // registered to no workflow

  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow(match_type, EdgeLongSleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, match_type, 0);

  const std::string match_query = "WorkflowType = '" + match_type + "'";
  std::vector<temporal::client::WorkflowDescription> listed;
  for (int i = 0; i < 40; ++i) {  // wait for the visibility index to catch up
    listed = client_->ListWorkflows(match_query);
    if (listed.size() == 1) {
      break;
    }
    std::this_thread::sleep_for(250ms);
  }
  ASSERT_EQ(listed.size(), 1U);
  EXPECT_EQ(listed[0].workflow_type, match_type);
  EXPECT_EQ(listed[0].run_id, handle.run_id());
  EXPECT_EQ(client_->CountWorkflows(match_query), 1);

  // The non-matching query: a type no workflow was ever started with -> 0.
  const std::string miss_query = "WorkflowType = '" + miss_type + "'";
  EXPECT_TRUE(client_->ListWorkflows(miss_query).empty());
  EXPECT_EQ(client_->CountWorkflows(miss_query), 0);

  handle.Terminate("edge test cleanup");  // don't leave a 60s sleeper running
  worker.Stop();
}

}  // namespace
