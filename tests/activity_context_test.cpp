#include <gtest/gtest.h>

#include <temporal/activity/activity.h>
#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>

namespace {

namespace activity = temporal::activity;

// The activity context surfaces the server's cancel request, which is delivered
// in the response to a heartbeat. (Triggering that request — a workflow
// cancelling its activity — is the separate, larger activity-cancellation
// feature; here we verify the activity-side observation mechanism.)
TEST(ActivityContext, IsCancelledReflectsHeartbeatResponse) {
  const auto dc = temporal::DataConverter::Default();
  bool server_cancel = false;
  activity::Context ctx({}, dc.get(),
                        [&](const temporal::Payloads&) { return server_cancel; });
  EXPECT_FALSE(ctx.IsCancelled());
  ctx.RecordHeartbeat(1);
  EXPECT_FALSE(ctx.IsCancelled());  // server has not requested cancellation
  server_cancel = true;
  ctx.RecordHeartbeat(2);
  EXPECT_TRUE(ctx.IsCancelled());  // observed via the heartbeat response
}

TEST(ActivityContext, IsCancelledFalseWithoutHeartbeatCallback) {
  const auto dc = temporal::DataConverter::Default();
  activity::Context ctx({}, dc.get());  // no heartbeat callback wired
  ctx.RecordHeartbeat(1);
  EXPECT_FALSE(ctx.IsCancelled());
}

}  // namespace
