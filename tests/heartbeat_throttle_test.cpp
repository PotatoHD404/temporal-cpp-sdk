#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include <temporal/activity/activity.h>
#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>

namespace {

namespace activity = temporal::activity;
using namespace std::chrono_literals;

// A tight RecordHeartbeat loop must not flood the server: the underlying report
// fires at most once per throttle interval. We drive ~20 calls across a handful
// of 50ms windows and assert the mock was invoked far fewer times than it was
// called, that the first call reports, and that a call after the interval reports.
TEST(HeartbeatThrottle, ThrottlesActualReports) {
  const auto dc = temporal::DataConverter::Default();
  std::atomic<int> reports{0};
  activity::Context ctx({}, dc.get(),
                        [&](const temporal::Payloads&) {
                          ++reports;
                          return false;
                        },
                        50ms);

  // First call always reports (no prior timestamp).
  ctx.RecordHeartbeat(1);
  EXPECT_EQ(reports.load(), 1);

  // 20 rapid calls with 5ms sleeps span ~100ms => ~2 more intervals elapse, so
  // only a small handful of additional reports get through.
  constexpr int kCalls = 20;
  for (int i = 0; i < kCalls; ++i) {
    ctx.RecordHeartbeat(i);
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_LT(reports.load(), kCalls);  // throttled: far fewer reports than calls
  EXPECT_LE(reports.load(), 6);       // ~120ms of windows at 50ms => only a few

  // After clearly more than one interval, the next call must report again.
  const int before = reports.load();
  std::this_thread::sleep_for(70ms);
  ctx.RecordHeartbeat(99);
  EXPECT_EQ(reports.load(), before + 1);
}

// Throttled calls (those skipped before the interval elapses) return the cancel
// state observed by the most recent ACTUAL report, not a stale default.
TEST(HeartbeatThrottle, CachedCancelStateReturnedBetweenReports) {
  const auto dc = temporal::DataConverter::Default();
  bool server_cancel = true;  // the next actual report observes a cancel request
  int reports = 0;
  activity::Context ctx({}, dc.get(),
                        [&](const temporal::Payloads&) {
                          ++reports;
                          return server_cancel;
                        },
                        1s);  // long interval => subsequent calls are throttled

  ctx.RecordHeartbeat(1);  // actual report; observes cancel
  EXPECT_EQ(reports, 1);
  EXPECT_TRUE(ctx.IsCancelled());

  // The server stops requesting cancel, but these calls are throttled (no actual
  // report), so the cached state stays as last observed.
  server_cancel = false;
  ctx.RecordHeartbeat(2);
  ctx.RecordHeartbeat(3);
  EXPECT_EQ(reports, 1);            // no further server calls
  EXPECT_TRUE(ctx.IsCancelled());  // still the last observed cancel state
}

// The default-constructed (no throttle interval) context reports on every call,
// preserving the behavior of existing direct constructions.
TEST(HeartbeatThrottle, NoThrottleReportsEveryCall) {
  const auto dc = temporal::DataConverter::Default();
  int reports = 0;
  activity::Context ctx({}, dc.get(), [&](const temporal::Payloads&) {
    ++reports;
    return false;
  });  // no throttle interval => report every call

  for (int i = 0; i < 5; ++i) {
    ctx.RecordHeartbeat(i);
  }
  EXPECT_EQ(reports, 5);
}

}  // namespace
