#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "internal/worker_impl.h"

namespace {

using temporal::internal::PollerScaler;

// Deterministic check of the elastic-poller target logic: a returned task scales
// the active target up (toward max_scalable), an empty-poll streak scales it back
// down, held slots persist across a scale-down, and Stop unblocks a parked poller.
TEST(PollerScaler, ScalesUpOnDemandDownOnIdle) {
  PollerScaler s;
  s.Configure(/*always_hot=*/1, /*max_scalable=*/3, /*empty_polls_before_scale_down=*/2);
  std::atomic<bool> stop{false};
  EXPECT_EQ(s.active(), 1);  // only the always-hot poller; target 0

  // Empty polls before any demand can't push the target below 0.
  s.Report(false);
  s.Report(false);
  EXPECT_EQ(s.active(), 1);

  // A returned task raises the target so one scalable poller may run.
  s.Report(true);
  ASSERT_TRUE(s.Acquire(stop));
  EXPECT_EQ(s.active(), 2);

  // Target is 1, so a second scalable acquire blocks until demand raises it.
  std::atomic<bool> got_second{false};
  std::thread t([&] {
    if (s.Acquire(stop)) {
      got_second = true;
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(got_second.load());  // active(1) >= target(1): parked
  s.Report(true);                   // target -> 2 unblocks it
  t.join();
  EXPECT_TRUE(got_second.load());
  EXPECT_EQ(s.active(), 3);  // always_hot(1) + 2 scalable

  // An empty streak scales the target down; already-held slots persist until
  // their poller releases.
  s.Report(false);
  s.Report(false);  // target 2 -> 1
  EXPECT_EQ(s.active(), 3);
  s.Release();
  EXPECT_EQ(s.active(), 2);

  // Stop unblocks a parked acquire and returns false (no slot taken).
  stop = true;
  s.Wake();
  EXPECT_FALSE(s.Acquire(stop));
  EXPECT_EQ(s.active(), 2);
}

// The target never exceeds max_scalable, so Acquire can be granted at most
// max_scalable times concurrently.
TEST(PollerScaler, TargetCappedAtMaxScalable) {
  PollerScaler s;
  s.Configure(/*always_hot=*/1, /*max_scalable=*/2, /*empty_polls_before_scale_down=*/3);
  std::atomic<bool> stop{false};
  for (int i = 0; i < 10; ++i) {
    s.Report(true);  // hammer demand well past the cap
  }
  EXPECT_TRUE(s.Acquire(stop));
  EXPECT_TRUE(s.Acquire(stop));
  EXPECT_EQ(s.active(), 3);  // always_hot(1) + max_scalable(2)

  // A third concurrent acquire must block (target capped at 2).
  std::atomic<bool> got_third{false};
  std::thread t([&] {
    if (s.Acquire(stop)) {
      got_third = true;
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(got_third.load());
  stop = true;
  s.Wake();
  t.join();
  EXPECT_FALSE(got_third.load());
}

}  // namespace
