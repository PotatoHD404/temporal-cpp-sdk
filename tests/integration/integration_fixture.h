#pragma once

// Shared integration-test fixture + helpers so per-capability test files can
// reuse one client connection + a collision-free task-queue scheme. Gated behind
// TEMPORAL_INTEGRATION=1 (TEMPORAL_ADDRESS overrides the default localhost:7233),
// so the default `ctest` run needs no server. The symbols are inline, so multiple
// translation units in the integration binary share a single definition.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

// Monotonic counter making each test's task queue unique within a process run.
inline std::atomic<int> g_seq{0};

inline std::string UniqueTaskQueue(const std::string& base) {
  return "itest-" + base + "-" + std::to_string(g_seq.fetch_add(1));
}

// Hard per-test deadline. A buggy/raced test can call Result()/Receive() on a
// workflow that never closes and then long-poll forever (~0% CPU); without a bound
// that silently hangs the whole suite. 120s is far above any real test (the
// slowest are ~6s), so overrunning means a genuine hang.
inline constexpr std::chrono::seconds kPerTestTimeout{120};

class IntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (std::getenv("TEMPORAL_INTEGRATION") == nullptr) {
      GTEST_SKIP() << "set TEMPORAL_INTEGRATION=1 and run `temporal server start-dev` to enable";
    }
    const char* addr = std::getenv("TEMPORAL_ADDRESS");
    temporal::ClientOptions opt;
    opt.target = (addr != nullptr) ? addr : "localhost:7233";
    client_ = std::make_unique<temporal::client::Client>(temporal::client::Client::Connect(opt));

    // Watchdog: if the test body hangs past the deadline, print which test and
    // abort — turning a silent multi-minute stall into an immediate, named
    // failure. Cancelled in TearDown when the test finishes normally.
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    const std::string name =
        info != nullptr ? std::string(info->test_suite_name()) + "." + info->name() : "?";
    watchdog_ = std::jthread([name](const std::stop_token& stop) {
      const auto deadline = std::chrono::steady_clock::now() + kPerTestTimeout;
      while (!stop.stop_requested()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (std::chrono::steady_clock::now() >= deadline) {
          std::fprintf(stderr, "\n[ TIMEOUT ] integration test %s exceeded %llds; aborting\n",
                       name.c_str(), static_cast<long long>(kPerTestTimeout.count()));
          std::fflush(stderr);
          std::abort();
        }
      }
    });
  }

  void TearDown() override { watchdog_.request_stop(); }

  std::unique_ptr<temporal::client::Client> client_;
  std::jthread watchdog_;
};
