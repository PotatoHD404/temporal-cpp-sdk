#pragma once

// Shared integration-test fixture + helpers so per-capability test files can
// reuse one client connection + a collision-free task-queue scheme. Gated behind
// TEMPORAL_INTEGRATION=1 (TEMPORAL_ADDRESS overrides the default localhost:7233),
// so the default `ctest` run needs no server. The symbols are inline, so multiple
// translation units in the integration binary share a single definition.
#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

// Monotonic counter making each test's task queue unique within a process run.
inline std::atomic<int> g_seq{0};

inline std::string UniqueTaskQueue(const std::string& base) {
  return "itest-" + base + "-" + std::to_string(g_seq.fetch_add(1));
}

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
  }

  std::unique_ptr<temporal::client::Client> client_;
};
