#pragma once

#include <chrono>
#include <string>
#include <utility>

#include <temporal/client/client.h>
#include <temporal/common/options.h>

namespace temporal::testing {

// A connection to the Temporal time-skipping test server (`temporal-test-server`).
//
// Workers and clients run against it exactly as against a real server, but the
// server fast-forwards through pending timers — so a workflow that sleeps for
// "days" finishes in milliseconds, letting timer-heavy logic be tested without
// wall-clock waits. The test server starts with time skipping LOCKED (counter
// = 1); this environment UNLOCKS it on construction so idle timers auto-skip,
// and exposes manual lock/unlock + clock reads for fine-grained control.
//
// The server is a separate binary (not the dev server, not bundled). Start it
// yourself, e.g.:
//
//     temporal-test-server 7244
//
// then:
//
//     temporal::testing::TestWorkflowEnvironment env("localhost:7244");
//     temporal::worker::Worker worker(env.client(), "tq");
//     worker.RegisterWorkflow("Sleeper", Sleeper);   // does ctx.Sleep(24h)
//     worker.Start();
//     auto h = env.client().StartWorkflow(opts, "Sleeper");
//     h.Result<...>();              // returns in ms — the 24h timer is skipped
//     auto now = env.CurrentTime(); // advanced ~24h on the (skipped) server clock
class TestWorkflowEnvironment {
 public:
  // Connect to a test server already listening at `target` (host:port) and unlock
  // time skipping. `options` lets you override namespace / identity / TLS; its
  // `target` is set from the argument.
  explicit TestWorkflowEnvironment(const std::string& target, ClientOptions options = {})
      : client_(MakeClient(target, std::move(options))) {
    client_.UnlockTimeSkipping();  // counter 1 -> 0: pending timers fast-forward when idle
  }

  // The client bound to the test server — start workflows on it, or build a
  // Worker with `temporal::worker::Worker(env.client(), task_queue)`.
  client::Client& client() { return client_; }

  // The current (possibly skipped-ahead) server clock.
  std::chrono::system_clock::time_point CurrentTime() { return client_.GetCurrentTime(); }
  // Block until the server clock advances by `duration` (fast-forwarded while
  // time skipping is unlocked).
  void Sleep(std::chrono::system_clock::duration duration) { client_.Sleep(duration); }
  // Pause / resume automatic time skipping. Counted — balance lock with unlock;
  // time skips only while the counter is 0.
  void LockTimeSkipping() { client_.LockTimeSkipping(); }
  void UnlockTimeSkipping() { client_.UnlockTimeSkipping(); }

 private:
  static client::Client MakeClient(const std::string& target, ClientOptions options) {
    options.target = target;
    return client::Client::Connect(options);
  }

  client::Client client_;
};

}  // namespace temporal::testing
