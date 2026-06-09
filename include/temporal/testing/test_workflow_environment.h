#pragma once

#include <chrono>
#include <exception>
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
    // Ensure time skipping is on: the server starts locked (counter = 1), so one
    // unlock enables fast-forward. This is best-effort + idempotent — if the
    // server is already unlocked (counter 0, e.g. a second environment or a reused
    // server) the extra unlock is rejected as "unbalanced", which is harmless
    // since it is already in the state we want, so we ignore it. A genuinely wrong
    // target (not a test server) surfaces on the first real call instead.
    try {
      client_.UnlockTimeSkipping();
    } catch (const std::exception&) {  // already unlocked — nothing to do
    }
  }

  // Re-lock on destruction to balance the constructor's unlock, returning the
  // server to its locked default. This keeps the shared server's lock counter
  // balanced across environments and stops it from drifting (an idle *unlocked*
  // server would otherwise keep fast-forwarding between tests). Best-effort — a
  // dropped connection at teardown must not throw out of a destructor.
  ~TestWorkflowEnvironment() {
    try {
      client_.LockTimeSkipping();
    } catch (const std::exception&) {
    }
  }

  // Non-copyable / non-movable: it owns a balanced unlock/lock pair on a shared
  // server, so duplicating or relocating it would unbalance the counter.
  TestWorkflowEnvironment(const TestWorkflowEnvironment&) = delete;
  TestWorkflowEnvironment& operator=(const TestWorkflowEnvironment&) = delete;
  TestWorkflowEnvironment(TestWorkflowEnvironment&&) = delete;
  TestWorkflowEnvironment& operator=(TestWorkflowEnvironment&&) = delete;

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
