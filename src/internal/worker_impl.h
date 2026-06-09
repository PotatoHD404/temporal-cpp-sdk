#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <temporal/common/options.h>
#include <temporal/converter/data_converter.h>
#include <temporal/log/logger.h>

#include "internal/activity_task_handler.h"
#include "internal/workflow_task_handler.h"

namespace temporal::internal {

class GrpcClient;

// A counting gate that bounds concurrent task executions. A limit of 0 means
// unlimited (Acquire never blocks). Wired with mutex+condition_variable rather
// than std::counting_semaphore so the unlimited case and the Stop()-wakeup are
// expressible without a compile-time max. Drain() lets Stop() block until every
// permit is back (i.e. all in-flight Handle() calls returned), bounded by a
// deadline so it can never hang forever.
class ConcurrencyGate {
 public:
  explicit ConcurrencyGate(int limit) : limit_(limit) {}

  // Blocks until a permit is free or the gate is released. Returns false if the
  // gate was released (Stop) while waiting, in which case no permit was taken.
  bool Acquire() {
    std::unique_lock<std::mutex> lock(mu_);
    if (limit_ <= 0) {  // unlimited: still track in-flight for Drain()/Gauge.
      ++in_flight_;
      return true;
    }
    cv_.wait(lock, [this] { return released_ || in_flight_ < limit_; });
    if (released_) {
      return false;
    }
    ++in_flight_;
    return true;
  }

  void Release() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (in_flight_ > 0) {
        --in_flight_;
      }
    }
    cv_.notify_all();  // wake a waiting Acquire() and any Drain().
  }

  // Stop blocking future Acquire() calls (they return false). Does not touch
  // in-flight permits.
  void ReleaseAll() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      released_ = true;
    }
    cv_.notify_all();
  }

  // Wait until in_flight_ reaches 0 or the deadline passes. Returns true if it
  // drained, false on timeout.
  bool Drain(std::chrono::steady_clock::time_point deadline) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_until(lock, deadline, [this] { return in_flight_ == 0; });
  }

  std::int64_t in_flight() const {
    std::lock_guard<std::mutex> lock(mu_);
    return in_flight_;
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  int limit_;
  std::int64_t in_flight_ = 0;
  bool released_ = false;
};

// RAII permit: releases on scope exit so a throwing Handle() (or an early break)
// can never leak a slot. Move-only.
class GateGuard {
 public:
  GateGuard(ConcurrencyGate& gate, bool held) : gate_(&gate), held_(held) {}
  GateGuard(const GateGuard&) = delete;
  GateGuard& operator=(const GateGuard&) = delete;
  GateGuard(GateGuard&& o) noexcept : gate_(o.gate_), held_(o.held_) { o.held_ = false; }
  GateGuard& operator=(GateGuard&&) = delete;
  ~GateGuard() {
    if (held_) {
      gate_->Release();
    }
  }
  bool held() const { return held_; }

 private:
  ConcurrencyGate* gate_;
  bool held_;
};

// Paces task starts to at most `per_second` per second (0 => unlimited). Steady
// (no burst): Acquire() hands out evenly-spaced slots and sleeps in short slices
// until the slot, so a Stop() (stop flag) is observed promptly.
class RateLimiter {
 public:
  explicit RateLimiter(double per_second) : per_second_(per_second) {}

  void Acquire(const std::atomic<bool>& stop) {
    if (per_second_ <= 0.0) {
      return;  // unlimited
    }
    const auto interval = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::duration<double>(1.0 / per_second_));
    std::chrono::steady_clock::time_point allowed;
    {
      const std::lock_guard<std::mutex> lock(mu_);
      const auto now = std::chrono::steady_clock::now();
      allowed = next_slot_ < now ? now : next_slot_;
      next_slot_ = allowed + interval;
    }
    while (!stop.load() && std::chrono::steady_clock::now() < allowed) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

 private:
  std::mutex mu_;
  double per_second_;
  std::chrono::steady_clock::time_point next_slot_;  // epoch by default
};

// Tracks in-flight workflow-task handling per poller thread so a watchdog can
// detect (and report) a task that overruns a deadline — a likely deadlock
// (blocking call / infinite loop in workflow code). It reports only; the coroutine
// runs on the poller thread and cannot be safely interrupted.
class DeadlockWatch {
 public:
  void Begin() {
    const std::lock_guard<std::mutex> lock(mu_);
    active_[std::this_thread::get_id()] = {std::chrono::steady_clock::now(), false};
  }
  void End() {
    const std::lock_guard<std::mutex> lock(mu_);
    active_.erase(std::this_thread::get_id());
  }
  // Count in-flight tasks that have exceeded `deadline` (each reported once).
  int ReportOverruns(std::chrono::steady_clock::duration deadline) {
    const auto now = std::chrono::steady_clock::now();
    const std::lock_guard<std::mutex> lock(mu_);
    int overruns = 0;
    for (auto& [id, entry] : active_) {
      if (!entry.reported && now - entry.start >= deadline) {
        entry.reported = true;
        ++overruns;
      }
    }
    return overruns;
  }

 private:
  struct Entry {
    std::chrono::steady_clock::time_point start;
    bool reported = false;
  };
  std::mutex mu_;
  std::map<std::thread::id, Entry> active_;
};

// Owns the poller threads and the two task handlers. One worker serves a single
// task queue.
class WorkerImpl {
 public:
  WorkerImpl(std::shared_ptr<GrpcClient> grpc, std::shared_ptr<DataConverter> converter,
             std::shared_ptr<log::Logger> logger, std::string task_queue, WorkerOptions options);
  ~WorkerImpl();
  WorkerImpl(const WorkerImpl&) = delete;
  WorkerImpl& operator=(const WorkerImpl&) = delete;
  WorkerImpl(WorkerImpl&&) = delete;
  WorkerImpl& operator=(WorkerImpl&&) = delete;

  void RegisterWorkflow(std::string name, worker::WorkflowFn fn);
  void RegisterActivity(std::string name, worker::ActivityFn fn);

  void Start();
  void Run();
  void Stop();

  // Replay a recorded history (Temporal JSON) against the registered workflow;
  // throws if the replay is non-deterministic. Makes no RPCs.
  void ReplayWorkflowHistory(const std::string& history_json);

  long cache_hits() const { return workflow_handler_.cache_hits(); }
  long replays() const { return workflow_handler_.replays(); }

 private:
  // sticky selects the sticky workflow queue; session selects the host-unique
  // session activity queue (otherwise the normal queue is polled).
  void WorkflowPollLoop(bool sticky);
  void ActivityPollLoop(bool session);
  void DeadlockWatchLoop();  // reports workflow tasks that overrun the deadline

  std::shared_ptr<GrpcClient> grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string task_queue_;
  std::string sticky_queue_;   // per-worker sticky task queue (initialized before handlers)
  std::string session_queue_;  // per-worker session task queue (host-unique)
  WorkerOptions options_;
  WorkflowTaskHandler workflow_handler_;
  ActivityTaskHandler activity_handler_;
  // Bound concurrent executions per task kind (constructed from options_, which
  // is declared above so it is initialized first).
  ConcurrencyGate workflow_gate_;
  ConcurrencyGate activity_gate_;
  ConcurrencyGate session_gate_;
  RateLimiter activity_rate_limiter_;  // paces activity starts (per second)
  DeadlockWatch deadlock_watch_;       // detects overrunning workflow tasks
  std::atomic<bool> stop_{false};
  std::atomic<bool> draining_{false};  // set by Stop() before pollers are joined
  std::atomic<bool> started_{false};
  // Autoscaling bookkeeping: count of "hot" (eagerly polling) pollers per kind.
  // A poller may park (back off) only while doing so keeps the count at or above
  // options_.min_concurrent_pollers, so the queue is never left unpolled.
  std::atomic<int> wf_hot_pollers_{0};
  std::atomic<int> act_hot_pollers_{0};
  std::vector<std::thread> threads_;
};

}  // namespace temporal::internal
