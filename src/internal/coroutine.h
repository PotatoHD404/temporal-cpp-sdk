#pragma once

#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <thread>

namespace temporal::internal {

// Thrown into a suspended coroutine to unwind it during teardown. Deliberately
// NOT derived from std::exception so workflow/user `catch (const std::exception&)`
// cannot swallow it.
struct CoroutineAbort {};

// A cooperative, stackful coroutine running its body on a dedicated thread with
// strict turn-based alternation: the controller (Resume) and the coroutine
// (Yield) never run concurrently, so state they share needs no further locking.
// This is the C++ analogue of the goroutine-based dispatcher the Go SDK uses to
// keep a blocked workflow's full stack/state alive across suspensions.
class Coroutine {
 public:
  explicit Coroutine(std::function<void()> body);
  ~Coroutine();
  Coroutine(const Coroutine&) = delete;
  Coroutine& operator=(const Coroutine&) = delete;
  Coroutine(Coroutine&&) = delete;
  Coroutine& operator=(Coroutine&&) = delete;

  // Controller side: run the coroutine until it Yields or finishes. No-op once done.
  void Resume();
  // Like Resume(), but gives up after `timeout` if the coroutine hasn't yielded
  // (a likely deadlock: a non-yielding loop or a blocking call in workflow code).
  // Returns true if it yielded/finished, false on timeout — in which case the
  // coroutine is STILL running on its thread and the caller MUST Abandon() it
  // (and keep this object alive) rather than destroy it.
  bool ResumeFor(std::chrono::steady_clock::duration timeout);
  bool Done() const;
  // Give up on a coroutine that overran (detach its thread; ~Coroutine will not
  // join/unwind it). The object must outlive the detached thread, which still
  // references it — so an abandoned coroutine is intentionally leaked, never freed.
  void Abandon();
  // Controller side: rethrow any exception the body let escape (other than abort).
  void RethrowIfError() const;

  // Coroutine side: hand control back to the controller until the next Resume.
  // Throws CoroutineAbort if the coroutine is being torn down.
  void Yield();

 private:
  enum class Turn { Controller, Coroutine };
  void RunBody();

  std::function<void()> body_;
  mutable std::mutex m_;
  std::condition_variable cv_;
  Turn turn_ = Turn::Controller;
  bool done_ = false;
  bool abort_ = false;
  bool abandoned_ = false;  // overran; thread detached, object intentionally leaked
  std::exception_ptr error_;
  std::thread thread_;
};

}  // namespace temporal::internal
