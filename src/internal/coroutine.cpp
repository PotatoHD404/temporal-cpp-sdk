#include "internal/coroutine.h"

#include <utility>

namespace temporal::internal {

Coroutine::Coroutine(std::function<void()> body) : body_(std::move(body)) {
  thread_ = std::thread([this] { RunBody(); });
}

Coroutine::~Coroutine() {
  if (abandoned_ || !thread_.joinable()) {
    return;  // abandoned: the detached thread still runs; do not unwind/join.
  }
  {
    std::unique_lock<std::mutex> lock(m_);
    if (!done_) {
      // Unwind a still-suspended coroutine: hand it control with abort set, then
      // wait for it to finish unwinding.
      abort_ = true;
      turn_ = Turn::Coroutine;
      cv_.notify_all();
      cv_.wait(lock, [this] { return turn_ == Turn::Controller; });
    }
  }
  thread_.join();
}

void Coroutine::RunBody() {
  {
    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [this] { return turn_ == Turn::Coroutine; });
  }
  if (!abort_) {
    try {
      body_();
      // NOLINTNEXTLINE(bugprone-empty-catch) -- intentional: teardown unwind point.
    } catch (const CoroutineAbort&) {
      // Torn down while suspended; the stack has unwound, nothing more to do.
    } catch (...) {
      error_ = std::current_exception();
    }
  }
  std::unique_lock<std::mutex> lock(m_);
  done_ = true;
  turn_ = Turn::Controller;
  cv_.notify_all();
}

void Coroutine::Resume() {
  std::unique_lock<std::mutex> lock(m_);
  if (done_) {
    return;
  }
  turn_ = Turn::Coroutine;
  cv_.notify_all();
  cv_.wait(lock, [this] { return turn_ == Turn::Controller; });
}

bool Coroutine::ResumeFor(std::chrono::steady_clock::duration timeout) {
  std::unique_lock<std::mutex> lock(m_);
  if (done_) {
    return true;
  }
  turn_ = Turn::Coroutine;
  cv_.notify_all();
  // Returns false if the coroutine did not hand control back within `timeout`
  // (still running on its thread); true if it yielded or finished.
  return cv_.wait_for(lock, timeout, [this] { return turn_ == Turn::Controller; });
}

void Coroutine::Abandon() {
  std::lock_guard<std::mutex> lock(m_);
  abandoned_ = true;
  if (thread_.joinable()) {
    thread_.detach();  // the body keeps running on a detached thread; never joined
  }
}

bool Coroutine::Done() const {
  const std::unique_lock<std::mutex> lock(m_);
  return done_;
}

void Coroutine::RethrowIfError() const {
  std::exception_ptr err;
  {
    const std::unique_lock<std::mutex> lock(m_);
    err = error_;
  }
  if (err) {
    std::rethrow_exception(err);
  }
}

void Coroutine::Yield() {
  std::unique_lock<std::mutex> lock(m_);
  turn_ = Turn::Controller;
  cv_.notify_all();
  cv_.wait(lock, [this] { return turn_ == Turn::Coroutine; });
  if (abort_) {
    throw CoroutineAbort{};
  }
}

}  // namespace temporal::internal
