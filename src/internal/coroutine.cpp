#include "internal/coroutine.h"

#include <utility>

namespace temporal::internal {

Coroutine::Coroutine(std::function<void()> body) : body_(std::move(body)) {
  thread_ = std::thread([this] { RunBody(); });
}

Coroutine::~Coroutine() {
  if (!thread_.joinable()) {
    return;
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
