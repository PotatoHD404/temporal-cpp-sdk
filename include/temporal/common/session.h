#pragma once

#include <chrono>
#include <string>

namespace temporal {

// Built-in activity types used by workflow::Context::CreateSession /
// CompleteSession for host-pinned sessions. The creation activity runs on the
// base task queue and returns the handling worker's host-unique session queue;
// the completion activity runs on that host queue to release the session slot.
inline constexpr const char* kSessionCreationActivityType = "__temporal_session_creation";
inline constexpr const char* kSessionCompletionActivityType = "__temporal_session_completion";

// Options for creating a worker session (host-pinned activity execution).
struct SessionOptions {
  // Upper bound on how long to wait for a session-enabled worker to accept the
  // creation activity (also the window during which it retries while every
  // session worker is at its `max_concurrent_sessions` capacity).
  std::chrono::milliseconds creation_timeout{30000};
};

// A created session. Schedule activities with ActivityOptions.task_queue set to
// `task_queue` to run them on the single worker host that owns the session, so a
// sequence of activities can share host-local resources.
struct SessionInfo {
  std::string session_id;  // identifies the session (the host queue, here)
  std::string task_queue;  // host-pinned queue for in-session activities
};

}  // namespace temporal
