#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

// Non-determinism detection. A deterministic workflow, replayed against its
// recorded history, must emit exactly the same orchestration commands in the
// same order. We capture the ordered stream of commands the workflow *produces*
// during a replay and the ordered stream of command-generating events history
// *recorded*, then match them pairwise (mirrors the Go SDK's
// matchReplayWithHistory / isCommandMatchEvent). History is authoritative.
namespace temporal::internal {

// One orchestration command, viewed either as the workflow produced it or as
// history recorded it. Identity is (kind, id[, name]) — matching the fields the
// server echoes back into the corresponding history event.
struct CommandEvent {
  enum class Kind : std::uint8_t {
    Activity,           // ScheduleActivityTask <-> ActivityTaskScheduled
    Timer,              // StartTimer           <-> TimerStarted
    ChildWorkflow,      // StartChildWorkflow   <-> StartChildWorkflowExecutionInitiated
    Marker,             // RecordMarker         <-> MarkerRecorded
    CompleteWorkflow,   // CompleteWorkflowExecution  <-> WorkflowExecutionCompleted
    FailWorkflow,       // FailWorkflowExecution      <-> WorkflowExecutionFailed
    ContinueAsNew,      // ContinueAsNewWorkflowExecution <-> WorkflowExecutionContinuedAsNew
  };

  Kind kind;
  std::string id;    // activity id / timer id / child workflow id / marker name
  std::string name;  // activity type / child workflow type (empty otherwise)
};

inline const char* CommandKindName(CommandEvent::Kind k) {
  switch (k) {
    case CommandEvent::Kind::Activity:
      return "ScheduleActivity";
    case CommandEvent::Kind::Timer:
      return "StartTimer";
    case CommandEvent::Kind::ChildWorkflow:
      return "StartChildWorkflow";
    case CommandEvent::Kind::Marker:
      return "RecordMarker";
    case CommandEvent::Kind::CompleteWorkflow:
      return "CompleteWorkflowExecution";
    case CommandEvent::Kind::FailWorkflow:
      return "FailWorkflowExecution";
    case CommandEvent::Kind::ContinueAsNew:
      return "ContinueAsNewWorkflowExecution";
  }
  return "Unknown";
}

// Does a produced command correspond to a recorded history event? Activities and
// child workflows are keyed by id + type name; timers and markers by id/name;
// terminal commands match on kind alone (there is at most one in a history).
inline bool CommandMatchesEvent(const CommandEvent& produced, const CommandEvent& expected) {
  if (produced.kind != expected.kind) {
    return false;
  }
  switch (produced.kind) {
    case CommandEvent::Kind::Activity:
    case CommandEvent::Kind::ChildWorkflow:
      return produced.id == expected.id && produced.name == expected.name;
    case CommandEvent::Kind::Timer:
    case CommandEvent::Kind::Marker:
      return produced.id == expected.id;
    case CommandEvent::Kind::CompleteWorkflow:
    case CommandEvent::Kind::FailWorkflow:
    case CommandEvent::Kind::ContinueAsNew:
      return true;
  }
  return false;
}

namespace detail {
inline std::string Describe(const CommandEvent& c) {
  std::string s = CommandKindName(c.kind);
  if (!c.id.empty()) {
    s += "/" + c.id;
  }
  if (!c.name.empty()) {
    s += "/" + c.name;
  }
  return s;
}
}  // namespace detail

// Compare the commands a workflow produced this replay against the command events
// history recorded, in order. Every recorded command event must be reproduced in
// the same position; the workflow may emit *additional* trailing commands (real
// forward progress past the replayed history). Returns a human-readable message
// for the first divergence, or std::nullopt when the replay is consistent.
inline std::optional<std::string> MatchReplayCommands(const std::vector<CommandEvent>& produced,
                                                      const std::vector<CommandEvent>& expected) {
  for (std::size_t i = 0; i < expected.size(); ++i) {
    if (i >= produced.size()) {
      return "[TMPRL1100] nondeterministic workflow: missing replay command for history " +
             detail::Describe(expected[i]);
    }
    if (!CommandMatchesEvent(produced[i], expected[i])) {
      return "[TMPRL1100] nondeterministic workflow: at command #" + std::to_string(i) +
             " history recorded " + detail::Describe(expected[i]) + " but replay produced " +
             detail::Describe(produced[i]);
    }
  }
  return std::nullopt;
}

}  // namespace temporal::internal
