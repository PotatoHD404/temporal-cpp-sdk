#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
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
    RequestCancelActivity,  // RequestCancelActivityTask <-> ActivityTaskCancelRequested
    Timer,              // StartTimer           <-> TimerStarted
    CancelTimer,        // CancelTimer          <-> TimerCanceled
    ChildWorkflow,      // StartChildWorkflow   <-> StartChildWorkflowExecutionInitiated
    RequestCancelExternalWorkflow,  // RequestCancelExternalWorkflowExecution <-> ...Initiated
    SignalExternalWorkflow,  // SignalExternalWorkflowExecution <-> ...Initiated
    UpsertSearchAttributes,  // UpsertWorkflowSearchAttributes <-> ...SearchAttributes event
    NexusOperation,     // ScheduleNexusOperation <-> NexusOperationScheduled
    Marker,             // RecordMarker         <-> MarkerRecorded
    CompleteWorkflow,   // CompleteWorkflowExecution  <-> WorkflowExecutionCompleted
    FailWorkflow,       // FailWorkflowExecution      <-> WorkflowExecutionFailed
    ContinueAsNew,      // ContinueAsNewWorkflowExecution <-> WorkflowExecutionContinuedAsNew
  };

  Kind kind;
  std::string id;    // activity id / timer id / child workflow id / marker name
  std::string name;  // activity type / child workflow type (empty otherwise)
};

// Command-kind display names, indexed by enum value (the enum is contiguous from
// 0). The static_assert below ties the table size to the enum count, so adding a
// Kind without a name fails to compile rather than silently returning "Unknown".
inline constexpr std::array<std::string_view, 13> kCommandKindNames = {
    "ScheduleActivity",
    "RequestCancelActivity",
    "StartTimer",
    "CancelTimer",
    "StartChildWorkflow",
    "RequestCancelExternalWorkflowExecution",
    "SignalExternalWorkflowExecution",
    "UpsertWorkflowSearchAttributes",
    "ScheduleNexusOperation",
    "RecordMarker",
    "CompleteWorkflowExecution",
    "FailWorkflowExecution",
    "ContinueAsNewWorkflowExecution",
};
static_assert(static_cast<std::size_t>(CommandEvent::Kind::ContinueAsNew) + 1 ==
                  kCommandKindNames.size(),
              "kCommandKindNames is out of sync with CommandEvent::Kind");

constexpr std::string_view CommandKindName(CommandEvent::Kind k) {
  const auto idx = static_cast<std::size_t>(k);
  return idx < kCommandKindNames.size() ? kCommandKindNames[idx] : std::string_view{"Unknown"};
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
    case CommandEvent::Kind::RequestCancelActivity:
    case CommandEvent::Kind::RequestCancelExternalWorkflow:
    case CommandEvent::Kind::SignalExternalWorkflow:
    case CommandEvent::Kind::UpsertSearchAttributes:  // no id; ordered position carries it
    case CommandEvent::Kind::Timer:
    case CommandEvent::Kind::CancelTimer:
    case CommandEvent::Kind::NexusOperation:  // keyed by the count-based seq id, like markers
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
  std::string s{CommandKindName(c.kind)};
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
