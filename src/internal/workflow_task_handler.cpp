#include "internal/workflow_task_handler.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "temporal/api/command/v1/message.pb.h"
#include "temporal/api/enums/v1/command_type.pb.h"
#include "temporal/api/enums/v1/event_type.pb.h"
#include "temporal/api/enums/v1/failed_cause.pb.h"
#include "temporal/api/enums/v1/query.pb.h"
#include "temporal/api/enums/v1/task_queue.pb.h"
#include "google/protobuf/any.pb.h"
#include "temporal/api/history/v1/message.pb.h"
#include "temporal/api/protocol/v1/message.pb.h"
#include "temporal/api/query/v1/message.pb.h"
#include "temporal/api/taskqueue/v1/message.pb.h"
#include "temporal/api/update/v1/message.pb.h"

#include "internal/coroutine.h"
#include "internal/determinism.h"
#include "internal/grpc_client.h"
#include "internal/proto_util.h"

#include <temporal/common/errors.h>
#include <temporal/internal/workflow_outbound.h>
#include <temporal/workflow/context.h>

namespace temporal::internal {
namespace {

namespace cmd = ::temporal::api::command::v1;
namespace enums = ::temporal::api::enums::v1;
namespace hist = ::temporal::api::history::v1;
namespace query = ::temporal::api::query::v1;
namespace protocol = ::temporal::api::protocol::v1;
namespace update = ::temporal::api::update::v1;

// Outcome of an activity, derived by scanning history.
struct ActivityOutcome {
  bool scheduled = false;
  bool resolved = false;  // completed, failed, or timed out
  bool failed = false;
  bool cancel_requested = false;  // an ActivityTaskCancelRequested is already in history
  Payloads result;
  std::string failure_type;
  std::string failure_message;
};

struct TimerOutcome {
  bool started = false;
  bool fired = false;
  bool cancelled = false;
};

// Outcome of a child workflow, keyed by the (parent-assigned) child workflow_id.
struct ChildOutcome {
  bool initiated = false;
  bool cancel_requested = false;
  bool resolved = false;
  bool failed = false;
  Payloads result;
  std::string failure_type;
  std::string failure_message;
};

struct Prescan {
  Payloads input;
  std::unordered_map<std::string, ActivityOutcome> activities;  // keyed by activity_id
  std::unordered_map<std::string, TimerOutcome> timers;         // keyed by timer_id
  std::unordered_map<std::string, ChildOutcome> children;       // keyed by child workflow_id
  std::unordered_map<std::string, std::vector<Payloads>> signals;  // keyed by signal name
  bool cancel_requested = false;
  int ext_signals_initiated = 0;  // count of SignalExternalWorkflow commands in history
  // For incremental (sticky) continuations: correlate completion events to the
  // activity_id of their schedule, and remember how far history has been consumed.
  std::unordered_map<std::int64_t, std::string> sched_event_to_activity;
  std::unordered_map<std::string, std::int64_t> activity_to_sched_event;  // for RequestCancelActivity
  std::int64_t last_event_id = 0;
  // Ordered command-generating events, for non-determinism detection on replay.
  std::vector<CommandEvent> commands;
  // SideEffect marker payloads in call order; Version markers as
  // (change-id payloads, version payloads), parsed lazily by the runner.
  std::vector<Payload> side_effects;
  std::vector<std::pair<Payloads, Payloads>> version_markers;
};

// Walk the event history once, indexing the inputs/outcomes the runner replays
// against. Activities are correlated to their completion/failure via the
// scheduled event id that those events carry.
Prescan ScanHistory(const hist::History& history) {
  Prescan ps;
  for (const auto& ev : history.events()) {
    ps.last_event_id = ev.event_id();
    switch (ev.event_type()) {
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        ps.input = FromProtoPayloads(ev.workflow_execution_started_event_attributes().input());
        break;
      case enums::EVENT_TYPE_ACTIVITY_TASK_SCHEDULED: {
        const auto& a = ev.activity_task_scheduled_event_attributes();
        ps.activities[a.activity_id()].scheduled = true;
        ps.sched_event_to_activity[ev.event_id()] = a.activity_id();
        ps.activity_to_sched_event[a.activity_id()] = ev.event_id();
        ps.commands.push_back({CommandEvent::Kind::Activity, a.activity_id(), a.activity_type().name()});
        break;
      }
      case enums::EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED: {
        const auto& a = ev.activity_task_cancel_requested_event_attributes();
        ps.commands.push_back(
            {CommandEvent::Kind::RequestCancelActivity, std::to_string(a.scheduled_event_id()), ""});
        const auto it = ps.sched_event_to_activity.find(a.scheduled_event_id());
        if (it != ps.sched_event_to_activity.end()) {
          ps.activities[it->second].cancel_requested = true;
        }
        break;
      }
      case enums::EVENT_TYPE_ACTIVITY_TASK_COMPLETED: {
        const auto& a = ev.activity_task_completed_event_attributes();
        const auto it = ps.sched_event_to_activity.find(a.scheduled_event_id());
        if (it != ps.sched_event_to_activity.end()) {
          auto& o = ps.activities[it->second];
          o.resolved = true;
          o.result = FromProtoPayloads(a.result());
        }
        break;
      }
      case enums::EVENT_TYPE_ACTIVITY_TASK_FAILED: {
        const auto& a = ev.activity_task_failed_event_attributes();
        const auto it = ps.sched_event_to_activity.find(a.scheduled_event_id());
        if (it != ps.sched_event_to_activity.end()) {
          auto& o = ps.activities[it->second];
          o.resolved = true;
          o.failed = true;
          o.failure_message = a.failure().message();
          o.failure_type = a.failure().application_failure_info().type();
        }
        break;
      }
      case enums::EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT: {
        const auto& a = ev.activity_task_timed_out_event_attributes();
        const auto it = ps.sched_event_to_activity.find(a.scheduled_event_id());
        if (it != ps.sched_event_to_activity.end()) {
          auto& o = ps.activities[it->second];
          o.resolved = true;
          o.failed = true;
          o.failure_message = "activity timed out";
          o.failure_type = "TimeoutError";
        }
        break;
      }
      case enums::EVENT_TYPE_ACTIVITY_TASK_CANCELED: {
        const auto& a = ev.activity_task_canceled_event_attributes();
        const auto it = ps.sched_event_to_activity.find(a.scheduled_event_id());
        if (it != ps.sched_event_to_activity.end()) {
          auto& o = ps.activities[it->second];
          o.resolved = true;
          o.failed = true;
          o.failure_message = "activity cancelled";
          o.failure_type = "CanceledError";
        }
        break;
      }
      case enums::EVENT_TYPE_TIMER_STARTED: {
        const auto& t = ev.timer_started_event_attributes();
        ps.timers[t.timer_id()].started = true;
        ps.commands.push_back({CommandEvent::Kind::Timer, t.timer_id(), ""});
        break;
      }
      case enums::EVENT_TYPE_TIMER_FIRED:
        ps.timers[ev.timer_fired_event_attributes().timer_id()].fired = true;
        break;
      case enums::EVENT_TYPE_TIMER_CANCELED: {
        const auto& t = ev.timer_canceled_event_attributes();
        ps.timers[t.timer_id()].cancelled = true;
        ps.commands.push_back({CommandEvent::Kind::CancelTimer, t.timer_id(), ""});
        break;
      }
      case enums::EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED: {
        const auto& c = ev.start_child_workflow_execution_initiated_event_attributes();
        ps.children[c.workflow_id()].initiated = true;
        ps.commands.push_back(
            {CommandEvent::Kind::ChildWorkflow, c.workflow_id(), c.workflow_type().name()});
        break;
      }
      case enums::EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED: {
        const auto& a = ev.child_workflow_execution_completed_event_attributes();
        auto& o = ps.children[a.workflow_execution().workflow_id()];
        o.resolved = true;
        o.result = FromProtoPayloads(a.result());
        break;
      }
      case enums::EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED: {
        const auto& a = ev.child_workflow_execution_failed_event_attributes();
        auto& o = ps.children[a.workflow_execution().workflow_id()];
        o.resolved = true;
        o.failed = true;
        o.failure_message = a.failure().message();
        o.failure_type = a.failure().application_failure_info().type();
        break;
      }
      case enums::EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED: {
        const auto& a = ev.request_cancel_external_workflow_execution_initiated_event_attributes();
        const std::string& wid = a.workflow_execution().workflow_id();
        ps.commands.push_back({CommandEvent::Kind::RequestCancelExternalWorkflow, wid, ""});
        ps.children[wid].cancel_requested = true;
        break;
      }
      case enums::EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED: {
        const auto& a = ev.signal_external_workflow_execution_initiated_event_attributes();
        ps.commands.push_back(
            {CommandEvent::Kind::SignalExternalWorkflow, a.workflow_execution().workflow_id(), ""});
        ++ps.ext_signals_initiated;
        break;
      }
      case enums::EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED: {
        const auto& a = ev.child_workflow_execution_canceled_event_attributes();
        auto& o = ps.children[a.workflow_execution().workflow_id()];
        o.resolved = true;
        o.failed = true;
        o.failure_message = "child workflow cancelled";
        o.failure_type = "CanceledError";
        break;
      }
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED: {
        const auto& a = ev.workflow_execution_signaled_event_attributes();
        ps.signals[a.signal_name()].push_back(FromProtoPayloads(a.input()));
        break;
      }
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        ps.cancel_requested = true;
        break;
      case enums::EVENT_TYPE_MARKER_RECORDED: {
        const auto& m = ev.marker_recorded_event_attributes();
        ps.commands.push_back({CommandEvent::Kind::Marker, m.marker_name(), ""});
        if (m.marker_name() == "SideEffect") {
          const auto it = m.details().find("data");
          if (it != m.details().end()) {
            Payloads data = FromProtoPayloads(it->second);
            if (!data.empty()) {
              ps.side_effects.push_back(std::move(data[0]));
            }
          }
        } else if (m.marker_name() == "Version") {
          const auto id_it = m.details().find("change-id");
          const auto ver_it = m.details().find("version");
          if (id_it != m.details().end() && ver_it != m.details().end()) {
            ps.version_markers.emplace_back(FromProtoPayloads(id_it->second),
                                            FromProtoPayloads(ver_it->second));
          }
        }
        break;
      }
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
        ps.commands.push_back({CommandEvent::Kind::CompleteWorkflow, "", ""});
        break;
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
        ps.commands.push_back({CommandEvent::Kind::FailWorkflow, "", ""});
        break;
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
        ps.commands.push_back({CommandEvent::Kind::ContinueAsNew, "", ""});
        break;
      default:
        break;
    }
  }
  return ps;
}

// The WorkflowOutbound the running workflow talks to. The workflow body runs on a
// stackful coroutine; awaiting an unresolved future/signal yields the coroutine
// (preserving the workflow's full state) rather than unwinding it, which is what
// lets queries and selectors observe live state. Activity/timer ids are assigned
// deterministically by call order so a later replay lines up with history.
class WorkflowRunner final : public WorkflowOutbound {
 public:
  enum class Status { Blocked, Completed, Failed, ContinueAsNew };

  WorkflowRunner(workflow::WorkflowInfo info, std::shared_ptr<log::Logger> logger, bool is_replaying,
                 Prescan scan, const DataConverter* converter, worker::WorkflowFn workflow_fn,
                 Payloads input)
      : info_(std::move(info)),
        logger_(std::move(logger)),
        is_replaying_(is_replaying),
        scan_(std::move(scan)),
        converter_(converter),
        workflow_fn_(std::move(workflow_fn)),
        input_(std::move(input)) {
    // Resolve recorded GetVersion markers up front so GetVersion calls during the
    // run see the version history recorded.
    for (const auto& [id_payloads, ver_payloads] : scan_.version_markers) {
      if (id_payloads.empty() || ver_payloads.empty()) {
        continue;
      }
      try {
        change_versions_[converter_->FromPayload<std::string>(id_payloads[0])] =
            converter_->FromPayload<int>(ver_payloads[0]);
      } catch (const std::exception&) {
        // Ignore a malformed version marker rather than failing the whole task.
      }
    }
  }

  // Run the workflow until it blocks or finishes. Prescan resolves every
  // history-known future up front, so a single resume reaches the current block.
  // Used for the first task and for replay after a sticky-cache miss.
  void Run() {
    commands_.clear();
    produced_commands_.clear();
    if (!coroutine_) {
      coroutine_ = std::make_unique<Coroutine>([this] { RunBody(); });
    }
    coroutine_->Resume();
  }

  // Sticky continuation: apply only the new history events to the live futures
  // (and signal/cancel state), then resume the cached coroutine from where it
  // parked — no re-replay from the start.
  void ApplyAndResume(const hist::History& history) {
    commands_.clear();
    produced_commands_.clear();
    ApplyEvents(history);
    if (coroutine_) {
      coroutine_->Resume();
    }
  }

  std::int64_t last_event_id() const { return scan_.last_event_id; }

  // Compare the commands this from-scratch replay produced against the command
  // events history recorded; returns the first divergence, or nullopt if
  // consistent. Only meaningful after a full Run() — the sticky/live path is the
  // source of truth, not a replay, so it is never checked.
  std::optional<std::string> CheckDeterminism() const {
    return MatchReplayCommands(produced_commands_, scan_.commands);
  }

  bool Completed() const { return status_ == Status::Completed; }
  bool Failed() const { return status_ == Status::Failed; }
  bool IsContinueAsNew() const { return status_ == Status::ContinueAsNew; }
  bool IsDone() const { return coroutine_ && coroutine_->Done(); }
  const Payloads& result() const { return result_; }
  const tapi::failure::v1::Failure& failure() const { return failure_; }
  const ContinueAsNewRequested& continue_as_new() const { return continue_as_new_; }

  // Invoke a registered query handler against the live (suspended) workflow state.
  Payloads RunQuery(const std::string& name, const Payloads& args) {
    const auto it = query_handlers_.find(name);
    if (it == query_handlers_.end()) {
      throw ApplicationError("unknown query type: " + name, "QueryNotRegistered");
    }
    return it->second(args);
  }

  // Invoke a registered update handler (may mutate live workflow state).
  Payloads RunUpdate(const std::string& name, const Payloads& args) {
    const auto it = update_handlers_.find(name);
    if (it == update_handlers_.end()) {
      throw ApplicationError("unknown update: " + name, "UpdateNotRegistered");
    }
    return it->second(args);
  }

  std::shared_ptr<FutureState> ScheduleActivity(std::string_view activity_type,
                                                const Payloads& input,
                                                const ActivityOptions& options) override {
    const std::string id = std::to_string(activity_seq_++);
    produced_commands_.push_back({CommandEvent::Kind::Activity, id, std::string(activity_type)});
    auto state = std::make_shared<FutureState>();
    state->op = FutureState::Op::Activity;
    state->op_id = id;
    const auto it = scan_.activities.find(id);
    if (it != scan_.activities.end() && it->second.scheduled) {
      const ActivityOutcome& o = it->second;
      if (o.resolved) {
        state->ready = true;
        state->failed = o.failed;
        state->result = o.result;
        state->failure_type = o.failure_type;
        state->failure_message = o.failure_message;
      }
      // scheduled but not yet resolved -> stays pending
    } else {
      EmitScheduleActivity(id, activity_type, input, options);
    }
    activity_futures_[id] = state;  // so incremental completion events can resolve it
    return state;
  }

  std::shared_ptr<FutureState> StartTimer(std::chrono::nanoseconds duration) override {
    const std::string id = "t" + std::to_string(timer_seq_++);
    produced_commands_.push_back({CommandEvent::Kind::Timer, id, ""});
    auto state = std::make_shared<FutureState>();
    state->op = FutureState::Op::Timer;
    state->op_id = id;
    const auto it = scan_.timers.find(id);
    if (it != scan_.timers.end() && it->second.started) {
      state->ready = it->second.fired;  // a cancelled timer is resolved by the workflow's Cancel()
    } else {
      EmitStartTimer(id, duration);
    }
    timer_futures_[id] = state;
    return state;
  }

  void Cancel(const std::shared_ptr<FutureState>& state) override {
    if (!state || state->cancelled) {
      return;
    }
    if (state->op == FutureState::Op::Timer) {
      produced_commands_.push_back({CommandEvent::Kind::CancelTimer, state->op_id, ""});
      // Emit the command only if the cancel isn't already recorded in history.
      const auto it = scan_.timers.find(state->op_id);
      if (!(it != scan_.timers.end() && it->second.cancelled)) {
        EmitCancelTimer(state->op_id);
      }
      state->ready = true;
      state->cancelled = true;  // a timer resolves immediately on cancel
      return;
    }
    if (state->op == FutureState::Op::Activity) {
      // Ask the server to cancel the activity. The future is NOT resolved here:
      // the activity observes the request via its heartbeat and finishes, which
      // resolves the future through the normal completion/canceled event. Only an
      // activity already scheduled in history can be cancelled.
      const auto sched = scan_.activity_to_sched_event.find(state->op_id);
      if (sched == scan_.activity_to_sched_event.end()) {
        logger_->Warn("cannot cancel an activity not yet scheduled in history",
                      {log::F("activity_id", state->op_id)});
        return;
      }
      produced_commands_.push_back(
          {CommandEvent::Kind::RequestCancelActivity, std::to_string(sched->second), ""});
      const auto act = scan_.activities.find(state->op_id);
      if (!(act != scan_.activities.end() && act->second.cancel_requested)) {
        EmitRequestCancelActivity(sched->second);
      }
      return;
    }
    if (state->op == FutureState::Op::ChildWorkflow) {
      // Ask the server to cancel the child workflow (child_workflow_only). Like an
      // activity, the future is resolved later by the child's own canceled /
      // completed / failed event, not here.
      produced_commands_.push_back(
          {CommandEvent::Kind::RequestCancelExternalWorkflow, state->op_id, ""});
      const auto child = scan_.children.find(state->op_id);
      if (!(child != scan_.children.end() && child->second.cancel_requested)) {
        EmitRequestCancelExternalWorkflow(state->op_id, /*child_only=*/true);
      }
      return;
    }
    logger_->Warn("cancellation not supported for this operation",
                  {log::F("op_id", state->op_id)});
  }

  void CancelExternalWorkflow(std::string_view workflow_id) override {
    // Request cancellation of an arbitrary (non-child) workflow by id.
    const std::string id(workflow_id);
    produced_commands_.push_back({CommandEvent::Kind::RequestCancelExternalWorkflow, id, ""});
    const auto it = scan_.children.find(id);
    if (!(it != scan_.children.end() && it->second.cancel_requested)) {
      EmitRequestCancelExternalWorkflow(id, /*child_only=*/false);
    }
  }

  void SignalExternalWorkflow(std::string_view workflow_id, std::string_view signal_name,
                              const Payloads& input) override {
    // Fire-and-forget signal to another workflow by id. Emitted only for calls
    // not already recorded in history (count-keyed, like SideEffect markers).
    produced_commands_.push_back(
        {CommandEvent::Kind::SignalExternalWorkflow, std::string(workflow_id), ""});
    if (ext_signal_seq_++ >= static_cast<std::size_t>(scan_.ext_signals_initiated)) {
      EmitSignalExternalWorkflow(workflow_id, signal_name, input);
    }
  }

  std::shared_ptr<FutureState> StartChildWorkflow(std::string_view workflow_type,
                                                  const Payloads& input,
                                                  const ChildWorkflowOptions& options) override {
    const std::string auto_id = info_.workflow_id + "_c" + std::to_string(child_seq_++);
    const std::string id = options.id.empty() ? auto_id : options.id;
    produced_commands_.push_back({CommandEvent::Kind::ChildWorkflow, id, std::string(workflow_type)});
    auto state = std::make_shared<FutureState>();
    state->op = FutureState::Op::ChildWorkflow;
    state->op_id = id;
    const auto it = scan_.children.find(id);
    if (it != scan_.children.end() && it->second.initiated) {
      const ChildOutcome& o = it->second;
      if (o.resolved) {
        state->ready = true;
        state->failed = o.failed;
        state->result = o.result;
        state->failure_type = o.failure_type;
        state->failure_message = o.failure_message;
      }
    } else {
      EmitStartChildWorkflow(id, workflow_type, input, options);
    }
    child_futures_[id] = state;
    return state;
  }

  std::optional<Payload> ReplaySideEffect() override {
    produced_commands_.push_back({CommandEvent::Kind::Marker, "SideEffect", ""});
    const std::size_t ordinal = side_effect_seq_++;
    if (ordinal < scan_.side_effects.size()) {
      return scan_.side_effects[ordinal];
    }
    return std::nullopt;
  }

  void RecordSideEffect(const Payload& value) override {
    const auto id = static_cast<std::int64_t>(side_effect_seq_ - 1);
    EmitRecordMarker(
        "SideEffect",
        {{"side-effect-id", Payloads{converter_->ToPayload(id)}}, {"data", Payloads{value}}});
  }

  int GetVersion(const std::string& change_id, int min_supported, int max_supported) override {
    const auto it = change_versions_.find(change_id);
    if (it != change_versions_.end()) {
      produced_commands_.push_back({CommandEvent::Kind::Marker, "Version", ""});
      const int v = it->second;
      if (v < min_supported || v > max_supported) {
        throw ApplicationError("workflow change '" + change_id + "': recorded version " +
                                   std::to_string(v) + " not in supported range [" +
                                   std::to_string(min_supported) + ", " +
                                   std::to_string(max_supported) + "]",
                               "VersionMismatch");
      }
      return v;
    }
    if (is_replaying_) {
      // A GetVersion call replaying history that predates it: no marker exists and
      // none is recorded here, so no command is produced.
      change_versions_[change_id] = workflow::kDefaultVersion;
      return workflow::kDefaultVersion;
    }
    produced_commands_.push_back({CommandEvent::Kind::Marker, "Version", ""});
    const int version = max_supported;
    EmitRecordMarker("Version", {{"change-id", Payloads{converter_->ToPayload(change_id)}},
                                 {"version", Payloads{converter_->ToPayload(version)}}});
    change_versions_[change_id] = version;
    return version;
  }

  void Block(const std::shared_ptr<FutureState>& state) override {
    while (!state->ready) {
      coroutine_->Yield();
    }
  }

  void Park() override { coroutine_->Yield(); }

  bool TryConsumeSignal(std::string_view name, Payloads& out) override {
    const std::string key(name);
    const auto it = scan_.signals.find(key);
    if (it == scan_.signals.end()) {
      return false;
    }
    std::size_t& cursor = signal_cursor_[key];
    if (cursor >= it->second.size()) {
      return false;
    }
    out = it->second[cursor];
    ++cursor;
    return true;
  }

  bool HasSignal(std::string_view name) const override {
    const std::string key(name);
    const auto sit = scan_.signals.find(key);
    if (sit == scan_.signals.end()) {
      return false;
    }
    const auto cit = signal_cursor_.find(key);
    const std::size_t cursor = cit == signal_cursor_.end() ? 0 : cit->second;
    return cursor < sit->second.size();
  }

  bool IsCancelRequested() const override { return scan_.cancel_requested; }

  std::shared_ptr<FutureState> AwaitCancellation() override {
    auto state = std::make_shared<FutureState>();
    if (scan_.cancel_requested) {
      state->ready = true;
    } else {
      cancel_futures_.push_back(state);  // resolved when a cancel event arrives
    }
    return state;
  }

  void RegisterQueryHandler(std::string name, QueryFn handler) override {
    query_handlers_.insert_or_assign(std::move(name), std::move(handler));
  }

  void RegisterUpdateHandler(std::string name, QueryFn handler) override {
    update_handlers_.insert_or_assign(std::move(name), std::move(handler));
  }

  void RegisterUpdateValidator(std::string name, QueryFn validator) override {
    update_validators_.insert_or_assign(std::move(name), std::move(validator));
  }

  // Run the registered validator for an update, if one exists. Throws to reject
  // the update (the caller turns the exception into an ephemeral Rejection).
  void ValidateUpdate(const std::string& name, const Payloads& args) {
    const auto it = update_validators_.find(name);
    if (it != update_validators_.end()) {
      it->second(args);  // QueryFn; return value ignored, a throw rejects
    }
  }

  const workflow::WorkflowInfo& Info() const override { return info_; }
  log::Logger& Logger() const override { return *logger_; }
  bool IsReplaying() const override { return is_replaying_; }

  const std::vector<cmd::Command>& commands() const { return commands_; }

 private:
  // Runs on the coroutine thread: executes the workflow function to its next
  // suspension point (or completion), capturing the result or failure.
  void RunBody() {
    workflow::Context ctx(this, converter_);
    try {
      result_ = workflow_fn_(ctx, input_);
      status_ = Status::Completed;
    } catch (const ContinueAsNewRequested& c) {
      continue_as_new_ = c;
      status_ = Status::ContinueAsNew;
    } catch (const ApplicationError& e) {
      failure_ = MakeApplicationFailure(e.what(), e.type());
      status_ = Status::Failed;
    } catch (const std::exception& e) {
      failure_ = MakeApplicationFailure(e.what(), "std::exception");
      status_ = Status::Failed;
    }
    // Record the terminal command so a replay can be matched against history's
    // terminal event (the response path emits the actual command).
    switch (status_) {
      case Status::Completed:
        produced_commands_.push_back({CommandEvent::Kind::CompleteWorkflow, "", ""});
        break;
      case Status::Failed:
        produced_commands_.push_back({CommandEvent::Kind::FailWorkflow, "", ""});
        break;
      case Status::ContinueAsNew:
        produced_commands_.push_back({CommandEvent::Kind::ContinueAsNew, "", ""});
        break;
      case Status::Blocked:
        break;
    }
  }

  // Resolve the live future for the activity whose schedule had this event id.
  void ResolveActivity(std::int64_t scheduled_event_id, bool failed, Payloads result,
                       std::string failure_type, std::string failure_message) {
    const auto m = scan_.sched_event_to_activity.find(scheduled_event_id);
    if (m == scan_.sched_event_to_activity.end()) {
      return;
    }
    const auto it = activity_futures_.find(m->second);
    if (it == activity_futures_.end()) {
      return;
    }
    auto& st = *it->second;
    st.ready = true;
    st.failed = failed;
    st.result = std::move(result);
    st.failure_type = std::move(failure_type);
    st.failure_message = std::move(failure_message);
  }

  // Apply history events newer than last_event_id to live futures / signal state.
  void ApplyEvents(const hist::History& history) {
    for (const auto& ev : history.events()) {
      if (ev.event_id() <= scan_.last_event_id) {
        continue;
      }
      switch (ev.event_type()) {
        case enums::EVENT_TYPE_ACTIVITY_TASK_SCHEDULED: {
          const auto& a = ev.activity_task_scheduled_event_attributes();
          scan_.sched_event_to_activity[ev.event_id()] = a.activity_id();
          scan_.activity_to_sched_event[a.activity_id()] = ev.event_id();
          break;
        }
        case enums::EVENT_TYPE_ACTIVITY_TASK_COMPLETED: {
          const auto& a = ev.activity_task_completed_event_attributes();
          ResolveActivity(a.scheduled_event_id(), false, FromProtoPayloads(a.result()), "", "");
          break;
        }
        case enums::EVENT_TYPE_ACTIVITY_TASK_FAILED: {
          const auto& a = ev.activity_task_failed_event_attributes();
          ResolveActivity(a.scheduled_event_id(), true, {},
                          a.failure().application_failure_info().type(), a.failure().message());
          break;
        }
        case enums::EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT: {
          const auto& a = ev.activity_task_timed_out_event_attributes();
          ResolveActivity(a.scheduled_event_id(), true, {}, "TimeoutError", "activity timed out");
          break;
        }
        case enums::EVENT_TYPE_ACTIVITY_TASK_CANCELED: {
          const auto& a = ev.activity_task_canceled_event_attributes();
          ResolveActivity(a.scheduled_event_id(), true, {}, "CanceledError", "activity cancelled");
          break;
        }
        case enums::EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED: {
          const auto& a = ev.activity_task_cancel_requested_event_attributes();
          const auto it = scan_.sched_event_to_activity.find(a.scheduled_event_id());
          if (it != scan_.sched_event_to_activity.end()) {
            scan_.activities[it->second].cancel_requested = true;
          }
          break;
        }
        case enums::EVENT_TYPE_TIMER_FIRED: {
          const auto it = timer_futures_.find(ev.timer_fired_event_attributes().timer_id());
          if (it != timer_futures_.end()) {
            it->second->ready = true;
          }
          break;
        }
        case enums::EVENT_TYPE_TIMER_CANCELED: {
          const auto it = timer_futures_.find(ev.timer_canceled_event_attributes().timer_id());
          if (it != timer_futures_.end()) {
            it->second->ready = true;
            it->second->cancelled = true;
          }
          break;
        }
        case enums::EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED: {
          const auto& a = ev.child_workflow_execution_completed_event_attributes();
          const auto it = child_futures_.find(a.workflow_execution().workflow_id());
          if (it != child_futures_.end()) {
            it->second->ready = true;
            it->second->result = FromProtoPayloads(a.result());
          }
          break;
        }
        case enums::EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED: {
          const auto& a = ev.child_workflow_execution_failed_event_attributes();
          const auto it = child_futures_.find(a.workflow_execution().workflow_id());
          if (it != child_futures_.end()) {
            it->second->ready = true;
            it->second->failed = true;
            it->second->failure_message = a.failure().message();
            it->second->failure_type = a.failure().application_failure_info().type();
          }
          break;
        }
        case enums::EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED: {
          const auto& a = ev.child_workflow_execution_canceled_event_attributes();
          const auto it = child_futures_.find(a.workflow_execution().workflow_id());
          if (it != child_futures_.end()) {
            it->second->ready = true;
            it->second->failed = true;
            it->second->failure_message = "child workflow cancelled";
            it->second->failure_type = "CanceledError";
          }
          break;
        }
        case enums::EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED: {
          const auto& a = ev.workflow_execution_signaled_event_attributes();
          scan_.signals[a.signal_name()].push_back(FromProtoPayloads(a.input()));
          break;
        }
        case enums::EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
          scan_.cancel_requested = true;
          for (const auto& s : cancel_futures_) {
            s->ready = true;
          }
          break;
        default:
          break;
      }
      scan_.last_event_id = ev.event_id();
    }
  }

  void EmitScheduleActivity(const std::string& id, std::string_view activity_type,
                            const Payloads& input, const ActivityOptions& options) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK);
    auto* attr = c.mutable_schedule_activity_task_command_attributes();
    attr->set_activity_id(id);
    attr->mutable_activity_type()->set_name(std::string(activity_type));
    attr->mutable_task_queue()->set_name(options.task_queue.empty() ? info_.task_queue
                                                                    : options.task_queue);
    if (!input.empty()) {
      *attr->mutable_input() = ToProtoPayloads(input);
    }
    if (options.schedule_to_close_timeout.count() > 0) {
      *attr->mutable_schedule_to_close_timeout() =
          ToProtoDuration(options.schedule_to_close_timeout);
    }
    if (options.schedule_to_start_timeout.count() > 0) {
      *attr->mutable_schedule_to_start_timeout() =
          ToProtoDuration(options.schedule_to_start_timeout);
    }
    if (options.start_to_close_timeout.count() > 0) {
      *attr->mutable_start_to_close_timeout() = ToProtoDuration(options.start_to_close_timeout);
    }
    if (options.heartbeat_timeout.count() > 0) {
      *attr->mutable_heartbeat_timeout() = ToProtoDuration(options.heartbeat_timeout);
    }
    if (options.retry_policy_set) {
      *attr->mutable_retry_policy() = ToProtoRetryPolicy(options.retry_policy);
    }
    commands_.push_back(std::move(c));
  }

  void EmitStartTimer(const std::string& id, std::chrono::nanoseconds duration) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_START_TIMER);
    auto* attr = c.mutable_start_timer_command_attributes();
    attr->set_timer_id(id);
    *attr->mutable_start_to_fire_timeout() = ToProtoDuration(duration);
    commands_.push_back(std::move(c));
  }

  void EmitCancelTimer(const std::string& id) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_CANCEL_TIMER);
    c.mutable_cancel_timer_command_attributes()->set_timer_id(id);
    commands_.push_back(std::move(c));
  }

  void EmitRequestCancelActivity(std::int64_t scheduled_event_id) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK);
    c.mutable_request_cancel_activity_task_command_attributes()->set_scheduled_event_id(
        scheduled_event_id);
    commands_.push_back(std::move(c));
  }

  void EmitStartChildWorkflow(const std::string& id, std::string_view workflow_type,
                              const Payloads& input, const ChildWorkflowOptions& options) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION);
    auto* attr = c.mutable_start_child_workflow_execution_command_attributes();
    attr->set_namespace_(info_.ns);
    attr->set_workflow_id(id);
    attr->mutable_workflow_type()->set_name(std::string(workflow_type));
    attr->mutable_task_queue()->set_name(options.task_queue.empty() ? info_.task_queue
                                                                     : options.task_queue);
    if (!input.empty()) {
      *attr->mutable_input() = ToProtoPayloads(input);
    }
    commands_.push_back(std::move(c));
  }

  void EmitRequestCancelExternalWorkflow(const std::string& workflow_id, bool child_only) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION);
    auto* attr = c.mutable_request_cancel_external_workflow_execution_command_attributes();
    attr->set_namespace_(info_.ns);
    attr->set_workflow_id(workflow_id);
    attr->set_child_workflow_only(child_only);
    commands_.push_back(std::move(c));
  }

  void EmitSignalExternalWorkflow(std::string_view workflow_id, std::string_view signal_name,
                                  const Payloads& input) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION);
    auto* attr = c.mutable_signal_external_workflow_execution_command_attributes();
    attr->set_namespace_(info_.ns);
    attr->mutable_execution()->set_workflow_id(std::string(workflow_id));
    attr->set_signal_name(std::string(signal_name));
    if (!input.empty()) {
      *attr->mutable_input() = ToProtoPayloads(input);
    }
    commands_.push_back(std::move(c));
  }

  void EmitRecordMarker(const std::string& name,
                        const std::vector<std::pair<std::string, Payloads>>& details) {
    cmd::Command c;
    c.set_command_type(enums::COMMAND_TYPE_RECORD_MARKER);
    auto* attr = c.mutable_record_marker_command_attributes();
    attr->set_marker_name(name);
    for (const auto& [key, payloads] : details) {
      (*attr->mutable_details())[key] = ToProtoPayloads(payloads);
    }
    commands_.push_back(std::move(c));
  }

  workflow::WorkflowInfo info_;
  std::shared_ptr<log::Logger> logger_;
  bool is_replaying_;
  Prescan scan_;
  const DataConverter* converter_;
  worker::WorkflowFn workflow_fn_;
  Payloads input_;
  std::unordered_map<std::string, std::size_t> signal_cursor_;
  std::unordered_map<std::string, QueryFn> query_handlers_;
  std::unordered_map<std::string, QueryFn> update_handlers_;
  std::unordered_map<std::string, QueryFn> update_validators_;
  std::unordered_map<std::string, std::shared_ptr<FutureState>> activity_futures_;  // by activity_id
  std::unordered_map<std::string, std::shared_ptr<FutureState>> timer_futures_;     // by timer_id
  std::unordered_map<std::string, std::shared_ptr<FutureState>> child_futures_;     // by child wf id
  std::vector<std::shared_ptr<FutureState>> cancel_futures_;  // resolved on workflow cancel
  std::vector<cmd::Command> commands_;
  std::vector<CommandEvent> produced_commands_;  // ordered, for non-determinism detection
  Status status_ = Status::Blocked;
  Payloads result_;
  tapi::failure::v1::Failure failure_;
  ContinueAsNewRequested continue_as_new_;
  int activity_seq_ = 0;
  int timer_seq_ = 0;
  int child_seq_ = 0;
  std::size_t side_effect_seq_ = 0;
  std::size_t ext_signal_seq_ = 0;
  std::unordered_map<std::string, int> change_versions_;
  std::unique_ptr<Coroutine> coroutine_;  // declared last -> destroyed first (tears down thread)
};

// Page in a running workflow's full history and replay it into a fresh runner so
// a query can be answered when we have no cached state for the run. A query must
// be answered from complete state, never from a partial-history rebuild (which
// would observe a workflow that never received its signals). Returns nullptr if
// the history could not be fetched. The runner is transient: callers must not
// cache it (a query must not populate the sticky cache).
std::shared_ptr<WorkflowRunner> BuildRunnerForQuery(GrpcClient* grpc,
                                                    const std::shared_ptr<log::Logger>& logger,
                                                    const DataConverter* converter,
                                                    const workflow::WorkflowInfo& info,
                                                    const worker::WorkflowFn& fn) {
  hist::History full;
  std::string page_token;
  for (;;) {
    wsv::GetWorkflowExecutionHistoryRequest req;
    req.set_namespace_(grpc->ns());
    req.mutable_execution()->set_workflow_id(info.workflow_id);
    if (!info.run_id.empty()) {
      req.mutable_execution()->set_run_id(info.run_id);
    }
    req.set_skip_archival(true);
    if (!page_token.empty()) {
      req.set_next_page_token(page_token);
    }
    wsv::GetWorkflowExecutionHistoryResponse resp;
    try {
      resp = grpc->GetWorkflowExecutionHistory(req);
    } catch (const std::exception& e) {
      logger->Error("failed to fetch history to answer query",
                    {log::F("workflow_id", info.workflow_id), log::F("error", e.what())});
      return nullptr;
    }
    for (const auto& ev : resp.history().events()) {
      *full.add_events() = ev;
    }
    page_token = resp.next_page_token();
    if (page_token.empty()) {
      break;
    }
  }

  Prescan scan = ScanHistory(full);
  Payloads input = scan.input;
  auto runner = std::make_shared<WorkflowRunner>(info, logger, /*is_replaying=*/true, std::move(scan),
                                                 converter, fn, std::move(input));
  runner->Run();
  return runner;
}

}  // namespace

WorkflowTaskHandler::WorkflowTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                                         std::shared_ptr<log::Logger> logger, std::string task_queue,
                                         std::string sticky_queue, WorkflowPanicPolicy panic_policy,
                                         int max_cached_workflows)
    : grpc_(grpc),
      converter_(std::move(converter)),
      logger_(std::move(logger)),
      task_queue_(std::move(task_queue)),
      sticky_queue_(std::move(sticky_queue)),
      panic_policy_(panic_policy),
      cache_(max_cached_workflows > 0 ? static_cast<std::size_t>(max_cached_workflows) : 0) {}

void WorkflowTaskHandler::Register(std::string name, worker::WorkflowFn fn) {
  workflows_.insert_or_assign(std::move(name), std::move(fn));
}

std::optional<std::string> WorkflowTaskHandler::ReplayHistory(const hist::History& history) {
  std::string workflow_type;
  for (const auto& ev : history.events()) {
    if (ev.event_type() == enums::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
      workflow_type = ev.workflow_execution_started_event_attributes().workflow_type().name();
      break;
    }
  }
  if (workflow_type.empty()) {
    throw ApplicationError("history has no WorkflowExecutionStarted event", "ReplayError");
  }
  const auto wf = workflows_.find(workflow_type);
  if (wf == workflows_.end()) {
    throw ApplicationError("no workflow registered for type: " + workflow_type, "ReplayError");
  }

  workflow::WorkflowInfo info;
  info.workflow_type = workflow_type;
  info.workflow_id = "replay";
  info.task_queue = task_queue_;
  info.ns = grpc_->ns();

  // Drive a fresh runner through the full history (always replaying) and report
  // whether the workflow reproduced the recorded commands in order.
  Prescan scan = ScanHistory(history);
  Payloads input = scan.input;
  auto runner = std::make_shared<WorkflowRunner>(info, logger_, /*is_replaying=*/true, std::move(scan),
                                                 converter_.get(), wf->second, std::move(input));
  runner->Run();
  return runner->CheckDeterminism();
}

void WorkflowTaskHandler::Handle(const wsv::PollWorkflowTaskQueueResponse& task) {
  workflow::WorkflowInfo info;
  info.workflow_id = task.workflow_execution().workflow_id();
  info.run_id = task.workflow_execution().run_id();
  info.workflow_type = task.workflow_type().name();
  info.task_queue = task_queue_;
  info.ns = grpc_->ns();

  const auto wf = workflows_.find(info.workflow_type);
  if (wf == workflows_.end()) {
    logger_->Error("no workflow registered for type", {log::F("type", info.workflow_type)});
    wsv::RespondWorkflowTaskFailedRequest req;
    req.set_task_token(task.task_token());
    req.set_identity(grpc_->identity());
    *req.mutable_failure() = MakeApplicationFailure(
        "no workflow registered for type: " + info.workflow_type, "NotRegisteredError");
    grpc_->RespondWorkflowTaskFailed(req);
    return;
  }

  const std::string& run_id = info.run_id;
  const auto& events = task.history().events();
  const std::int64_t first_event_id = events.empty() ? 0 : events.Get(0).event_id();
  // "Full history" begins at the WorkflowExecutionStarted event. Anything else
  // (empty, or starting mid-stream) is a sticky continuation or a sticky query
  // task, both of which the server delivers expecting us to hold cached state.
  const bool is_full_history =
      !events.empty() &&
      events.Get(0).event_type() == enums::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED;
  const bool is_query_task = task.has_query();

  // Sticky cache: continue a cached coroutine if this task's history picks up
  // exactly where we left off, or answer a query against the resident state.
  std::shared_ptr<WorkflowRunner> runner;
  bool cache_hit = false;
  if (!is_full_history) {
    const std::lock_guard<std::mutex> lock(cache_mu_);
    auto* slot = cache_.Get(run_id);  // marks the run most-recently-used
    if (slot != nullptr) {
      auto cached = std::static_pointer_cast<WorkflowRunner>(*slot);
      // A sticky query carries no new history and is answered from the resident
      // state as-is; a normal continuation must resume exactly at our checkpoint.
      if (is_query_task || first_event_id == cached->last_event_id() + 1) {
        runner = std::move(cached);
        cache_hit = true;
      }
    }
  }

  if (cache_hit) {
    cache_hits_.fetch_add(1, std::memory_order_relaxed);
    // A query observes current state without advancing the workflow; a normal
    // continuation applies its new events and resumes the parked coroutine.
    if (!is_query_task) {
      runner->ApplyAndResume(task.history());
    }
  } else if (is_query_task) {
    // Query for a run we don't have cached: page in full history and rebuild
    // transient state to answer it (never cached). Null if the fetch failed,
    // which surfaces as a failed query below.
    runner = BuildRunnerForQuery(grpc_, logger_, converter_.get(), info, wf->second);
  } else if (!is_full_history) {
    // Sticky-cache miss on a normal continuation: ask the server to resend full
    // history on the normal queue.
    wsv::RespondWorkflowTaskFailedRequest req;
    req.set_task_token(task.task_token());
    req.set_identity(grpc_->identity());
    req.set_cause(enums::WORKFLOW_TASK_FAILED_CAUSE_RESET_STICKY_TASK_QUEUE);
    grpc_->RespondWorkflowTaskFailed(req);
    return;
  } else {
    replays_.fetch_add(1, std::memory_order_relaxed);
    Prescan scan = ScanHistory(task.history());
    Payloads input = scan.input;
    const bool is_replaying = task.previous_started_event_id() > 0;
    runner = std::make_shared<WorkflowRunner>(info, logger_, is_replaying, std::move(scan),
                                              converter_.get(), wf->second, std::move(input));
    runner->Run();
    // Non-determinism detection: on a real replay the workflow must reproduce the
    // commands history recorded, in order. Queries are read-only reconstructions
    // (answered, not failed), so they are exempt.
    if (!is_query_task) {
      if (const auto mismatch = runner->CheckDeterminism()) {
        logger_->Error("nondeterministic workflow",
                       {log::F("workflow_id", info.workflow_id), log::F("detail", *mismatch)});
        if (panic_policy_ == WorkflowPanicPolicy::FailWorkflow) {
          wsv::RespondWorkflowTaskCompletedRequest fail;
          fail.set_task_token(task.task_token());
          fail.set_identity(grpc_->identity());
          auto* c = fail.add_commands();
          c->set_command_type(enums::COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION);
          *c->mutable_fail_workflow_execution_command_attributes()->mutable_failure() =
              MakeApplicationFailure(*mismatch, "NonDeterministicError");
          grpc_->RespondWorkflowTaskCompleted(fail);
        } else {  // BlockWorkflow (default): fail the task so the server retries it.
          wsv::RespondWorkflowTaskFailedRequest fail;
          fail.set_task_token(task.task_token());
          fail.set_identity(grpc_->identity());
          fail.set_cause(enums::WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR);
          *fail.mutable_failure() = MakeApplicationFailure(*mismatch, "NonDeterministicError");
          grpc_->RespondWorkflowTaskFailed(fail);
        }
        return;  // never cache a poisoned runner
      }
      // Don't cache a context built solely to answer a (full-history) query.
      const std::lock_guard<std::mutex> lock(cache_mu_);
      cache_.Put(run_id, runner);  // evicts the LRU run if over capacity
    }
  }

  // A legacy direct-query task carries a single `query` and no new commands.
  if (task.has_query()) {
    wsv::RespondQueryTaskCompletedRequest qreq;
    qreq.set_task_token(task.task_token());
    try {
      if (!runner) {
        throw ApplicationError("could not load workflow state to answer query", "QueryFailed");
      }
      if (runner->IsDone()) {
        throw ApplicationError("cannot query a completed workflow", "QueryFailed");
      }
      const Payloads answer =
          runner->RunQuery(task.query().query_type(), FromProtoPayloads(task.query().query_args()));
      qreq.set_completed_type(enums::QUERY_RESULT_TYPE_ANSWERED);
      if (!answer.empty()) {
        *qreq.mutable_query_result() = ToProtoPayloads(answer);
      }
    } catch (const std::exception& e) {
      qreq.set_completed_type(enums::QUERY_RESULT_TYPE_FAILED);
      qreq.set_error_message(e.what());
    }
    grpc_->RespondQueryTaskCompleted(qreq);
    return;
  }

  wsv::RespondWorkflowTaskCompletedRequest req;
  req.set_task_token(task.task_token());
  req.set_identity(grpc_->identity());
  // Route future tasks for this run to our sticky queue (so they arrive as
  // incremental continuations rather than full-history replays).
  if (!sticky_queue_.empty()) {
    auto* sticky = req.mutable_sticky_attributes();
    sticky->mutable_worker_task_queue()->set_name(sticky_queue_);
    sticky->mutable_worker_task_queue()->set_kind(enums::TASK_QUEUE_KIND_STICKY);
    sticky->mutable_worker_task_queue()->set_normal_name(task_queue_);
    *sticky->mutable_schedule_to_start_timeout() = ToProtoDuration(std::chrono::seconds(10));
  }
  for (const auto& c : runner->commands()) {
    *req.add_commands() = c;
  }
  // Process workflow updates delivered as protocol messages: optionally validate,
  // then accept, run the handler against live state, and complete. A validator
  // that throws rejects the update before acceptance — a Rejection message with
  // no command, so nothing is written to history and the handler never runs.
  for (const auto& msg : task.messages()) {
    update::Request request;
    if (!msg.body().UnpackTo(&request)) {
      continue;  // not an update request
    }
    const std::string& instance_id = msg.protocol_instance_id();
    const std::string update_name = request.input().name();
    const Payloads update_args = FromProtoPayloads(request.input().args());

    // Validation phase. The validator runs only on the live path (an inbound
    // update message); on replay the acceptance is already in history. Throwing
    // rejects the update ephemerally: a Rejection message and NO command.
    try {
      runner->ValidateUpdate(update_name, update_args);
    } catch (const std::exception& e) {
      auto* reject_msg = req.add_messages();
      reject_msg->set_id(instance_id + "/reject");
      reject_msg->set_protocol_instance_id(instance_id);
      update::Rejection rejection;
      rejection.set_rejected_request_message_id(msg.id());
      rejection.set_rejected_request_sequencing_event_id(msg.event_id());
      *rejection.mutable_rejected_request() = request;
      *rejection.mutable_failure() = MakeApplicationFailure(e.what(), "UpdateValidationRejected");
      static_cast<void>(reject_msg->mutable_body()->PackFrom(rejection));
      continue;  // do not accept, do not run the handler — no history written
    }

    // Accept the update (matches the Go SDK's message body, including the
    // request's sequencing event id).
    auto* accept_msg = req.add_messages();
    accept_msg->set_id(instance_id + "/accept");
    accept_msg->set_protocol_instance_id(instance_id);
    update::Acceptance acceptance;
    acceptance.set_accepted_request_message_id(msg.id());
    acceptance.set_accepted_request_sequencing_event_id(msg.event_id());
    *acceptance.mutable_accepted_request() = request;
    static_cast<void>(accept_msg->mutable_body()->PackFrom(acceptance));  // cannot fail here

    // Run the handler and complete the update.
    update::Response response;
    response.mutable_meta()->set_update_id(instance_id);
    response.mutable_meta()->set_identity(grpc_->identity());
    try {
      const Payloads result = runner->RunUpdate(update_name, update_args);
      *response.mutable_outcome()->mutable_success() = ToProtoPayloads(result);
    } catch (const std::exception& e) {
      *response.mutable_outcome()->mutable_failure() = MakeApplicationFailure(e.what(), "UpdateFailed");
    }
    auto* complete_msg = req.add_messages();
    complete_msg->set_id(instance_id + "/complete");
    complete_msg->set_protocol_instance_id(instance_id);
    static_cast<void>(complete_msg->mutable_body()->PackFrom(response));
  }
  if (runner->Completed()) {
    auto* c = req.add_commands();
    c->set_command_type(enums::COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION);
    auto* attr = c->mutable_complete_workflow_execution_command_attributes();
    if (!runner->result().empty()) {
      *attr->mutable_result() = ToProtoPayloads(runner->result());
    }
  } else if (runner->Failed()) {
    auto* c = req.add_commands();
    c->set_command_type(enums::COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION);
    *c->mutable_fail_workflow_execution_command_attributes()->mutable_failure() = runner->failure();
  } else if (runner->IsContinueAsNew()) {
    auto* c = req.add_commands();
    c->set_command_type(enums::COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION);
    auto* attr = c->mutable_continue_as_new_workflow_execution_command_attributes();
    attr->mutable_workflow_type()->set_name(runner->continue_as_new().workflow_type);
    if (!runner->continue_as_new().input.empty()) {
      *attr->mutable_input() = ToProtoPayloads(runner->continue_as_new().input);
    }
  }
  for (const auto& entry : task.queries()) {
    query::WorkflowQueryResult result;
    try {
      if (runner->IsDone()) {
        throw ApplicationError("cannot query a completed workflow", "QueryFailed");
      }
      const Payloads answer = runner->RunQuery(entry.second.query_type(),
                                               FromProtoPayloads(entry.second.query_args()));
      result.set_result_type(enums::QUERY_RESULT_TYPE_ANSWERED);
      if (!answer.empty()) {
        *result.mutable_answer() = ToProtoPayloads(answer);
      }
    } catch (const std::exception& e) {
      result.set_result_type(enums::QUERY_RESULT_TYPE_FAILED);
      result.set_error_message(e.what());
    }
    (*req.mutable_query_results())[entry.first] = result;
  }
  grpc_->RespondWorkflowTaskCompleted(req);

  // Drop finished workflows from the cache.
  if (runner->IsDone()) {
    const std::lock_guard<std::mutex> lock(cache_mu_);
    cache_.Erase(run_id);
  }
}

}  // namespace temporal::internal
