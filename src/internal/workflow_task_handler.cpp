#include "internal/workflow_task_handler.h"

#include <chrono>
#include <cstdint>
#include <exception>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "temporal/api/command/v1/message.pb.h"
#include "temporal/api/enums/v1/command_type.pb.h"
#include "temporal/api/enums/v1/event_type.pb.h"
#include "temporal/api/history/v1/message.pb.h"

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

// Outcome of an activity, derived by scanning history.
struct ActivityOutcome {
  bool scheduled = false;
  bool resolved = false;  // completed, failed, or timed out
  bool failed = false;
  Payloads result;
  std::string failure_type;
  std::string failure_message;
};

struct TimerOutcome {
  bool started = false;
  bool fired = false;
};

struct Prescan {
  Payloads input;
  std::unordered_map<std::string, ActivityOutcome> activities;  // keyed by activity_id
  std::unordered_map<std::string, TimerOutcome> timers;         // keyed by timer_id
};

// Walk the event history once, indexing the inputs/outcomes the runner replays
// against. Activities are correlated to their completion/failure via the
// scheduled event id that those events carry.
Prescan ScanHistory(const hist::History& history) {
  Prescan ps;
  std::unordered_map<std::int64_t, std::string> sched_event_to_activity;
  for (const auto& ev : history.events()) {
    switch (ev.event_type()) {
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        ps.input = FromProtoPayloads(ev.workflow_execution_started_event_attributes().input());
        break;
      case enums::EVENT_TYPE_ACTIVITY_TASK_SCHEDULED: {
        const auto& a = ev.activity_task_scheduled_event_attributes();
        ps.activities[a.activity_id()].scheduled = true;
        sched_event_to_activity[ev.event_id()] = a.activity_id();
        break;
      }
      case enums::EVENT_TYPE_ACTIVITY_TASK_COMPLETED: {
        const auto& a = ev.activity_task_completed_event_attributes();
        const auto it = sched_event_to_activity.find(a.scheduled_event_id());
        if (it != sched_event_to_activity.end()) {
          auto& o = ps.activities[it->second];
          o.resolved = true;
          o.result = FromProtoPayloads(a.result());
        }
        break;
      }
      case enums::EVENT_TYPE_ACTIVITY_TASK_FAILED: {
        const auto& a = ev.activity_task_failed_event_attributes();
        const auto it = sched_event_to_activity.find(a.scheduled_event_id());
        if (it != sched_event_to_activity.end()) {
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
        const auto it = sched_event_to_activity.find(a.scheduled_event_id());
        if (it != sched_event_to_activity.end()) {
          auto& o = ps.activities[it->second];
          o.resolved = true;
          o.failed = true;
          o.failure_message = "activity timed out";
          o.failure_type = "TimeoutError";
        }
        break;
      }
      case enums::EVENT_TYPE_TIMER_STARTED:
        ps.timers[ev.timer_started_event_attributes().timer_id()].started = true;
        break;
      case enums::EVENT_TYPE_TIMER_FIRED:
        ps.timers[ev.timer_fired_event_attributes().timer_id()].fired = true;
        break;
      default:
        break;
    }
  }
  return ps;
}

// The WorkflowOutbound the running workflow talks to. Activity/timer ids are
// assigned deterministically by call order, so the same call on a later replay
// maps to the same history event.
class WorkflowRunner final : public WorkflowOutbound {
 public:
  WorkflowRunner(workflow::WorkflowInfo info, std::shared_ptr<log::Logger> logger,
                 bool is_replaying, Prescan scan)
      : info_(std::move(info)),
        logger_(std::move(logger)),
        is_replaying_(is_replaying),
        scan_(std::move(scan)) {}

  std::shared_ptr<FutureState> ScheduleActivity(std::string_view activity_type,
                                                const Payloads& input,
                                                const ActivityOptions& options) override {
    const std::string id = std::to_string(activity_seq_++);
    auto state = std::make_shared<FutureState>();
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
    return state;
  }

  std::shared_ptr<FutureState> StartTimer(std::chrono::nanoseconds duration) override {
    const std::string id = "t" + std::to_string(timer_seq_++);
    auto state = std::make_shared<FutureState>();
    const auto it = scan_.timers.find(id);
    if (it != scan_.timers.end() && it->second.started) {
      state->ready = it->second.fired;
    } else {
      EmitStartTimer(id, duration);
    }
    return state;
  }

  void Block(const std::shared_ptr<FutureState>& state) override {
    if (!state->ready) {
      throw WorkflowBlocked{};
    }
  }

  const workflow::WorkflowInfo& Info() const override { return info_; }
  log::Logger& Logger() const override { return *logger_; }
  bool IsReplaying() const override { return is_replaying_; }

  const std::vector<cmd::Command>& commands() const { return commands_; }

 private:
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

  workflow::WorkflowInfo info_;
  std::shared_ptr<log::Logger> logger_;
  bool is_replaying_;
  Prescan scan_;
  std::vector<cmd::Command> commands_;
  int activity_seq_ = 0;
  int timer_seq_ = 0;
};

}  // namespace

WorkflowTaskHandler::WorkflowTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                                         std::shared_ptr<log::Logger> logger, std::string task_queue)
    : grpc_(grpc),
      converter_(std::move(converter)),
      logger_(std::move(logger)),
      task_queue_(std::move(task_queue)) {}

void WorkflowTaskHandler::Register(std::string name, worker::WorkflowFn fn) {
  workflows_.insert_or_assign(std::move(name), std::move(fn));
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

  Prescan scan = ScanHistory(task.history());
  const Payloads input = scan.input;
  const bool is_replaying = task.previous_started_event_id() > 0;
  WorkflowRunner runner(info, logger_, is_replaying, std::move(scan));
  workflow::Context ctx(&runner, converter_.get());

  enum class Term { kBlocked, kCompleted, kFailed };
  Term term = Term::kBlocked;
  Payloads result;
  tapi::failure::v1::Failure failure;
  try {
    result = wf->second(ctx, input);
    term = Term::kCompleted;
  } catch (const WorkflowBlocked&) {
    term = Term::kBlocked;
  } catch (const ApplicationError& e) {
    term = Term::kFailed;
    failure = MakeApplicationFailure(e.what(), e.type());
  } catch (const std::exception& e) {
    term = Term::kFailed;
    failure = MakeApplicationFailure(e.what(), "std::exception");
  }

  wsv::RespondWorkflowTaskCompletedRequest req;
  req.set_task_token(task.task_token());
  req.set_identity(grpc_->identity());
  for (const auto& c : runner.commands()) {
    *req.add_commands() = c;
  }
  if (term == Term::kCompleted) {
    auto* c = req.add_commands();
    c->set_command_type(enums::COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION);
    auto* attr = c->mutable_complete_workflow_execution_command_attributes();
    if (!result.empty()) {
      *attr->mutable_result() = ToProtoPayloads(result);
    }
  } else if (term == Term::kFailed) {
    auto* c = req.add_commands();
    c->set_command_type(enums::COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION);
    *c->mutable_fail_workflow_execution_command_attributes()->mutable_failure() = failure;
  }
  grpc_->RespondWorkflowTaskCompleted(req);
}

}  // namespace temporal::internal
