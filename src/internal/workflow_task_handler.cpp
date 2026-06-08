#include "internal/workflow_task_handler.h"

#include <chrono>
#include <cstddef>
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
#include "temporal/api/enums/v1/failed_cause.pb.h"
#include "temporal/api/enums/v1/query.pb.h"
#include "temporal/api/enums/v1/task_queue.pb.h"
#include "temporal/api/history/v1/message.pb.h"
#include "temporal/api/query/v1/message.pb.h"
#include "temporal/api/taskqueue/v1/message.pb.h"

#include "internal/coroutine.h"
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

// Outcome of a child workflow, keyed by the (parent-assigned) child workflow_id.
struct ChildOutcome {
  bool initiated = false;
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
  // For incremental (sticky) continuations: correlate completion events to the
  // activity_id of their schedule, and remember how far history has been consumed.
  std::unordered_map<std::int64_t, std::string> sched_event_to_activity;
  std::int64_t last_event_id = 0;
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
      case enums::EVENT_TYPE_TIMER_STARTED:
        ps.timers[ev.timer_started_event_attributes().timer_id()].started = true;
        break;
      case enums::EVENT_TYPE_TIMER_FIRED:
        ps.timers[ev.timer_fired_event_attributes().timer_id()].fired = true;
        break;
      case enums::EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
        ps.children[ev.start_child_workflow_execution_initiated_event_attributes().workflow_id()]
            .initiated = true;
        break;
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
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED: {
        const auto& a = ev.workflow_execution_signaled_event_attributes();
        ps.signals[a.signal_name()].push_back(FromProtoPayloads(a.input()));
        break;
      }
      case enums::EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        ps.cancel_requested = true;
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
  enum class Status { Blocked, Completed, Failed };

  WorkflowRunner(workflow::WorkflowInfo info, std::shared_ptr<log::Logger> logger, bool is_replaying,
                 Prescan scan, const DataConverter* converter, worker::WorkflowFn workflow_fn,
                 Payloads input)
      : info_(std::move(info)),
        logger_(std::move(logger)),
        is_replaying_(is_replaying),
        scan_(std::move(scan)),
        converter_(converter),
        workflow_fn_(std::move(workflow_fn)),
        input_(std::move(input)) {}

  // Run the workflow until it blocks or finishes. Prescan resolves every
  // history-known future up front, so a single resume reaches the current block.
  // Used for the first task and for replay after a sticky-cache miss.
  void Run() {
    commands_.clear();
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
    ApplyEvents(history);
    if (coroutine_) {
      coroutine_->Resume();
    }
  }

  std::int64_t last_event_id() const { return scan_.last_event_id; }

  bool Completed() const { return status_ == Status::Completed; }
  bool Failed() const { return status_ == Status::Failed; }
  bool IsDone() const { return coroutine_ && coroutine_->Done(); }
  const Payloads& result() const { return result_; }
  const tapi::failure::v1::Failure& failure() const { return failure_; }

  // Invoke a registered query handler against the live (suspended) workflow state.
  Payloads RunQuery(const std::string& name, const Payloads& args) {
    const auto it = query_handlers_.find(name);
    if (it == query_handlers_.end()) {
      throw ApplicationError("unknown query type: " + name, "QueryNotRegistered");
    }
    return it->second(args);
  }

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
    activity_futures_[id] = state;  // so incremental completion events can resolve it
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
    timer_futures_[id] = state;
    return state;
  }

  std::shared_ptr<FutureState> StartChildWorkflow(std::string_view workflow_type,
                                                  const Payloads& input,
                                                  const ChildWorkflowOptions& options) override {
    const std::string auto_id = info_.workflow_id + "_c" + std::to_string(child_seq_++);
    const std::string id = options.id.empty() ? auto_id : options.id;
    auto state = std::make_shared<FutureState>();
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

  bool IsCancelRequested() const override { return scan_.cancel_requested; }

  void RegisterQueryHandler(std::string name, QueryFn handler) override {
    query_handlers_.insert_or_assign(std::move(name), std::move(handler));
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
    } catch (const ApplicationError& e) {
      failure_ = MakeApplicationFailure(e.what(), e.type());
      status_ = Status::Failed;
    } catch (const std::exception& e) {
      failure_ = MakeApplicationFailure(e.what(), "std::exception");
      status_ = Status::Failed;
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
        case enums::EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
          scan_.sched_event_to_activity[ev.event_id()] =
              ev.activity_task_scheduled_event_attributes().activity_id();
          break;
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
        case enums::EVENT_TYPE_TIMER_FIRED: {
          const auto it = timer_futures_.find(ev.timer_fired_event_attributes().timer_id());
          if (it != timer_futures_.end()) {
            it->second->ready = true;
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
        case enums::EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED: {
          const auto& a = ev.workflow_execution_signaled_event_attributes();
          scan_.signals[a.signal_name()].push_back(FromProtoPayloads(a.input()));
          break;
        }
        case enums::EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
          scan_.cancel_requested = true;
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

  workflow::WorkflowInfo info_;
  std::shared_ptr<log::Logger> logger_;
  bool is_replaying_;
  Prescan scan_;
  const DataConverter* converter_;
  worker::WorkflowFn workflow_fn_;
  Payloads input_;
  std::unordered_map<std::string, std::size_t> signal_cursor_;
  std::unordered_map<std::string, QueryFn> query_handlers_;
  std::unordered_map<std::string, std::shared_ptr<FutureState>> activity_futures_;  // by activity_id
  std::unordered_map<std::string, std::shared_ptr<FutureState>> timer_futures_;     // by timer_id
  std::unordered_map<std::string, std::shared_ptr<FutureState>> child_futures_;     // by child wf id
  std::vector<cmd::Command> commands_;
  Status status_ = Status::Blocked;
  Payloads result_;
  tapi::failure::v1::Failure failure_;
  int activity_seq_ = 0;
  int timer_seq_ = 0;
  int child_seq_ = 0;
  std::unique_ptr<Coroutine> coroutine_;  // declared last -> destroyed first (tears down thread)
};

}  // namespace

WorkflowTaskHandler::WorkflowTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                                         std::shared_ptr<log::Logger> logger, std::string task_queue,
                                         std::string sticky_queue)
    : grpc_(grpc),
      converter_(std::move(converter)),
      logger_(std::move(logger)),
      task_queue_(std::move(task_queue)),
      sticky_queue_(std::move(sticky_queue)) {}

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

  const std::string& run_id = info.run_id;
  const auto& events = task.history().events();
  const std::int64_t first_event_id = events.empty() ? 0 : events.Get(0).event_id();

  // Sticky cache: continue a cached coroutine if this task's history picks up
  // exactly where we left off; otherwise (re)play from full history.
  std::shared_ptr<WorkflowRunner> runner;
  {
    const std::lock_guard<std::mutex> lock(cache_mu_);
    const auto it = cache_.find(run_id);
    if (it != cache_.end()) {
      auto cached = std::static_pointer_cast<WorkflowRunner>(it->second);
      if (first_event_id == cached->last_event_id() + 1) {
        runner = std::move(cached);
      }
    }
  }

  if (runner) {
    cache_hits_.fetch_add(1, std::memory_order_relaxed);
    runner->ApplyAndResume(task.history());
  } else if (first_event_id > 1) {
    // Incremental history with nothing to continue (sticky-cache miss): ask the
    // server to resend full history on the normal queue.
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
    const std::lock_guard<std::mutex> lock(cache_mu_);
    cache_[run_id] = runner;
  }

  // A legacy direct-query task carries a single `query` and no new commands.
  if (task.has_query()) {
    wsv::RespondQueryTaskCompletedRequest qreq;
    qreq.set_task_token(task.task_token());
    try {
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
    cache_.erase(run_id);
  }
}

}  // namespace temporal::internal
