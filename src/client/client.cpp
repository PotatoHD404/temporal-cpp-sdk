#include <temporal/client/client.h>

#include <string>
#include <string_view>
#include <utility>

#include "google/protobuf/util/json_util.h"
#include "temporal/api/enums/v1/event_type.pb.h"
#include "temporal/api/enums/v1/update.pb.h"
#include "temporal/api/enums/v1/workflow.pb.h"
#include "temporal/api/history/v1/message.pb.h"
#include "temporal/api/query/v1/message.pb.h"
#include "temporal/api/schedule/v1/message.pb.h"
#include "temporal/api/update/v1/message.pb.h"

#include "internal/grpc_client.h"
#include "internal/proto_util.h"

#include <temporal/common/errors.h>
#include <temporal/log/logger.h>

namespace temporal::client {

namespace enums = ::temporal::api::enums::v1;
namespace wf = ::temporal::api::workflow::v1;

namespace {

// Strips the verbose "WORKFLOW_EXECUTION_STATUS_" prefix off the proto enum name,
// e.g. WORKFLOW_EXECUTION_STATUS_RUNNING -> "Running".
std::string StatusName(enums::WorkflowExecutionStatus status) {
  std::string name = enums::WorkflowExecutionStatus_Name(status);
  const std::string prefix = "WORKFLOW_EXECUTION_STATUS_";
  if (name.starts_with(prefix)) {
    name = name.substr(prefix.size());
  }
  return name;
}

// Builds a WorkflowDescription from a visibility/describe WorkflowExecutionInfo.
WorkflowDescription DescriptionFromInfo(const wf::WorkflowExecutionInfo& info) {
  WorkflowDescription out;
  out.workflow_id = info.execution().workflow_id();
  out.run_id = info.execution().run_id();
  out.workflow_type = info.type().name();
  out.status = StatusName(info.status());
  for (const auto& [key, value] : info.memo().fields()) {
    out.memo[key] = internal::FromProtoPayload(value);
  }
  return out;
}

}  // namespace

WorkflowHandle::WorkflowHandle(std::shared_ptr<internal::GrpcClient> grpc,
                               std::shared_ptr<DataConverter> converter, std::string ns,
                               std::string workflow_id, std::string run_id)
    : grpc_(std::move(grpc)),
      converter_(std::move(converter)),
      ns_(std::move(ns)),
      workflow_id_(std::move(workflow_id)),
      run_id_(std::move(run_id)) {}

std::string WorkflowHandle::FetchHistoryJson() {
  ::temporal::api::history::v1::History full;
  std::string token;
  for (;;) {
    internal::wsv::GetWorkflowExecutionHistoryRequest req;
    req.set_namespace_(ns_);
    req.mutable_execution()->set_workflow_id(workflow_id_);
    if (!run_id_.empty()) {
      req.mutable_execution()->set_run_id(run_id_);
    }
    req.set_skip_archival(true);
    if (!token.empty()) {
      req.set_next_page_token(token);
    }
    const auto resp = grpc_->GetWorkflowExecutionHistory(req);
    for (const auto& ev : resp.history().events()) {
      *full.add_events() = ev;
    }
    token = resp.next_page_token();
    if (token.empty()) {
      break;
    }
  }
  std::string json;
  static_cast<void>(google::protobuf::util::MessageToJsonString(full, &json));
  return json;
}

WorkflowDescription WorkflowHandle::Describe() {
  internal::wsv::DescribeWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.mutable_execution()->set_workflow_id(workflow_id_);
  if (!run_id_.empty()) {
    req.mutable_execution()->set_run_id(run_id_);
  }
  const auto resp = grpc_->DescribeWorkflowExecution(req);
  return DescriptionFromInfo(resp.workflow_execution_info());
}

Payloads WorkflowHandle::ResultPayloads() {
  for (;;) {
    internal::wsv::GetWorkflowExecutionHistoryRequest req;
    req.set_namespace_(ns_);
    req.mutable_execution()->set_workflow_id(workflow_id_);
    if (!run_id_.empty()) {
      req.mutable_execution()->set_run_id(run_id_);
    }
    req.set_wait_new_event(true);
    req.set_history_event_filter_type(enums::HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT);
    req.set_skip_archival(true);

    const auto resp = grpc_->GetWorkflowExecutionHistory(req);
    for (const auto& ev : resp.history().events()) {
      if (ev.event_type() == enums::EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED) {
        return internal::FromProtoPayloads(
            ev.workflow_execution_completed_event_attributes().result());
      }
      if (ev.event_type() == enums::EVENT_TYPE_WORKFLOW_EXECUTION_FAILED) {
        throw WorkflowFailedError(
            "workflow failed: " +
            ev.workflow_execution_failed_event_attributes().failure().message());
      }
      if (ev.event_type() == enums::EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW) {
        // Follow the continue-as-new chain to the next run.
        run_id_ = ev.workflow_execution_continued_as_new_event_attributes().new_execution_run_id();
        break;
      }
      throw WorkflowFailedError("workflow did not complete successfully: " +
                                std::string(enums::EventType_Name(ev.event_type())));
    }
    // No close event yet; the long poll returned empty, so try again.
  }
}

void WorkflowHandle::Signal(std::string_view signal_name, const Payloads& args) {
  internal::wsv::SignalWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.mutable_workflow_execution()->set_workflow_id(workflow_id_);
  if (!run_id_.empty()) {
    req.mutable_workflow_execution()->set_run_id(run_id_);
  }
  req.set_signal_name(std::string(signal_name));
  if (!args.empty()) {
    *req.mutable_input() = internal::ToProtoPayloads(args);
  }
  req.set_identity(grpc_->identity());
  req.set_request_id(internal::NewUuid());
  grpc_->SignalWorkflowExecution(req);
}

void WorkflowHandle::Cancel() {
  internal::wsv::RequestCancelWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.mutable_workflow_execution()->set_workflow_id(workflow_id_);
  if (!run_id_.empty()) {
    req.mutable_workflow_execution()->set_run_id(run_id_);
  }
  req.set_identity(grpc_->identity());
  req.set_request_id(internal::NewUuid());
  grpc_->RequestCancelWorkflowExecution(req);
}

void WorkflowHandle::Terminate(std::string_view reason) {
  internal::wsv::TerminateWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.mutable_workflow_execution()->set_workflow_id(workflow_id_);
  if (!run_id_.empty()) {
    req.mutable_workflow_execution()->set_run_id(run_id_);
  }
  if (!reason.empty()) {
    req.set_reason(std::string(reason));
  }
  req.set_identity(grpc_->identity());
  grpc_->TerminateWorkflowExecution(req);
}

Payloads WorkflowHandle::QueryPayloads(std::string_view query_type, const Payloads& args) {
  internal::wsv::QueryWorkflowRequest req;
  req.set_namespace_(ns_);
  req.mutable_execution()->set_workflow_id(workflow_id_);
  if (!run_id_.empty()) {
    req.mutable_execution()->set_run_id(run_id_);
  }
  req.mutable_query()->set_query_type(std::string(query_type));
  if (!args.empty()) {
    *req.mutable_query()->mutable_query_args() = internal::ToProtoPayloads(args);
  }
  const auto resp = grpc_->QueryWorkflow(req);
  return internal::FromProtoPayloads(resp.query_result());
}

Payloads WorkflowHandle::UpdatePayloads(std::string_view update_name, const Payloads& args) {
  internal::wsv::UpdateWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.mutable_workflow_execution()->set_workflow_id(workflow_id_);
  if (!run_id_.empty()) {
    req.mutable_workflow_execution()->set_run_id(run_id_);
  }
  req.mutable_request()->mutable_meta()->set_update_id(internal::NewUuid());
  req.mutable_request()->mutable_input()->set_name(std::string(update_name));
  if (!args.empty()) {
    *req.mutable_request()->mutable_input()->mutable_args() = internal::ToProtoPayloads(args);
  }
  req.mutable_wait_policy()->set_lifecycle_stage(
      enums::UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED);

  const auto resp = grpc_->UpdateWorkflowExecution(req);
  if (resp.outcome().has_failure()) {
    throw WorkflowFailedError("update failed: " + resp.outcome().failure().message());
  }
  return internal::FromProtoPayloads(resp.outcome().success());
}

Client Client::Connect(const ClientOptions& options) {
  Client c;
  c.ns_ = options.ns.empty() ? std::string("default") : options.ns;
  c.identity_ = options.identity.empty() ? internal::DefaultIdentity() : options.identity;
  c.converter_ = options.data_converter ? options.data_converter : DataConverter::Default();
  c.logger_ = options.logger ? options.logger : log::DefaultLogger();
  const std::string target =
      options.target.empty() ? std::string("localhost:7233") : options.target;
  c.grpc_ =
      std::make_shared<internal::GrpcClient>(target, c.ns_, c.identity_, options.tls, options.api_key);
  return c;
}

WorkflowHandle Client::GetHandle(std::string workflow_id, std::string run_id) {
  return {grpc_, converter_, ns_, std::move(workflow_id), std::move(run_id)};
}

std::vector<WorkflowDescription> Client::ListWorkflows(const std::string& query) {
  std::vector<WorkflowDescription> out;
  std::string page_token;
  for (;;) {
    internal::wsv::ListWorkflowExecutionsRequest req;
    req.set_namespace_(ns_);
    if (!query.empty()) {
      req.set_query(query);
    }
    if (!page_token.empty()) {
      req.set_next_page_token(page_token);
    }
    const auto resp = grpc_->ListWorkflowExecutions(req);
    for (const auto& info : resp.executions()) {
      out.push_back(DescriptionFromInfo(info));
    }
    page_token = resp.next_page_token();
    if (page_token.empty()) {
      break;
    }
  }
  return out;
}

std::int64_t Client::CountWorkflows(const std::string& query) {
  internal::wsv::CountWorkflowExecutionsRequest req;
  req.set_namespace_(ns_);
  if (!query.empty()) {
    req.set_query(query);
  }
  return grpc_->CountWorkflowExecutions(req).count();
}

WorkflowHandle Client::StartWorkflowPayloads(const StartWorkflowOptions& options,
                                             std::string_view workflow_type, const Payloads& input) {
  const std::string workflow_id = options.id.empty() ? internal::NewUuid() : options.id;

  internal::wsv::StartWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.set_workflow_id(workflow_id);
  req.mutable_workflow_type()->set_name(std::string(workflow_type));
  req.mutable_task_queue()->set_name(options.task_queue);
  if (!input.empty()) {
    *req.mutable_input() = internal::ToProtoPayloads(input);
  }
  req.set_identity(identity_);
  req.set_request_id(internal::NewUuid());
  if (options.execution_timeout.count() > 0) {
    *req.mutable_workflow_execution_timeout() = internal::ToProtoDuration(options.execution_timeout);
  }
  if (options.run_timeout.count() > 0) {
    *req.mutable_workflow_run_timeout() = internal::ToProtoDuration(options.run_timeout);
  }
  if (options.task_timeout.count() > 0) {
    *req.mutable_workflow_task_timeout() = internal::ToProtoDuration(options.task_timeout);
  }
  if (options.retry_policy_set) {
    *req.mutable_retry_policy() = internal::ToProtoRetryPolicy(options.retry_policy);
  }
  for (const auto& [key, value] : options.memo) {
    (*req.mutable_memo()->mutable_fields())[key] = internal::ToProtoPayload(value);
  }
  for (const auto& [key, value] : options.search_attributes) {
    (*req.mutable_search_attributes()->mutable_indexed_fields())[key] = internal::ToProtoPayload(value);
  }

  const auto resp = grpc_->StartWorkflowExecution(req);
  return {grpc_, converter_, ns_, workflow_id, resp.run_id()};
}

WorkflowHandle Client::SignalWithStartWorkflowPayloads(const StartWorkflowOptions& options,
                                                       std::string_view workflow_type,
                                                       std::string_view signal_name,
                                                       const Payloads& signal_input,
                                                       const Payloads& input) {
  const std::string workflow_id = options.id.empty() ? internal::NewUuid() : options.id;

  internal::wsv::SignalWithStartWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.set_workflow_id(workflow_id);
  req.mutable_workflow_type()->set_name(std::string(workflow_type));
  req.mutable_task_queue()->set_name(options.task_queue);
  if (!input.empty()) {
    *req.mutable_input() = internal::ToProtoPayloads(input);
  }
  req.set_signal_name(std::string(signal_name));
  if (!signal_input.empty()) {
    *req.mutable_signal_input() = internal::ToProtoPayloads(signal_input);
  }
  req.set_identity(identity_);
  req.set_request_id(internal::NewUuid());
  if (options.execution_timeout.count() > 0) {
    *req.mutable_workflow_execution_timeout() = internal::ToProtoDuration(options.execution_timeout);
  }
  if (options.run_timeout.count() > 0) {
    *req.mutable_workflow_run_timeout() = internal::ToProtoDuration(options.run_timeout);
  }
  if (options.task_timeout.count() > 0) {
    *req.mutable_workflow_task_timeout() = internal::ToProtoDuration(options.task_timeout);
  }
  if (options.retry_policy_set) {
    *req.mutable_retry_policy() = internal::ToProtoRetryPolicy(options.retry_policy);
  }
  for (const auto& [key, value] : options.memo) {
    (*req.mutable_memo()->mutable_fields())[key] = internal::ToProtoPayload(value);
  }

  const auto resp = grpc_->SignalWithStartWorkflowExecution(req);
  return {grpc_, converter_, ns_, workflow_id, resp.run_id()};
}

void Client::CreateSchedule(const std::string& schedule_id, const ScheduleOptions& options) {
  internal::wsv::CreateScheduleRequest req;
  req.set_namespace_(ns_);
  req.set_schedule_id(schedule_id);
  req.set_identity(identity_);
  req.set_request_id(internal::NewUuid());
  auto* schedule = req.mutable_schedule();
  if (options.interval.count() > 0) {
    *schedule->mutable_spec()->add_interval()->mutable_interval() =
        internal::ToProtoDuration(options.interval);
  }
  auto* start = schedule->mutable_action()->mutable_start_workflow();
  start->set_workflow_id(options.workflow_id.empty() ? schedule_id + "-workflow"
                                                     : options.workflow_id);
  start->mutable_workflow_type()->set_name(options.workflow_type);
  start->mutable_task_queue()->set_name(options.task_queue);
  grpc_->CreateSchedule(req);
}

bool Client::DescribeSchedule(const std::string& schedule_id) {
  internal::wsv::DescribeScheduleRequest req;
  req.set_namespace_(ns_);
  req.set_schedule_id(schedule_id);
  return grpc_->DescribeSchedule(req).has_schedule();
}

void Client::DeleteSchedule(const std::string& schedule_id) {
  internal::wsv::DeleteScheduleRequest req;
  req.set_namespace_(ns_);
  req.set_schedule_id(schedule_id);
  req.set_identity(identity_);
  grpc_->DeleteSchedule(req);
}

void Client::UpdateSchedule(const std::string& schedule_id, const ScheduleOptions& options) {
  internal::wsv::UpdateScheduleRequest req;
  req.set_namespace_(ns_);
  req.set_schedule_id(schedule_id);
  req.set_identity(identity_);
  req.set_request_id(internal::NewUuid());
  auto* schedule = req.mutable_schedule();
  if (options.interval.count() > 0) {
    *schedule->mutable_spec()->add_interval()->mutable_interval() =
        internal::ToProtoDuration(options.interval);
  }
  auto* start = schedule->mutable_action()->mutable_start_workflow();
  start->set_workflow_id(options.workflow_id.empty() ? schedule_id + "-workflow"
                                                     : options.workflow_id);
  start->mutable_workflow_type()->set_name(options.workflow_type);
  start->mutable_task_queue()->set_name(options.task_queue);
  grpc_->UpdateSchedule(req);
}

void Client::TriggerSchedule(const std::string& schedule_id) {
  internal::wsv::PatchScheduleRequest req;
  req.set_namespace_(ns_);
  req.set_schedule_id(schedule_id);
  req.set_identity(identity_);
  req.set_request_id(internal::NewUuid());
  req.mutable_patch()->mutable_trigger_immediately();
  grpc_->PatchSchedule(req);
}

void Client::PauseSchedule(const std::string& schedule_id, const std::string& note) {
  internal::wsv::PatchScheduleRequest req;
  req.set_namespace_(ns_);
  req.set_schedule_id(schedule_id);
  req.set_identity(identity_);
  req.set_request_id(internal::NewUuid());
  req.mutable_patch()->set_pause(note.empty() ? "paused" : note);
  grpc_->PatchSchedule(req);
}

void Client::UnpauseSchedule(const std::string& schedule_id, const std::string& note) {
  internal::wsv::PatchScheduleRequest req;
  req.set_namespace_(ns_);
  req.set_schedule_id(schedule_id);
  req.set_identity(identity_);
  req.set_request_id(internal::NewUuid());
  req.mutable_patch()->set_unpause(note.empty() ? "unpaused" : note);
  grpc_->PatchSchedule(req);
}

std::vector<std::string> Client::ListSchedules() {
  std::vector<std::string> out;
  std::string page_token;
  for (;;) {
    internal::wsv::ListSchedulesRequest req;
    req.set_namespace_(ns_);
    if (!page_token.empty()) {
      req.set_next_page_token(page_token);
    }
    const auto resp = grpc_->ListSchedules(req);
    for (const auto& entry : resp.schedules()) {
      out.push_back(entry.schedule_id());
    }
    page_token = resp.next_page_token();
    if (page_token.empty()) {
      break;
    }
  }
  return out;
}

void Client::CompleteActivityPayloads(const std::string& task_token, const Payloads& result) {
  internal::wsv::RespondActivityTaskCompletedRequest req;
  req.set_namespace_(ns_);
  req.set_task_token(task_token);
  req.set_identity(identity_);
  if (!result.empty()) {
    *req.mutable_result() = internal::ToProtoPayloads(result);
  }
  grpc_->RespondActivityTaskCompleted(req);
}

void Client::FailActivity(const std::string& task_token, const std::string& message,
                          const std::string& type) {
  internal::wsv::RespondActivityTaskFailedRequest req;
  req.set_namespace_(ns_);
  req.set_task_token(task_token);
  req.set_identity(identity_);
  auto* failure = req.mutable_failure();
  failure->set_message(message);
  failure->mutable_application_failure_info()->set_type(type);
  grpc_->RespondActivityTaskFailed(req);
}

}  // namespace temporal::client
