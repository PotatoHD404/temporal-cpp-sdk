#include <temporal/client/client.h>

#include <cctype>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "google/protobuf/util/json_util.h"
#include "temporal/api/enums/v1/batch_operation.pb.h"
#include "temporal/api/enums/v1/common.pb.h"
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
#include <temporal/interceptor/interceptor.h>
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

// Maps a user-facing search-attribute type string to its IndexedValueType enum,
// throwing std::invalid_argument on an unrecognized type (before any RPC).
enums::IndexedValueType IndexedValueTypeFromString(const std::string& type) {
  if (type == "Keyword") return enums::INDEXED_VALUE_TYPE_KEYWORD;
  if (type == "Text") return enums::INDEXED_VALUE_TYPE_TEXT;
  if (type == "Int") return enums::INDEXED_VALUE_TYPE_INT;
  if (type == "Double") return enums::INDEXED_VALUE_TYPE_DOUBLE;
  if (type == "Bool") return enums::INDEXED_VALUE_TYPE_BOOL;
  if (type == "Datetime") return enums::INDEXED_VALUE_TYPE_DATETIME;
  if (type == "KeywordList") return enums::INDEXED_VALUE_TYPE_KEYWORD_LIST;
  throw std::invalid_argument("unknown search attribute type: " + type);
}

// Strips the "INDEXED_VALUE_TYPE_" prefix and TitleCases the enum name, e.g.
// INDEXED_VALUE_TYPE_KEYWORD_LIST -> "KeywordList".
std::string IndexedValueTypeName(enums::IndexedValueType type) {
  std::string name = enums::IndexedValueType_Name(type);
  const std::string prefix = "INDEXED_VALUE_TYPE_";
  if (name.starts_with(prefix)) {
    name = name.substr(prefix.size());
  }
  bool at_word_start = true;
  std::string out;
  out.reserve(name.size());
  for (char ch : name) {
    if (ch == '_') {
      at_word_start = true;
      continue;
    }
    out.push_back(at_word_start ? ch : static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    at_word_start = false;
  }
  return out;
}

// Terminal client-outbound interceptor: issues the real RPC. The interceptor
// chain wraps this; with no interceptors it is the head and the call runs
// directly. Holds the actual-work callback so the Client method stays the source
// of the RPC logic.
class RootClientOutbound : public interceptor::ClientOutboundInterceptor {
 public:
  using StartFn = std::function<std::string(interceptor::StartWorkflowInput&, interceptor::Header&)>;
  explicit RootClientOutbound(StartFn start) : start_(std::move(start)) {}
  std::string StartWorkflow(interceptor::StartWorkflowInput& in,
                            interceptor::Header& header) override {
    return start_(in, header);
  }

 private:
  StartFn start_;
};

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
  c.interceptors_ = options.interceptors;
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

  // Chain terminal: issue the real RPC using the (interceptor-augmented) header.
  auto do_start = [this, &workflow_id](interceptor::StartWorkflowInput& in,
                                       interceptor::Header& header) -> std::string {
    internal::wsv::StartWorkflowExecutionRequest req;
    req.set_namespace_(ns_);
    req.set_workflow_id(workflow_id);
    req.mutable_workflow_type()->set_name(in.workflow_type);
    req.mutable_task_queue()->set_name(in.options.task_queue);
    if (!in.args.empty()) {
      *req.mutable_input() = internal::ToProtoPayloads(in.args);
    }
    req.set_identity(identity_);
    req.set_request_id(internal::NewUuid());
    if (in.options.execution_timeout.count() > 0) {
      *req.mutable_workflow_execution_timeout() =
          internal::ToProtoDuration(in.options.execution_timeout);
    }
    if (in.options.run_timeout.count() > 0) {
      *req.mutable_workflow_run_timeout() = internal::ToProtoDuration(in.options.run_timeout);
    }
    if (in.options.task_timeout.count() > 0) {
      *req.mutable_workflow_task_timeout() = internal::ToProtoDuration(in.options.task_timeout);
    }
    if (in.options.retry_policy_set) {
      *req.mutable_retry_policy() = internal::ToProtoRetryPolicy(in.options.retry_policy);
    }
    for (const auto& [key, value] : in.options.memo) {
      (*req.mutable_memo()->mutable_fields())[key] = internal::ToProtoPayload(value);
    }
    for (const auto& [key, value] : in.options.search_attributes) {
      (*req.mutable_search_attributes()->mutable_indexed_fields())[key] =
          internal::ToProtoPayload(value);
    }
    for (const auto& [key, value] : header) {  // interceptors may have added entries
      (*req.mutable_header()->mutable_fields())[key] = internal::ToProtoPayload(value);
    }
    return grpc_->StartWorkflowExecution(req).run_id();
  };

  // Run StartWorkflow through the client-outbound interceptor chain (terminal =
  // do_start). With no interceptors the head is the terminal: a direct call.
  RootClientOutbound root(do_start);
  std::vector<interceptor::Interceptor*> factories;
  factories.reserve(interceptors_.size());
  for (const auto& i : interceptors_) {
    factories.push_back(i.get());
  }
  auto chain = interceptor::BuildClientOutboundChain(factories, &root);
  interceptor::StartWorkflowInput in;
  in.workflow_type = std::string(workflow_type);
  in.options = options;
  in.options.id = workflow_id;
  in.args = input;
  interceptor::Header header(options.headers);
  const std::string run_id = chain.head()->StartWorkflow(in, header);
  return {grpc_, converter_, ns_, workflow_id, run_id};
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
  try {
    static_cast<void>(grpc_->DescribeSchedule(req));
    return true;  // the RPC succeeded => the schedule exists
  } catch (const RpcError& e) {
    if (e.not_found()) {
      return false;  // contract: an absent schedule reports false, not an error
    }
    throw;
  }
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

std::string Client::ResetWorkflow(const std::string& workflow_id, const std::string& run_id,
                                  const std::string& reason,
                                  std::int64_t workflow_task_finish_event_id) {
  internal::wsv::ResetWorkflowExecutionRequest req;
  req.set_namespace_(ns_);
  req.mutable_workflow_execution()->set_workflow_id(workflow_id);
  if (!run_id.empty()) {
    req.mutable_workflow_execution()->set_run_id(run_id);
  }
  req.set_reason(reason);
  req.set_workflow_task_finish_event_id(workflow_task_finish_event_id);
  req.set_request_id(internal::NewUuid());
  return grpc_->ResetWorkflowExecution(req).run_id();
}

std::vector<std::vector<std::string>> Client::GetWorkerBuildIdCompatibility(
    const std::string& task_queue) {
  internal::wsv::GetWorkerBuildIdCompatibilityRequest req;
  req.set_namespace_(ns_);
  req.set_task_queue(task_queue);
  const auto resp = grpc_->GetWorkerBuildIdCompatibility(req);
  std::vector<std::vector<std::string>> out;
  for (const auto& set : resp.major_version_sets()) {
    std::vector<std::string> ids;
    for (const auto& id : set.build_ids()) {
      ids.push_back(id);
    }
    out.push_back(std::move(ids));
  }
  return out;
}

void Client::UpdateWorkerBuildIdCompatibility(const std::string& task_queue,
                                              const std::string& build_id) {
  internal::wsv::UpdateWorkerBuildIdCompatibilityRequest req;
  req.set_namespace_(ns_);
  req.set_task_queue(task_queue);
  req.set_add_new_build_id_in_new_default_set(build_id);
  grpc_->UpdateWorkerBuildIdCompatibility(req);
}

void Client::PromoteWorkerBuildIdSet(const std::string& task_queue, const std::string& build_id) {
  internal::wsv::UpdateWorkerBuildIdCompatibilityRequest req;
  req.set_namespace_(ns_);
  req.set_task_queue(task_queue);
  req.set_promote_set_by_build_id(build_id);
  grpc_->UpdateWorkerBuildIdCompatibility(req);
}

WorkerVersioningRules Client::GetWorkerVersioningRules(const std::string& task_queue) {
  internal::wsv::GetWorkerVersioningRulesRequest req;
  req.set_namespace_(ns_);
  req.set_task_queue(task_queue);
  const auto resp = grpc_->GetWorkerVersioningRules(req);
  WorkerVersioningRules out;
  for (const auto& rule : resp.assignment_rules()) {
    out.assignment_rule_target_build_ids.push_back(rule.rule().target_build_id());
  }
  for (const auto& rule : resp.compatible_redirect_rules()) {
    out.redirect_rules.emplace_back(rule.rule().source_build_id(), rule.rule().target_build_id());
  }
  out.conflict_token = resp.conflict_token();
  return out;
}

void Client::InsertWorkerAssignmentRule(const std::string& task_queue,
                                        const std::string& target_build_id) {
  internal::wsv::UpdateWorkerVersioningRulesRequest req;
  req.set_namespace_(ns_);
  req.set_task_queue(task_queue);
  // The server enforces optimistic concurrency: every update must echo the
  // current conflict token, even the first insert, so fetch it first.
  req.set_conflict_token(GetWorkerVersioningRules(task_queue).conflict_token);
  auto* insert = req.mutable_insert_assignment_rule();
  insert->set_rule_index(0);
  insert->mutable_rule()->set_target_build_id(target_build_id);
  grpc_->UpdateWorkerVersioningRules(req);
}

void Client::AddWorkerRedirectRule(const std::string& task_queue,
                                   const std::string& source_build_id,
                                   const std::string& target_build_id) {
  internal::wsv::UpdateWorkerVersioningRulesRequest req;
  req.set_namespace_(ns_);
  req.set_task_queue(task_queue);
  req.set_conflict_token(GetWorkerVersioningRules(task_queue).conflict_token);
  auto* add = req.mutable_add_compatible_redirect_rule();
  add->mutable_rule()->set_source_build_id(source_build_id);
  add->mutable_rule()->set_target_build_id(target_build_id);
  grpc_->UpdateWorkerVersioningRules(req);
}

void Client::StartBatchTerminate(const std::string& job_id, const std::string& visibility_query,
                                 const std::string& reason) {
  internal::wsv::StartBatchOperationRequest req;
  req.set_namespace_(ns_);
  req.set_job_id(job_id);
  req.set_visibility_query(visibility_query);
  req.set_reason(reason);
  req.mutable_termination_operation()->set_identity(identity_);
  grpc_->StartBatchOperation(req);
}

void Client::StartBatchCancel(const std::string& job_id, const std::string& visibility_query,
                              const std::string& reason) {
  internal::wsv::StartBatchOperationRequest req;
  req.set_namespace_(ns_);
  req.set_job_id(job_id);
  req.set_visibility_query(visibility_query);
  req.set_reason(reason);
  req.mutable_cancellation_operation()->set_identity(identity_);
  grpc_->StartBatchOperation(req);
}

BatchOperationDescription Client::DescribeBatchOperation(const std::string& job_id) {
  internal::wsv::DescribeBatchOperationRequest req;
  req.set_namespace_(ns_);
  req.set_job_id(job_id);
  const auto resp = grpc_->DescribeBatchOperation(req);
  BatchOperationDescription out;
  out.job_id = resp.job_id();
  std::string state = enums::BatchOperationState_Name(resp.state());
  const std::string prefix = "BATCH_OPERATION_STATE_";
  if (state.starts_with(prefix)) {
    state = state.substr(prefix.size());
  }
  out.state = state;
  out.total_operations = resp.total_operation_count();
  out.completed_operations = resp.complete_operation_count();
  out.failed_operations = resp.failure_operation_count();
  return out;
}

std::vector<std::string> Client::ListBatchOperations() {
  std::vector<std::string> out;
  std::string page_token;
  for (;;) {
    internal::wsv::ListBatchOperationsRequest req;
    req.set_namespace_(ns_);
    if (!page_token.empty()) {
      req.set_next_page_token(page_token);
    }
    const auto resp = grpc_->ListBatchOperations(req);
    for (const auto& info : resp.operation_info()) {
      out.push_back(info.job_id());
    }
    page_token = resp.next_page_token();
    if (page_token.empty()) {
      break;
    }
  }
  return out;
}

void Client::AddSearchAttributes(const std::map<std::string, std::string>& name_to_type) {
  internal::osv::AddSearchAttributesRequest req;
  req.set_namespace_(ns_);
  // Validate every type first so the whole call is rejected before the RPC.
  for (const auto& [name, type] : name_to_type) {
    (*req.mutable_search_attributes())[name] = IndexedValueTypeFromString(type);
  }
  grpc_->AddSearchAttributes(req);
}

SearchAttributes Client::ListSearchAttributes() {
  internal::osv::ListSearchAttributesRequest req;
  req.set_namespace_(ns_);
  const auto resp = grpc_->ListSearchAttributes(req);
  SearchAttributes out;
  for (const auto& [name, type] : resp.custom_attributes()) {
    out.custom[name] = IndexedValueTypeName(type);
  }
  for (const auto& [name, type] : resp.system_attributes()) {
    out.system[name] = IndexedValueTypeName(type);
  }
  return out;
}

void Client::RemoveSearchAttributes(const std::vector<std::string>& names) {
  internal::osv::RemoveSearchAttributesRequest req;
  req.set_namespace_(ns_);
  for (const auto& name : names) {
    req.add_search_attributes(name);
  }
  grpc_->RemoveSearchAttributes(req);
}

ClusterDescription Client::DescribeCluster() {
  // Cluster-scoped: GetClusterInfo carries no namespace and takes an empty request.
  internal::wsv::GetClusterInfoRequest req;
  const auto resp = grpc_->GetClusterInfo(req);
  ClusterDescription out;
  out.cluster_name = resp.cluster_name();
  out.cluster_id = resp.cluster_id();
  out.server_version = resp.server_version();
  out.history_shard_count = resp.history_shard_count();
  out.persistence_store = resp.persistence_store();
  out.visibility_store = resp.visibility_store();
  return out;
}

std::vector<std::string> Client::ListClusters() {
  std::vector<std::string> out;
  std::string page_token;
  for (;;) {
    // Cluster-scoped: ListClusters carries no namespace.
    internal::osv::ListClustersRequest req;
    if (!page_token.empty()) {
      req.set_next_page_token(page_token);
    }
    const auto resp = grpc_->ListClusters(req);
    for (const auto& cluster : resp.clusters()) {
      out.push_back(cluster.cluster_name());
    }
    page_token = resp.next_page_token();
    if (page_token.empty()) {
      break;
    }
  }
  return out;
}

std::string Client::CreateNexusEndpoint(const std::string& name,
                                        const std::string& target_task_queue) {
  internal::osv::CreateNexusEndpointRequest req;
  // The request itself is not namespace-scoped; the namespace lives on the
  // worker target (EndpointTarget.Worker.namespace).
  auto* spec = req.mutable_spec();
  spec->set_name(name);
  auto* worker = spec->mutable_target()->mutable_worker();
  worker->set_namespace_(grpc_->ns());
  worker->set_task_queue(target_task_queue);
  return grpc_->CreateNexusEndpoint(req).endpoint().id();
}

NexusEndpointDescription Client::GetNexusEndpoint(const std::string& id) {
  internal::osv::GetNexusEndpointRequest req;
  req.set_id(id);
  const auto resp = grpc_->GetNexusEndpoint(req);
  const auto& endpoint = resp.endpoint();
  NexusEndpointDescription out;
  out.id = endpoint.id();
  out.name = endpoint.spec().name();
  out.target_namespace = endpoint.spec().target().worker().namespace_();
  out.target_task_queue = endpoint.spec().target().worker().task_queue();
  return out;
}

std::vector<std::string> Client::ListNexusEndpoints() {
  std::vector<std::string> out;
  std::string page_token;
  for (;;) {
    // Cluster-scoped: ListNexusEndpoints carries no namespace.
    internal::osv::ListNexusEndpointsRequest req;
    if (!page_token.empty()) {
      req.set_next_page_token(page_token);
    }
    const auto resp = grpc_->ListNexusEndpoints(req);
    for (const auto& endpoint : resp.endpoints()) {
      out.push_back(endpoint.spec().name());
    }
    page_token = resp.next_page_token();
    if (page_token.empty()) {
      break;
    }
  }
  return out;
}

std::vector<std::string> Client::ListWorkerDeployments() {
  std::vector<std::string> out;
  std::string page_token;
  for (;;) {
    internal::wsv::ListWorkerDeploymentsRequest req;
    req.set_namespace_(ns_);
    if (!page_token.empty()) {
      req.set_next_page_token(page_token);
    }
    const auto resp = grpc_->ListWorkerDeployments(req);
    for (const auto& summary : resp.worker_deployments()) {
      out.push_back(summary.name());
    }
    page_token = resp.next_page_token();
    if (page_token.empty()) {
      break;
    }
  }
  return out;
}

WorkerDeploymentDescription Client::DescribeWorkerDeployment(const std::string& name) {
  internal::wsv::DescribeWorkerDeploymentRequest req;
  req.set_namespace_(ns_);
  req.set_deployment_name(name);
  const auto resp = grpc_->DescribeWorkerDeployment(req);
  const auto& info = resp.worker_deployment_info();
  WorkerDeploymentDescription out;
  out.name = info.name();
  // Read the non-deprecated structured versions (the flat version strings on
  // RoutingConfig are deprecated). Build ids are empty when unset.
  out.current_version_build_id = info.routing_config().current_deployment_version().build_id();
  out.ramping_version_build_id = info.routing_config().ramping_deployment_version().build_id();
  out.conflict_token = resp.conflict_token();
  return out;
}

std::string Client::SetWorkerDeploymentCurrentVersion(const std::string& deployment_name,
                                                      const std::string& build_id,
                                                      const std::string& conflict_token) {
  internal::wsv::SetWorkerDeploymentCurrentVersionRequest req;
  req.set_namespace_(ns_);
  req.set_deployment_name(deployment_name);
  req.set_build_id(build_id);
  req.set_identity(identity_);
  if (!conflict_token.empty()) {
    req.set_conflict_token(conflict_token);
  }
  return grpc_->SetWorkerDeploymentCurrentVersion(req).conflict_token();
}

void Client::AddOrUpdateRemoteCluster(const std::string& frontend_address, bool enable_connection) {
  // Cluster-scoped: the request carries no namespace.
  internal::osv::AddOrUpdateRemoteClusterRequest req;
  req.set_frontend_address(frontend_address);
  req.set_enable_remote_cluster_connection(enable_connection);
  grpc_->AddOrUpdateRemoteCluster(req);
}

void Client::RemoveRemoteCluster(const std::string& cluster_name) {
  // Cluster-scoped: the request carries no namespace.
  internal::osv::RemoveRemoteClusterRequest req;
  req.set_cluster_name(cluster_name);
  grpc_->RemoveRemoteCluster(req);
}

std::string Client::DeleteNamespace(const std::string& namespace_name) {
  internal::osv::DeleteNamespaceRequest req;
  req.set_namespace_(namespace_name);
  return grpc_->DeleteNamespace(req).deleted_namespace();
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
