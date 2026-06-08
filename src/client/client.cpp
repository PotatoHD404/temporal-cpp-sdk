#include <temporal/client/client.h>

#include <string>
#include <string_view>
#include <utility>

#include "temporal/api/enums/v1/event_type.pb.h"
#include "temporal/api/enums/v1/update.pb.h"
#include "temporal/api/history/v1/message.pb.h"
#include "temporal/api/query/v1/message.pb.h"
#include "temporal/api/update/v1/message.pb.h"

#include "internal/grpc_client.h"
#include "internal/proto_util.h"

#include <temporal/common/errors.h>
#include <temporal/log/logger.h>

namespace temporal::client {

namespace enums = ::temporal::api::enums::v1;

WorkflowHandle::WorkflowHandle(std::shared_ptr<internal::GrpcClient> grpc,
                               std::shared_ptr<DataConverter> converter, std::string ns,
                               std::string workflow_id, std::string run_id)
    : grpc_(std::move(grpc)),
      converter_(std::move(converter)),
      ns_(std::move(ns)),
      workflow_id_(std::move(workflow_id)),
      run_id_(std::move(run_id)) {}

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
  c.grpc_ = std::make_shared<internal::GrpcClient>(target, c.ns_, c.identity_);
  return c;
}

WorkflowHandle Client::GetHandle(std::string workflow_id, std::string run_id) {
  return {grpc_, converter_, ns_, std::move(workflow_id), std::move(run_id)};
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

  const auto resp = grpc_->StartWorkflowExecution(req);
  return {grpc_, converter_, ns_, workflow_id, resp.run_id()};
}

}  // namespace temporal::client
