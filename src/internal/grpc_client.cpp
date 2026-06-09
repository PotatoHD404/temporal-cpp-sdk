#include "internal/grpc_client.h"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <grpcpp/grpcpp.h>

#include <temporal/common/errors.h>

namespace temporal::internal {
namespace {

// Idle long-poll bound. Short enough that Worker::Stop() returns promptly; a task
// that becomes available is still delivered immediately (server-side long poll).
constexpr int kPollSeconds = 5;

}  // namespace

GrpcClient::GrpcClient(const std::string& target, std::string ns, std::string identity,
                       const TlsConfig& tls, std::string api_key)
    : ns_(std::move(ns)), identity_(std::move(identity)), api_key_(std::move(api_key)) {
  std::shared_ptr<grpc::ChannelCredentials> creds;
  if (tls.enabled) {
    grpc::SslCredentialsOptions ssl;
    ssl.pem_root_certs = tls.server_ca_cert;   // empty -> system trust store
    ssl.pem_private_key = tls.client_key;      // mTLS (optional)
    ssl.pem_cert_chain = tls.client_cert;      // mTLS (optional)
    creds = grpc::SslCredentials(ssl);
  } else {
    creds = grpc::InsecureChannelCredentials();
  }
  grpc::ChannelArguments args;
  if (!tls.server_name.empty()) {
    args.SetSslTargetNameOverride(tls.server_name);
  }
  auto channel = grpc::CreateCustomChannel(target, creds, args);
  stub_ = wsv::WorkflowService::NewStub(channel);
  // OperatorService lives on the same frontend channel as WorkflowService.
  operator_stub_ = osv::OperatorService::NewStub(channel);
}

template <class Resp, class Invoke>
Resp GrpcClient::UnaryCall(const char* name, bool poll, Invoke&& invoke) const {
  grpc::ClientContext ctx;
  if (poll) {
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(kPollSeconds));
  }
  if (!api_key_.empty()) {
    ctx.AddMetadata("authorization", "Bearer " + api_key_);
    ctx.AddMetadata("temporal-namespace", ns_);
  }
  Resp resp;
  const grpc::Status status = std::forward<Invoke>(invoke)(&ctx, &resp);
  if (!status.ok()) {
    if (poll && status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      return Resp{};
    }
    throw RpcError(std::string("rpc ") + name + " failed: " + status.error_message());
  }
  return resp;
}

wsv::StartWorkflowExecutionResponse GrpcClient::StartWorkflowExecution(
    const wsv::StartWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::StartWorkflowExecutionResponse>(
      "StartWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::StartWorkflowExecutionResponse* p) {
        return stub_->StartWorkflowExecution(c, req, p);
      });
}

wsv::DescribeWorkflowExecutionResponse GrpcClient::DescribeWorkflowExecution(
    const wsv::DescribeWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::DescribeWorkflowExecutionResponse>(
      "DescribeWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::DescribeWorkflowExecutionResponse* p) {
        return stub_->DescribeWorkflowExecution(c, req, p);
      });
}

wsv::ListWorkflowExecutionsResponse GrpcClient::ListWorkflowExecutions(
    const wsv::ListWorkflowExecutionsRequest& req) {
  return UnaryCall<wsv::ListWorkflowExecutionsResponse>(
      "ListWorkflowExecutions", false,
      [&](grpc::ClientContext* c, wsv::ListWorkflowExecutionsResponse* p) {
        return stub_->ListWorkflowExecutions(c, req, p);
      });
}

wsv::CountWorkflowExecutionsResponse GrpcClient::CountWorkflowExecutions(
    const wsv::CountWorkflowExecutionsRequest& req) {
  return UnaryCall<wsv::CountWorkflowExecutionsResponse>(
      "CountWorkflowExecutions", false,
      [&](grpc::ClientContext* c, wsv::CountWorkflowExecutionsResponse* p) {
        return stub_->CountWorkflowExecutions(c, req, p);
      });
}

wsv::SignalWithStartWorkflowExecutionResponse GrpcClient::SignalWithStartWorkflowExecution(
    const wsv::SignalWithStartWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::SignalWithStartWorkflowExecutionResponse>(
      "SignalWithStartWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::SignalWithStartWorkflowExecutionResponse* p) {
        return stub_->SignalWithStartWorkflowExecution(c, req, p);
      });
}

wsv::CreateScheduleResponse GrpcClient::CreateSchedule(const wsv::CreateScheduleRequest& req) {
  return UnaryCall<wsv::CreateScheduleResponse>(
      "CreateSchedule", false,
      [&](grpc::ClientContext* c, wsv::CreateScheduleResponse* p) {
        return stub_->CreateSchedule(c, req, p);
      });
}

wsv::DescribeScheduleResponse GrpcClient::DescribeSchedule(const wsv::DescribeScheduleRequest& req) {
  return UnaryCall<wsv::DescribeScheduleResponse>(
      "DescribeSchedule", false,
      [&](grpc::ClientContext* c, wsv::DescribeScheduleResponse* p) {
        return stub_->DescribeSchedule(c, req, p);
      });
}

wsv::DeleteScheduleResponse GrpcClient::DeleteSchedule(const wsv::DeleteScheduleRequest& req) {
  return UnaryCall<wsv::DeleteScheduleResponse>(
      "DeleteSchedule", false,
      [&](grpc::ClientContext* c, wsv::DeleteScheduleResponse* p) {
        return stub_->DeleteSchedule(c, req, p);
      });
}

wsv::UpdateScheduleResponse GrpcClient::UpdateSchedule(const wsv::UpdateScheduleRequest& req) {
  return UnaryCall<wsv::UpdateScheduleResponse>(
      "UpdateSchedule", false, [&](grpc::ClientContext* c, wsv::UpdateScheduleResponse* p) {
        return stub_->UpdateSchedule(c, req, p);
      });
}

wsv::PatchScheduleResponse GrpcClient::PatchSchedule(const wsv::PatchScheduleRequest& req) {
  return UnaryCall<wsv::PatchScheduleResponse>(
      "PatchSchedule", false, [&](grpc::ClientContext* c, wsv::PatchScheduleResponse* p) {
        return stub_->PatchSchedule(c, req, p);
      });
}

wsv::ListSchedulesResponse GrpcClient::ListSchedules(const wsv::ListSchedulesRequest& req) {
  return UnaryCall<wsv::ListSchedulesResponse>(
      "ListSchedules", false, [&](grpc::ClientContext* c, wsv::ListSchedulesResponse* p) {
        return stub_->ListSchedules(c, req, p);
      });
}

wsv::GetWorkflowExecutionHistoryResponse GrpcClient::GetWorkflowExecutionHistory(
    const wsv::GetWorkflowExecutionHistoryRequest& req) {
  return UnaryCall<wsv::GetWorkflowExecutionHistoryResponse>(
      "GetWorkflowExecutionHistory", true,
      [&](grpc::ClientContext* c, wsv::GetWorkflowExecutionHistoryResponse* p) {
        return stub_->GetWorkflowExecutionHistory(c, req, p);
      });
}

wsv::PollWorkflowTaskQueueResponse GrpcClient::PollWorkflowTaskQueue(
    const wsv::PollWorkflowTaskQueueRequest& req) {
  return UnaryCall<wsv::PollWorkflowTaskQueueResponse>(
      "PollWorkflowTaskQueue", true,
      [&](grpc::ClientContext* c, wsv::PollWorkflowTaskQueueResponse* p) {
        return stub_->PollWorkflowTaskQueue(c, req, p);
      });
}

wsv::RespondWorkflowTaskCompletedResponse GrpcClient::RespondWorkflowTaskCompleted(
    const wsv::RespondWorkflowTaskCompletedRequest& req) {
  return UnaryCall<wsv::RespondWorkflowTaskCompletedResponse>(
      "RespondWorkflowTaskCompleted", false,
      [&](grpc::ClientContext* c, wsv::RespondWorkflowTaskCompletedResponse* p) {
        return stub_->RespondWorkflowTaskCompleted(c, req, p);
      });
}

wsv::RespondWorkflowTaskFailedResponse GrpcClient::RespondWorkflowTaskFailed(
    const wsv::RespondWorkflowTaskFailedRequest& req) {
  return UnaryCall<wsv::RespondWorkflowTaskFailedResponse>(
      "RespondWorkflowTaskFailed", false,
      [&](grpc::ClientContext* c, wsv::RespondWorkflowTaskFailedResponse* p) {
        return stub_->RespondWorkflowTaskFailed(c, req, p);
      });
}

wsv::PollActivityTaskQueueResponse GrpcClient::PollActivityTaskQueue(
    const wsv::PollActivityTaskQueueRequest& req) {
  return UnaryCall<wsv::PollActivityTaskQueueResponse>(
      "PollActivityTaskQueue", true,
      [&](grpc::ClientContext* c, wsv::PollActivityTaskQueueResponse* p) {
        return stub_->PollActivityTaskQueue(c, req, p);
      });
}

wsv::RespondActivityTaskCompletedResponse GrpcClient::RespondActivityTaskCompleted(
    const wsv::RespondActivityTaskCompletedRequest& req) {
  return UnaryCall<wsv::RespondActivityTaskCompletedResponse>(
      "RespondActivityTaskCompleted", false,
      [&](grpc::ClientContext* c, wsv::RespondActivityTaskCompletedResponse* p) {
        return stub_->RespondActivityTaskCompleted(c, req, p);
      });
}

wsv::RespondActivityTaskFailedResponse GrpcClient::RespondActivityTaskFailed(
    const wsv::RespondActivityTaskFailedRequest& req) {
  return UnaryCall<wsv::RespondActivityTaskFailedResponse>(
      "RespondActivityTaskFailed", false,
      [&](grpc::ClientContext* c, wsv::RespondActivityTaskFailedResponse* p) {
        return stub_->RespondActivityTaskFailed(c, req, p);
      });
}

wsv::RecordActivityTaskHeartbeatResponse GrpcClient::RecordActivityTaskHeartbeat(
    const wsv::RecordActivityTaskHeartbeatRequest& req) {
  return UnaryCall<wsv::RecordActivityTaskHeartbeatResponse>(
      "RecordActivityTaskHeartbeat", false,
      [&](grpc::ClientContext* c, wsv::RecordActivityTaskHeartbeatResponse* p) {
        return stub_->RecordActivityTaskHeartbeat(c, req, p);
      });
}

wsv::SignalWorkflowExecutionResponse GrpcClient::SignalWorkflowExecution(
    const wsv::SignalWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::SignalWorkflowExecutionResponse>(
      "SignalWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::SignalWorkflowExecutionResponse* p) {
        return stub_->SignalWorkflowExecution(c, req, p);
      });
}

wsv::RequestCancelWorkflowExecutionResponse GrpcClient::RequestCancelWorkflowExecution(
    const wsv::RequestCancelWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::RequestCancelWorkflowExecutionResponse>(
      "RequestCancelWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::RequestCancelWorkflowExecutionResponse* p) {
        return stub_->RequestCancelWorkflowExecution(c, req, p);
      });
}

wsv::TerminateWorkflowExecutionResponse GrpcClient::TerminateWorkflowExecution(
    const wsv::TerminateWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::TerminateWorkflowExecutionResponse>(
      "TerminateWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::TerminateWorkflowExecutionResponse* p) {
        return stub_->TerminateWorkflowExecution(c, req, p);
      });
}

wsv::QueryWorkflowResponse GrpcClient::QueryWorkflow(const wsv::QueryWorkflowRequest& req) {
  return UnaryCall<wsv::QueryWorkflowResponse>(
      "QueryWorkflow", false, [&](grpc::ClientContext* c, wsv::QueryWorkflowResponse* p) {
        return stub_->QueryWorkflow(c, req, p);
      });
}

wsv::RespondQueryTaskCompletedResponse GrpcClient::RespondQueryTaskCompleted(
    const wsv::RespondQueryTaskCompletedRequest& req) {
  return UnaryCall<wsv::RespondQueryTaskCompletedResponse>(
      "RespondQueryTaskCompleted", false,
      [&](grpc::ClientContext* c, wsv::RespondQueryTaskCompletedResponse* p) {
        return stub_->RespondQueryTaskCompleted(c, req, p);
      });
}

wsv::UpdateWorkflowExecutionResponse GrpcClient::UpdateWorkflowExecution(
    const wsv::UpdateWorkflowExecutionRequest& req) {
  // Blocks until the update reaches the requested lifecycle stage.
  return UnaryCall<wsv::UpdateWorkflowExecutionResponse>(
      "UpdateWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::UpdateWorkflowExecutionResponse* p) {
        return stub_->UpdateWorkflowExecution(c, req, p);
      });
}

wsv::ResetWorkflowExecutionResponse GrpcClient::ResetWorkflowExecution(
    const wsv::ResetWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::ResetWorkflowExecutionResponse>(
      "ResetWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::ResetWorkflowExecutionResponse* p) {
        return stub_->ResetWorkflowExecution(c, req, p);
      });
}

wsv::GetWorkerBuildIdCompatibilityResponse GrpcClient::GetWorkerBuildIdCompatibility(
    const wsv::GetWorkerBuildIdCompatibilityRequest& req) {
  return UnaryCall<wsv::GetWorkerBuildIdCompatibilityResponse>(
      "GetWorkerBuildIdCompatibility", false,
      [&](grpc::ClientContext* c, wsv::GetWorkerBuildIdCompatibilityResponse* p) {
        return stub_->GetWorkerBuildIdCompatibility(c, req, p);
      });
}

wsv::UpdateWorkerBuildIdCompatibilityResponse GrpcClient::UpdateWorkerBuildIdCompatibility(
    const wsv::UpdateWorkerBuildIdCompatibilityRequest& req) {
  return UnaryCall<wsv::UpdateWorkerBuildIdCompatibilityResponse>(
      "UpdateWorkerBuildIdCompatibility", false,
      [&](grpc::ClientContext* c, wsv::UpdateWorkerBuildIdCompatibilityResponse* p) {
        return stub_->UpdateWorkerBuildIdCompatibility(c, req, p);
      });
}

wsv::GetWorkerVersioningRulesResponse GrpcClient::GetWorkerVersioningRules(
    const wsv::GetWorkerVersioningRulesRequest& req) {
  return UnaryCall<wsv::GetWorkerVersioningRulesResponse>(
      "GetWorkerVersioningRules", false,
      [&](grpc::ClientContext* c, wsv::GetWorkerVersioningRulesResponse* p) {
        return stub_->GetWorkerVersioningRules(c, req, p);
      });
}

wsv::UpdateWorkerVersioningRulesResponse GrpcClient::UpdateWorkerVersioningRules(
    const wsv::UpdateWorkerVersioningRulesRequest& req) {
  return UnaryCall<wsv::UpdateWorkerVersioningRulesResponse>(
      "UpdateWorkerVersioningRules", false,
      [&](grpc::ClientContext* c, wsv::UpdateWorkerVersioningRulesResponse* p) {
        return stub_->UpdateWorkerVersioningRules(c, req, p);
      });
}

wsv::StartBatchOperationResponse GrpcClient::StartBatchOperation(
    const wsv::StartBatchOperationRequest& req) {
  return UnaryCall<wsv::StartBatchOperationResponse>(
      "StartBatchOperation", false,
      [&](grpc::ClientContext* c, wsv::StartBatchOperationResponse* p) {
        return stub_->StartBatchOperation(c, req, p);
      });
}

wsv::StopBatchOperationResponse GrpcClient::StopBatchOperation(
    const wsv::StopBatchOperationRequest& req) {
  return UnaryCall<wsv::StopBatchOperationResponse>(
      "StopBatchOperation", false, [&](grpc::ClientContext* c, wsv::StopBatchOperationResponse* p) {
        return stub_->StopBatchOperation(c, req, p);
      });
}

wsv::DescribeBatchOperationResponse GrpcClient::DescribeBatchOperation(
    const wsv::DescribeBatchOperationRequest& req) {
  return UnaryCall<wsv::DescribeBatchOperationResponse>(
      "DescribeBatchOperation", false,
      [&](grpc::ClientContext* c, wsv::DescribeBatchOperationResponse* p) {
        return stub_->DescribeBatchOperation(c, req, p);
      });
}

wsv::ListBatchOperationsResponse GrpcClient::ListBatchOperations(
    const wsv::ListBatchOperationsRequest& req) {
  return UnaryCall<wsv::ListBatchOperationsResponse>(
      "ListBatchOperations", false,
      [&](grpc::ClientContext* c, wsv::ListBatchOperationsResponse* p) {
        return stub_->ListBatchOperations(c, req, p);
      });
}

wsv::GetClusterInfoResponse GrpcClient::GetClusterInfo(const wsv::GetClusterInfoRequest& req) {
  return UnaryCall<wsv::GetClusterInfoResponse>(
      "GetClusterInfo", false,
      [&](grpc::ClientContext* c, wsv::GetClusterInfoResponse* p) {
        return stub_->GetClusterInfo(c, req, p);
      });
}

osv::AddSearchAttributesResponse GrpcClient::AddSearchAttributes(
    const osv::AddSearchAttributesRequest& req) {
  return UnaryCall<osv::AddSearchAttributesResponse>(
      "AddSearchAttributes", false,
      [&](grpc::ClientContext* c, osv::AddSearchAttributesResponse* p) {
        return operator_stub_->AddSearchAttributes(c, req, p);
      });
}

osv::ListSearchAttributesResponse GrpcClient::ListSearchAttributes(
    const osv::ListSearchAttributesRequest& req) {
  return UnaryCall<osv::ListSearchAttributesResponse>(
      "ListSearchAttributes", false,
      [&](grpc::ClientContext* c, osv::ListSearchAttributesResponse* p) {
        return operator_stub_->ListSearchAttributes(c, req, p);
      });
}

osv::RemoveSearchAttributesResponse GrpcClient::RemoveSearchAttributes(
    const osv::RemoveSearchAttributesRequest& req) {
  return UnaryCall<osv::RemoveSearchAttributesResponse>(
      "RemoveSearchAttributes", false,
      [&](grpc::ClientContext* c, osv::RemoveSearchAttributesResponse* p) {
        return operator_stub_->RemoveSearchAttributes(c, req, p);
      });
}

osv::ListClustersResponse GrpcClient::ListClusters(const osv::ListClustersRequest& req) {
  return UnaryCall<osv::ListClustersResponse>(
      "ListClusters", false,
      [&](grpc::ClientContext* c, osv::ListClustersResponse* p) {
        return operator_stub_->ListClusters(c, req, p);
      });
}

}  // namespace temporal::internal
