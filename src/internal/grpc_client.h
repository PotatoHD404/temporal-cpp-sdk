#pragma once

#include <memory>
#include <string>

#include <temporal/common/options.h>

#include "temporal/api/operatorservice/v1/request_response.pb.h"
#include "temporal/api/operatorservice/v1/service.grpc.pb.h"
#include "temporal/api/workflowservice/v1/request_response.pb.h"
#include "temporal/api/workflowservice/v1/service.grpc.pb.h"

namespace temporal::internal {

namespace wsv = ::temporal::api::workflowservice::v1;
namespace osv = ::temporal::api::operatorservice::v1;

// Thin wrapper over the generated WorkflowService stub. Each method issues one
// unary RPC. Long-poll methods (PollWorkflowTaskQueue, PollActivityTaskQueue,
// GetWorkflowExecutionHistory) tolerate DEADLINE_EXCEEDED by returning an empty
// response; everything else throws RpcError on a non-OK status.
class GrpcClient {
 public:
  GrpcClient(const std::string& target, std::string ns, std::string identity,
             const TlsConfig& tls = {}, std::string api_key = {});

  const std::string& ns() const { return ns_; }
  const std::string& identity() const { return identity_; }

  wsv::StartWorkflowExecutionResponse StartWorkflowExecution(
      const wsv::StartWorkflowExecutionRequest& req);
  wsv::DescribeWorkflowExecutionResponse DescribeWorkflowExecution(
      const wsv::DescribeWorkflowExecutionRequest& req);
  wsv::ListWorkflowExecutionsResponse ListWorkflowExecutions(
      const wsv::ListWorkflowExecutionsRequest& req);
  wsv::CountWorkflowExecutionsResponse CountWorkflowExecutions(
      const wsv::CountWorkflowExecutionsRequest& req);
  wsv::SignalWithStartWorkflowExecutionResponse SignalWithStartWorkflowExecution(
      const wsv::SignalWithStartWorkflowExecutionRequest& req);
  wsv::CreateScheduleResponse CreateSchedule(const wsv::CreateScheduleRequest& req);
  wsv::DescribeScheduleResponse DescribeSchedule(const wsv::DescribeScheduleRequest& req);
  wsv::DeleteScheduleResponse DeleteSchedule(const wsv::DeleteScheduleRequest& req);
  wsv::UpdateScheduleResponse UpdateSchedule(const wsv::UpdateScheduleRequest& req);
  wsv::PatchScheduleResponse PatchSchedule(const wsv::PatchScheduleRequest& req);
  wsv::ListSchedulesResponse ListSchedules(const wsv::ListSchedulesRequest& req);
  wsv::GetWorkflowExecutionHistoryResponse GetWorkflowExecutionHistory(
      const wsv::GetWorkflowExecutionHistoryRequest& req);
  wsv::PollWorkflowTaskQueueResponse PollWorkflowTaskQueue(
      const wsv::PollWorkflowTaskQueueRequest& req);
  wsv::RespondWorkflowTaskCompletedResponse RespondWorkflowTaskCompleted(
      const wsv::RespondWorkflowTaskCompletedRequest& req);
  wsv::RespondWorkflowTaskFailedResponse RespondWorkflowTaskFailed(
      const wsv::RespondWorkflowTaskFailedRequest& req);
  wsv::PollActivityTaskQueueResponse PollActivityTaskQueue(
      const wsv::PollActivityTaskQueueRequest& req);
  wsv::RespondActivityTaskCompletedResponse RespondActivityTaskCompleted(
      const wsv::RespondActivityTaskCompletedRequest& req);
  wsv::RespondActivityTaskFailedResponse RespondActivityTaskFailed(
      const wsv::RespondActivityTaskFailedRequest& req);
  wsv::RecordActivityTaskHeartbeatResponse RecordActivityTaskHeartbeat(
      const wsv::RecordActivityTaskHeartbeatRequest& req);
  wsv::SignalWorkflowExecutionResponse SignalWorkflowExecution(
      const wsv::SignalWorkflowExecutionRequest& req);
  wsv::RequestCancelWorkflowExecutionResponse RequestCancelWorkflowExecution(
      const wsv::RequestCancelWorkflowExecutionRequest& req);
  wsv::TerminateWorkflowExecutionResponse TerminateWorkflowExecution(
      const wsv::TerminateWorkflowExecutionRequest& req);
  wsv::QueryWorkflowResponse QueryWorkflow(const wsv::QueryWorkflowRequest& req);
  wsv::RespondQueryTaskCompletedResponse RespondQueryTaskCompleted(
      const wsv::RespondQueryTaskCompletedRequest& req);
  wsv::UpdateWorkflowExecutionResponse UpdateWorkflowExecution(
      const wsv::UpdateWorkflowExecutionRequest& req);
  wsv::ResetWorkflowExecutionResponse ResetWorkflowExecution(
      const wsv::ResetWorkflowExecutionRequest& req);
  wsv::GetWorkerBuildIdCompatibilityResponse GetWorkerBuildIdCompatibility(
      const wsv::GetWorkerBuildIdCompatibilityRequest& req);
  wsv::UpdateWorkerBuildIdCompatibilityResponse UpdateWorkerBuildIdCompatibility(
      const wsv::UpdateWorkerBuildIdCompatibilityRequest& req);
  wsv::GetWorkerVersioningRulesResponse GetWorkerVersioningRules(
      const wsv::GetWorkerVersioningRulesRequest& req);
  wsv::UpdateWorkerVersioningRulesResponse UpdateWorkerVersioningRules(
      const wsv::UpdateWorkerVersioningRulesRequest& req);
  wsv::StartBatchOperationResponse StartBatchOperation(
      const wsv::StartBatchOperationRequest& req);
  wsv::StopBatchOperationResponse StopBatchOperation(const wsv::StopBatchOperationRequest& req);
  wsv::DescribeBatchOperationResponse DescribeBatchOperation(
      const wsv::DescribeBatchOperationRequest& req);
  wsv::ListBatchOperationsResponse ListBatchOperations(
      const wsv::ListBatchOperationsRequest& req);
  wsv::GetClusterInfoResponse GetClusterInfo(const wsv::GetClusterInfoRequest& req);
  wsv::ListWorkerDeploymentsResponse ListWorkerDeployments(
      const wsv::ListWorkerDeploymentsRequest& req);
  wsv::DescribeWorkerDeploymentResponse DescribeWorkerDeployment(
      const wsv::DescribeWorkerDeploymentRequest& req);
  wsv::SetWorkerDeploymentCurrentVersionResponse SetWorkerDeploymentCurrentVersion(
      const wsv::SetWorkerDeploymentCurrentVersionRequest& req);

  // OperatorService RPCs (separate gRPC service sharing the same channel).
  osv::AddSearchAttributesResponse AddSearchAttributes(
      const osv::AddSearchAttributesRequest& req);
  osv::ListSearchAttributesResponse ListSearchAttributes(
      const osv::ListSearchAttributesRequest& req);
  osv::RemoveSearchAttributesResponse RemoveSearchAttributes(
      const osv::RemoveSearchAttributesRequest& req);
  osv::ListClustersResponse ListClusters(const osv::ListClustersRequest& req);
  osv::CreateNexusEndpointResponse CreateNexusEndpoint(
      const osv::CreateNexusEndpointRequest& req);
  osv::GetNexusEndpointResponse GetNexusEndpoint(const osv::GetNexusEndpointRequest& req);
  osv::ListNexusEndpointsResponse ListNexusEndpoints(const osv::ListNexusEndpointsRequest& req);
  osv::AddOrUpdateRemoteClusterResponse AddOrUpdateRemoteCluster(
      const osv::AddOrUpdateRemoteClusterRequest& req);
  osv::RemoveRemoteClusterResponse RemoveRemoteCluster(
      const osv::RemoveRemoteClusterRequest& req);
  osv::DeleteNamespaceResponse DeleteNamespace(const osv::DeleteNamespaceRequest& req);

 private:
  // Issues one unary RPC, attaching auth metadata (Authorization + namespace) when
  // an API key is configured. Defined in the .cpp (only instantiated there).
  template <class Resp, class Invoke>
  Resp UnaryCall(const char* name, bool poll, Invoke&& invoke) const;

  std::unique_ptr<wsv::WorkflowService::Stub> stub_;
  std::unique_ptr<osv::OperatorService::Stub> operator_stub_;
  std::string ns_;
  std::string identity_;
  std::string api_key_;
};

}  // namespace temporal::internal
