#pragma once

#include <memory>
#include <string>

#include "temporal/api/workflowservice/v1/request_response.pb.h"
#include "temporal/api/workflowservice/v1/service.grpc.pb.h"

namespace temporal::internal {

namespace wsv = ::temporal::api::workflowservice::v1;

// Thin wrapper over the generated WorkflowService stub. Each method issues one
// unary RPC. Long-poll methods (PollWorkflowTaskQueue, PollActivityTaskQueue,
// GetWorkflowExecutionHistory) tolerate DEADLINE_EXCEEDED by returning an empty
// response; everything else throws RpcError on a non-OK status.
class GrpcClient {
 public:
  GrpcClient(const std::string& target, std::string ns, std::string identity);

  const std::string& ns() const { return ns_; }
  const std::string& identity() const { return identity_; }

  wsv::StartWorkflowExecutionResponse StartWorkflowExecution(
      const wsv::StartWorkflowExecutionRequest& req);
  wsv::DescribeWorkflowExecutionResponse DescribeWorkflowExecution(
      const wsv::DescribeWorkflowExecutionRequest& req);
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

 private:
  std::unique_ptr<wsv::WorkflowService::Stub> stub_;
  std::string ns_;
  std::string identity_;
};

}  // namespace temporal::internal
