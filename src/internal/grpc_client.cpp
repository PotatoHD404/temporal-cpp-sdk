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

template <class Resp, class Invoke>
Resp UnaryCall(const char* name, bool poll, Invoke&& invoke) {
  grpc::ClientContext ctx;
  if (poll) {
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(kPollSeconds));
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

}  // namespace

GrpcClient::GrpcClient(const std::string& target, std::string ns, std::string identity)
    : ns_(std::move(ns)), identity_(std::move(identity)) {
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  stub_ = wsv::WorkflowService::NewStub(channel);
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

wsv::SignalWithStartWorkflowExecutionResponse GrpcClient::SignalWithStartWorkflowExecution(
    const wsv::SignalWithStartWorkflowExecutionRequest& req) {
  return UnaryCall<wsv::SignalWithStartWorkflowExecutionResponse>(
      "SignalWithStartWorkflowExecution", false,
      [&](grpc::ClientContext* c, wsv::SignalWithStartWorkflowExecutionResponse* p) {
        return stub_->SignalWithStartWorkflowExecution(c, req, p);
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

}  // namespace temporal::internal
