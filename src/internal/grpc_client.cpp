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
  if (!api_key_.empty()) {
    auth_header_ = "Bearer " + api_key_;  // built once; reused on every RPC
  }
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
  // TestService is served only by the time-skipping test server; the stub is
  // harmless against other servers (its RPCs return UNIMPLEMENTED there).
  test_stub_ = tsv::TestService::NewStub(channel);
}

template <class Resp, class Invoke>
Resp GrpcClient::UnaryCall(const char* name, bool poll, Invoke&& invoke) const {
  grpc::ClientContext ctx;
  if (poll) {
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(kPollSeconds));
  }
  if (!auth_header_.empty()) {
    ctx.AddMetadata("authorization", auth_header_);
    ctx.AddMetadata("temporal-namespace", ns_);
  }
  Resp resp;
  const grpc::Status status = std::forward<Invoke>(invoke)(&ctx, &resp);
  if (!status.ok()) {
    if (poll && status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      return Resp{};
    }
    const std::string msg = std::string("rpc ") + name + " failed: " + status.error_message();
    if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
      throw NotFoundError(msg);  // a RpcError subtype; not_found() stays true
    }
    throw RpcError(msg);
  }
  return resp;
}

// Every RPC wrapper is the same thin shape — forward `req` to the stub method of
// the matching name and route the response through UnaryCall. These macros expand
// to exactly that (one per service), so the request/response types and method
// name are written once. `Poll` is true only for the long-poll RPCs. The header
// still declares each method with its concrete signature.
#define TEMPORAL_WS_CALL(Method, Poll)                                                       \
  wsv::Method##Response GrpcClient::Method(const wsv::Method##Request& req) {                 \
    return UnaryCall<wsv::Method##Response>(#Method, (Poll),                                  \
        [&](grpc::ClientContext* c, wsv::Method##Response* p) { return stub_->Method(c, req, p); }); \
  }
#define TEMPORAL_OP_CALL(Method)                                                              \
  osv::Method##Response GrpcClient::Method(const osv::Method##Request& req) {                 \
    return UnaryCall<osv::Method##Response>(#Method, false, [&](grpc::ClientContext* c,       \
                                                                osv::Method##Response* p) {   \
      return operator_stub_->Method(c, req, p);                                               \
    });                                                                                       \
  }

// WorkflowService (the three long polls pass Poll=true; everything else is unary).
TEMPORAL_WS_CALL(StartWorkflowExecution, false)
TEMPORAL_WS_CALL(DescribeWorkflowExecution, false)
TEMPORAL_WS_CALL(ListWorkflowExecutions, false)
TEMPORAL_WS_CALL(CountWorkflowExecutions, false)
TEMPORAL_WS_CALL(SignalWithStartWorkflowExecution, false)
TEMPORAL_WS_CALL(CreateSchedule, false)
TEMPORAL_WS_CALL(DescribeSchedule, false)
TEMPORAL_WS_CALL(DeleteSchedule, false)
TEMPORAL_WS_CALL(UpdateSchedule, false)
TEMPORAL_WS_CALL(PatchSchedule, false)
TEMPORAL_WS_CALL(ListSchedules, false)
TEMPORAL_WS_CALL(GetWorkflowExecutionHistory, true)
TEMPORAL_WS_CALL(PollWorkflowTaskQueue, true)
TEMPORAL_WS_CALL(RespondWorkflowTaskCompleted, false)
TEMPORAL_WS_CALL(RespondWorkflowTaskFailed, false)
TEMPORAL_WS_CALL(PollActivityTaskQueue, true)
TEMPORAL_WS_CALL(RespondActivityTaskCompleted, false)
TEMPORAL_WS_CALL(RespondActivityTaskFailed, false)
TEMPORAL_WS_CALL(PollNexusTaskQueue, true)
TEMPORAL_WS_CALL(RespondNexusTaskCompleted, false)
TEMPORAL_WS_CALL(RespondNexusTaskFailed, false)
TEMPORAL_WS_CALL(RecordActivityTaskHeartbeat, false)
TEMPORAL_WS_CALL(SignalWorkflowExecution, false)
TEMPORAL_WS_CALL(RequestCancelWorkflowExecution, false)
TEMPORAL_WS_CALL(TerminateWorkflowExecution, false)
TEMPORAL_WS_CALL(QueryWorkflow, false)
TEMPORAL_WS_CALL(RespondQueryTaskCompleted, false)
TEMPORAL_WS_CALL(UpdateWorkflowExecution, false)
TEMPORAL_WS_CALL(ResetWorkflowExecution, false)
TEMPORAL_WS_CALL(GetWorkerBuildIdCompatibility, false)
TEMPORAL_WS_CALL(UpdateWorkerBuildIdCompatibility, false)
TEMPORAL_WS_CALL(GetWorkerVersioningRules, false)
TEMPORAL_WS_CALL(UpdateWorkerVersioningRules, false)
TEMPORAL_WS_CALL(StartBatchOperation, false)
TEMPORAL_WS_CALL(StopBatchOperation, false)
TEMPORAL_WS_CALL(DescribeBatchOperation, false)
TEMPORAL_WS_CALL(ListBatchOperations, false)
TEMPORAL_WS_CALL(GetClusterInfo, false)
TEMPORAL_WS_CALL(ListWorkerDeployments, false)
TEMPORAL_WS_CALL(DescribeWorkerDeployment, false)
TEMPORAL_WS_CALL(SetWorkerDeploymentCurrentVersion, false)

// OperatorService.
TEMPORAL_OP_CALL(AddSearchAttributes)
TEMPORAL_OP_CALL(ListSearchAttributes)
TEMPORAL_OP_CALL(RemoveSearchAttributes)
TEMPORAL_OP_CALL(ListClusters)
TEMPORAL_OP_CALL(CreateNexusEndpoint)
TEMPORAL_OP_CALL(GetNexusEndpoint)
TEMPORAL_OP_CALL(ListNexusEndpoints)
TEMPORAL_OP_CALL(AddOrUpdateRemoteCluster)
TEMPORAL_OP_CALL(RemoveRemoteCluster)
TEMPORAL_OP_CALL(DeleteNamespace)

#undef TEMPORAL_WS_CALL
#undef TEMPORAL_OP_CALL

// TestService (time-skipping test server). The request/response type names don't
// follow the uniform Method##Request/Response shape (Sleep returns SleepResponse;
// GetCurrentTime takes Empty), so each is written out rather than macro-expanded.
tsv::LockTimeSkippingResponse GrpcClient::LockTimeSkipping(const tsv::LockTimeSkippingRequest& req) {
  return UnaryCall<tsv::LockTimeSkippingResponse>(
      "LockTimeSkipping", false,
      [&](grpc::ClientContext* c, tsv::LockTimeSkippingResponse* p) {
        return test_stub_->LockTimeSkipping(c, req, p);
      });
}

tsv::UnlockTimeSkippingResponse GrpcClient::UnlockTimeSkipping(
    const tsv::UnlockTimeSkippingRequest& req) {
  return UnaryCall<tsv::UnlockTimeSkippingResponse>(
      "UnlockTimeSkipping", false,
      [&](grpc::ClientContext* c, tsv::UnlockTimeSkippingResponse* p) {
        return test_stub_->UnlockTimeSkipping(c, req, p);
      });
}

tsv::SleepResponse GrpcClient::TestServerSleep(const tsv::SleepRequest& req) {
  return UnaryCall<tsv::SleepResponse>(
      "Sleep", false,
      [&](grpc::ClientContext* c, tsv::SleepResponse* p) { return test_stub_->Sleep(c, req, p); });
}

tsv::GetCurrentTimeResponse GrpcClient::GetCurrentTime(const google::protobuf::Empty& req) {
  return UnaryCall<tsv::GetCurrentTimeResponse>(
      "GetCurrentTime", false,
      [&](grpc::ClientContext* c, tsv::GetCurrentTimeResponse* p) {
        return test_stub_->GetCurrentTime(c, req, p);
      });
}

}  // namespace temporal::internal
