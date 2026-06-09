#include "internal/nexus_task_handler.h"

#include <exception>
#include <string>
#include <utility>

#include "temporal/api/nexus/v1/message.pb.h"

#include "internal/grpc_client.h"
#include "internal/proto_util.h"

#include <temporal/common/errors.h>

namespace temporal::internal {
namespace {

namespace nexus = ::temporal::api::nexus::v1;

}  // namespace

NexusTaskHandler::NexusTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                                   std::shared_ptr<log::Logger> logger, std::string task_queue)
    : grpc_(grpc),
      converter_(std::move(converter)),
      logger_(std::move(logger)),
      task_queue_(std::move(task_queue)) {}

void NexusTaskHandler::Register(std::string service, std::string operation,
                                worker::NexusOperationFn fn) {
  operations_.insert_or_assign(Key(service, operation), std::move(fn));
}

void NexusTaskHandler::Handle(const wsv::PollNexusTaskQueueResponse& task) {
  const std::string token = task.task_token();
  // This handler only serves start-operation requests (synchronous operations);
  // a cancel request has no registered handler path, so it is failed below.
  const nexus::Request& request = task.request();
  if (request.variant_case() != nexus::Request::kStartOperation) {
    wsv::RespondNexusTaskFailedRequest req;
    req.set_namespace_(grpc_->ns());
    req.set_identity(grpc_->identity());
    req.set_task_token(token);
    auto* err = req.mutable_error();
    err->set_error_type("NOT_IMPLEMENTED");
    err->mutable_failure()->set_message("only start-operation Nexus requests are supported");
    grpc_->RespondNexusTaskFailed(req);
    return;
  }

  const nexus::StartOperationRequest& start = request.start_operation();
  const std::string& service = start.service();
  const std::string& operation = start.operation();

  const auto it = operations_.find(Key(service, operation));
  if (it == operations_.end()) {
    wsv::RespondNexusTaskFailedRequest req;
    req.set_namespace_(grpc_->ns());
    req.set_identity(grpc_->identity());
    req.set_task_token(token);
    auto* err = req.mutable_error();
    err->set_error_type("NOT_FOUND");  // no handler for this (service, operation)
    err->mutable_failure()->set_message("no Nexus operation registered for service '" + service +
                                        "' operation '" + operation + "'");
    grpc_->RespondNexusTaskFailed(req);
    return;
  }

  // Nexus input/result are a SINGLE Payload each (not Payloads).
  const Payload input = FromProtoPayload(start.payload());
  try {
    const Payload result = it->second(*converter_, input);
    wsv::RespondNexusTaskCompletedRequest req;
    req.set_namespace_(grpc_->ns());
    req.set_identity(grpc_->identity());
    req.set_task_token(token);
    // Response -> StartOperationResponse.Sync -> a single result Payload.
    *req.mutable_response()
         ->mutable_start_operation()
         ->mutable_sync_success()
         ->mutable_payload() = ToProtoPayload(result);
    grpc_->RespondNexusTaskCompleted(req);
  } catch (const std::exception& e) {
    logger_->Warn("nexus operation failed", {log::F("service", service),
                                             log::F("operation", operation),
                                             log::F("error", e.what())});
    wsv::RespondNexusTaskFailedRequest req;
    req.set_namespace_(grpc_->ns());
    req.set_identity(grpc_->identity());
    req.set_task_token(token);
    auto* err = req.mutable_error();
    err->set_error_type("INTERNAL");  // handler raised -> internal handler error
    err->mutable_failure()->set_message(e.what());
    grpc_->RespondNexusTaskFailed(req);
  }
}

}  // namespace temporal::internal
