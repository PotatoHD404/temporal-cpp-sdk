#include "internal/activity_task_handler.h"

#include <exception>
#include <string>
#include <utility>

#include "internal/grpc_client.h"
#include "internal/proto_util.h"

#include <temporal/activity/activity.h>
#include <temporal/common/errors.h>

namespace temporal::internal {

ActivityTaskHandler::ActivityTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                                         std::shared_ptr<log::Logger> logger, std::string task_queue)
    : grpc_(grpc),
      converter_(std::move(converter)),
      logger_(std::move(logger)),
      task_queue_(std::move(task_queue)) {}

void ActivityTaskHandler::Register(std::string name, worker::ActivityFn fn) {
  activities_.insert_or_assign(std::move(name), std::move(fn));
}

void ActivityTaskHandler::Handle(const wsv::PollActivityTaskQueueResponse& task) {
  activity::ActivityInfo info;
  info.activity_id = task.activity_id();
  info.activity_type = task.activity_type().name();
  info.workflow_id = task.workflow_execution().workflow_id();
  info.run_id = task.workflow_execution().run_id();
  info.task_queue = task_queue_;
  info.attempt = task.attempt();

  auto heartbeat = [this, task_token = task.task_token()](const Payloads& details) -> bool {
    wsv::RecordActivityTaskHeartbeatRequest req;
    req.set_task_token(task_token);
    req.set_namespace_(grpc_->ns());
    req.set_identity(grpc_->identity());
    if (!details.empty()) {
      *req.mutable_details() = ToProtoPayloads(details);
    }
    return grpc_->RecordActivityTaskHeartbeat(req).cancel_requested();
  };
  activity::Context ctx(info, converter_.get(), heartbeat);

  const auto it = activities_.find(info.activity_type);
  if (it == activities_.end()) {
    wsv::RespondActivityTaskFailedRequest req;
    req.set_task_token(task.task_token());
    req.set_identity(grpc_->identity());
    *req.mutable_failure() = MakeApplicationFailure(
        "no activity registered for type: " + info.activity_type, "NotRegisteredError");
    grpc_->RespondActivityTaskFailed(req);
    return;
  }

  const Payloads input = FromProtoPayloads(task.input());
  try {
    const Payloads result = it->second(ctx, input);
    wsv::RespondActivityTaskCompletedRequest req;
    req.set_task_token(task.task_token());
    req.set_identity(grpc_->identity());
    if (!result.empty()) {
      *req.mutable_result() = ToProtoPayloads(result);
    }
    grpc_->RespondActivityTaskCompleted(req);
  } catch (const ApplicationError& e) {
    logger_->Warn("activity failed",
                  {log::F("activity", info.activity_type), log::F("error", e.what())});
    wsv::RespondActivityTaskFailedRequest req;
    req.set_task_token(task.task_token());
    req.set_identity(grpc_->identity());
    *req.mutable_failure() = MakeApplicationFailure(e.what(), e.type());
    grpc_->RespondActivityTaskFailed(req);
  } catch (const std::exception& e) {
    logger_->Error("activity raised an exception",
                   {log::F("activity", info.activity_type), log::F("error", e.what())});
    wsv::RespondActivityTaskFailedRequest req;
    req.set_task_token(task.task_token());
    req.set_identity(grpc_->identity());
    *req.mutable_failure() = MakeApplicationFailure(e.what(), "std::exception");
    grpc_->RespondActivityTaskFailed(req);
  }
}

}  // namespace temporal::internal
