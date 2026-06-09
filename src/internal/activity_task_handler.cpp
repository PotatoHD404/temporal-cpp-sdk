#include "internal/activity_task_handler.h"

#include <chrono>
#include <exception>
#include <string>
#include <utility>

#include "internal/grpc_client.h"
#include "internal/proto_util.h"

#include <temporal/activity/activity.h>
#include <temporal/common/errors.h>
#include <temporal/converter/failure_converter.h>
#include <temporal/interceptor/interceptor.h>

namespace temporal::internal {
namespace {

// Terminal activity-inbound interceptor: runs the real registered activity fn.
// The interceptor chain wraps this; with no interceptors it is the head and the
// activity runs directly (no overhead).
class RootActivityInbound : public interceptor::ActivityInboundInterceptor {
 public:
  explicit RootActivityInbound(const worker::ActivityFn& fn) : fn_(fn) {}
  Payloads ExecuteActivity(activity::Context& ctx, interceptor::ExecuteActivityInput& in,
                           const interceptor::Header& /*header*/) override {
    return fn_(ctx, in.args);
  }

 private:
  const worker::ActivityFn& fn_;
};

// How often the activity may actually report a heartbeat to the server. We
// throttle to a fraction of the heartbeat timeout (matching the Go SDK's ~80%),
// leaving headroom so a report still lands before the timeout expires. When the
// task carries no heartbeat timeout, fall back to a sane fixed interval.
std::chrono::steady_clock::duration HeartbeatThrottleInterval(
    const wsv::PollActivityTaskQueueResponse& task) {
  constexpr auto kDefaultInterval = std::chrono::seconds(30);
  if (!task.has_heartbeat_timeout()) {
    return kDefaultInterval;
  }
  const auto& d = task.heartbeat_timeout();
  const auto timeout =
      std::chrono::seconds(d.seconds()) + std::chrono::nanoseconds(d.nanos());
  if (timeout <= std::chrono::nanoseconds::zero()) {
    return kDefaultInterval;
  }
  return std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout * 8 / 10);
}

}  // namespace

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
  info.task_token = task.task_token();
  info.attempt = task.attempt();
  for (const auto& [key, value] : task.header().fields()) {
    info.headers[key] = FromProtoPayload(value);
  }

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
  activity::Context ctx(info, converter_.get(), heartbeat, HeartbeatThrottleInterval(task));

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
    // Run the activity through the inbound interceptor chain (terminal = real fn).
    RootActivityInbound root(it->second);
    std::vector<interceptor::Interceptor*> factories;
    factories.reserve(interceptors_.size());
    for (const auto& i : interceptors_) {
      factories.push_back(i.get());
    }
    auto chain = interceptor::BuildActivityInboundChain(factories, &root);
    interceptor::ExecuteActivityInput in{input};
    interceptor::Header header(info.headers);  // inbound propagation context
    const Payloads result = chain.head()->ExecuteActivity(ctx, in, header);
    if (ctx.WillCompleteAsync()) {
      return;  // left open; completed out-of-band via Client::CompleteActivity/FailActivity
    }
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
    if (const auto& fc = converter_->failure_converter()) {
      fc->ErrorToFailure(e, *req.mutable_failure());  // custom failure encoding
    } else {
      // Carry non_retryable so the server stops retrying a non-retryable error.
      *req.mutable_failure() = MakeApplicationFailure(e.what(), e.type(), e.non_retryable());
    }
    grpc_->RespondActivityTaskFailed(req);
  } catch (const std::exception& e) {
    logger_->Error("activity raised an exception",
                   {log::F("activity", info.activity_type), log::F("error", e.what())});
    wsv::RespondActivityTaskFailedRequest req;
    req.set_task_token(task.task_token());
    req.set_identity(grpc_->identity());
    if (const auto& fc = converter_->failure_converter()) {
      fc->ErrorToFailure(e, *req.mutable_failure());  // custom failure encoding
    } else {
      *req.mutable_failure() = MakeApplicationFailure(e.what(), "std::exception");
    }
    grpc_->RespondActivityTaskFailed(req);
  }
}

}  // namespace temporal::internal
