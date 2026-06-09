#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "temporal/api/workflowservice/v1/request_response.pb.h"

#include <temporal/converter/data_converter.h>
#include <temporal/log/logger.h>
#include <temporal/worker/worker.h>

namespace temporal::interceptor {
class Interceptor;  // full type in <temporal/interceptor/interceptor.h> (included in the .cpp)
}  // namespace temporal::interceptor

namespace temporal::internal {

namespace wsv = ::temporal::api::workflowservice::v1;

class GrpcClient;

// Runs polled activity tasks. Activities execute in real time (no determinism
// constraints), so this is a straightforward decode -> invoke -> respond loop.
class ActivityTaskHandler {
 public:
  ActivityTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                      std::shared_ptr<log::Logger> logger, std::string task_queue);

  void Register(std::string name, worker::ActivityFn fn);
  bool has_activities() const { return !activities_.empty(); }
  // The registered function for `name`, or an empty std::function if none. Used by
  // the workflow handler to run local activities inline (no activity-task poll).
  worker::ActivityFn Lookup(const std::string& name) const {
    const auto it = activities_.find(name);
    return it == activities_.end() ? worker::ActivityFn{} : it->second;
  }

  void Handle(const wsv::PollActivityTaskQueueResponse& task);

  // Install activity-inbound interceptors (set by the worker from WorkerOptions).
  void SetInterceptors(std::vector<std::shared_ptr<interceptor::Interceptor>> interceptors) {
    interceptors_ = std::move(interceptors);
  }

 private:
  GrpcClient* grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string task_queue_;
  std::unordered_map<std::string, worker::ActivityFn> activities_;
  std::vector<std::shared_ptr<interceptor::Interceptor>> interceptors_;
};

}  // namespace temporal::internal
