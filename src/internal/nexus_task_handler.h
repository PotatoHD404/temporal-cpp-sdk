#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "temporal/api/workflowservice/v1/request_response.pb.h"

#include <temporal/converter/data_converter.h>
#include <temporal/log/logger.h>
#include <temporal/worker/worker.h>

namespace temporal::internal {

namespace wsv = ::temporal::api::workflowservice::v1;

class GrpcClient;

// Runs polled Nexus tasks. A Nexus operation handler is a synchronous
// single-Payload-in / single-Payload-out function (the operation completes inline,
// like a sync Nexus operation). Handlers are keyed by (service, operation); the
// poll/dispatch is a straightforward decode -> invoke -> respond loop, with no
// determinism constraints (it runs in real time, off a worker thread).
class NexusTaskHandler {
 public:
  NexusTaskHandler(GrpcClient* grpc, std::shared_ptr<DataConverter> converter,
                   std::shared_ptr<log::Logger> logger, std::string task_queue);

  // Register a handler for (service, operation). Re-registering replaces it.
  void Register(std::string service, std::string operation, worker::NexusOperationFn fn);
  bool has_operations() const { return !operations_.empty(); }

  void Handle(const wsv::PollNexusTaskQueueResponse& task);

 private:
  // Composite key for the operation registry: "<service>\0<operation>" so two
  // distinct (service, operation) pairs never collide on concatenation.
  static std::string Key(const std::string& service, const std::string& operation) {
    return service + '\0' + operation;
  }

  GrpcClient* grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string task_queue_;
  std::unordered_map<std::string, worker::NexusOperationFn> operations_;
};

}  // namespace temporal::internal
