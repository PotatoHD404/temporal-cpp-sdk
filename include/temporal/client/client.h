#pragma once

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <temporal/common/options.h>
#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>

namespace temporal {

namespace internal {
class GrpcClient;
}

namespace client {

// A snapshot of a workflow execution, returned by WorkflowHandle::Describe.
struct WorkflowDescription {
  std::string workflow_id;
  std::string run_id;
  std::string status;  // e.g. "Running", "Completed", "Failed", "Terminated"
  std::map<std::string, Payload> memo;
};

// Handle to a started (or looked-up) workflow execution.
class WorkflowHandle {
 public:
  WorkflowHandle(std::shared_ptr<internal::GrpcClient> grpc, std::shared_ptr<DataConverter> converter,
                 std::string ns, std::string workflow_id, std::string run_id);

  const std::string& id() const { return workflow_id_; }
  const std::string& run_id() const { return run_id_; }

  // Blocks (long-polling history) until the workflow closes, then decodes the
  // result. Throws WorkflowFailedError if it failed/timed out/was terminated.
  template <class R>
  R Result() {
    Payloads payloads = ResultPayloads();
    if constexpr (std::is_void_v<R>) {
      (void)payloads;
      return;
    } else {
      return converter_->FromPayload<R>(payloads.at(0));
    }
  }

  // Synchronously query the workflow, encoding `args` and decoding the result.
  template <class R, class... Args>
  R Query(std::string_view query_type, const Args&... args) {
    Payloads result = QueryPayloads(query_type, converter_->ToPayloads(args...));
    if constexpr (std::is_void_v<R>) {
      (void)result;
      return;
    } else {
      return converter_->FromPayload<R>(result.at(0));
    }
  }

  // Synchronously send an update and wait for its result (throws on failure).
  template <class R, class... Args>
  R Update(std::string_view update_name, const Args&... args) {
    Payloads result = UpdatePayloads(update_name, converter_->ToPayloads(args...));
    if constexpr (std::is_void_v<R>) {
      (void)result;
      return;
    } else {
      return converter_->FromPayload<R>(result.at(0));
    }
  }

  void Signal(std::string_view signal_name, const Payloads& args);
  void Cancel();
  void Terminate(std::string_view reason = "");

  // Fetch this workflow's full history as Temporal JSON (pages internally). Feed
  // it to Worker::ReplayWorkflowHistory to test a workflow against real history.
  std::string FetchHistoryJson();

  // Fetch a point-in-time snapshot (status + memo) of this workflow execution.
  WorkflowDescription Describe();

 private:
  Payloads ResultPayloads();  // non-template; defined in client.cpp
  Payloads QueryPayloads(std::string_view query_type, const Payloads& args);
  Payloads UpdatePayloads(std::string_view update_name, const Payloads& args);

  std::shared_ptr<internal::GrpcClient> grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::string ns_;
  std::string workflow_id_;
  std::string run_id_;
};

// A client connection to the Temporal frontend service. Cheap to copy (shared
// gRPC channel). Mirrors the Go SDK's `client.Client`.
class Client {
 public:
  static Client Connect(const ClientOptions& options = {});

  // Start a workflow by type name, encoding `args` through the data converter.
  template <class... Args>
  WorkflowHandle StartWorkflow(const StartWorkflowOptions& options, std::string_view workflow_type,
                               const Args&... args) {
    Payloads input = converter_->ToPayloads(args...);
    return StartWorkflowPayloads(options, workflow_type, input);
  }

  WorkflowHandle GetHandle(std::string workflow_id, std::string run_id = "");

  // Accessors used by Worker.
  const std::shared_ptr<internal::GrpcClient>& grpc() const { return grpc_; }
  const std::shared_ptr<DataConverter>& data_converter() const { return converter_; }
  const std::shared_ptr<log::Logger>& logger() const { return logger_; }
  const std::string& ns() const { return ns_; }
  const std::string& identity() const { return identity_; }

 private:
  Client() = default;
  WorkflowHandle StartWorkflowPayloads(const StartWorkflowOptions& options,
                                       std::string_view workflow_type, const Payloads& input);

  std::shared_ptr<internal::GrpcClient> grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string ns_;
  std::string identity_;
};

}  // namespace client
}  // namespace temporal
