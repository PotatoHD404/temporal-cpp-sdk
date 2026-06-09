#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <temporal/common/options.h>
#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>
#include <temporal/typed_handles.h>

namespace temporal {

namespace internal {
class GrpcClient;
}

namespace client {

// A snapshot of a workflow execution, returned by WorkflowHandle::Describe and
// Client::ListWorkflows.
struct WorkflowDescription {
  std::string workflow_id;
  std::string run_id;
  std::string workflow_type;
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

  // Typed-handle overload: result type + name come from the QueryRef.
  template <class R, class... Args>
  R Query(const QueryRef<R>& ref, const Args&... args) {
    return Query<R>(ref.name, args...);
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

  // Typed-handle overload: result type + name come from the UpdateRef.
  template <class R, class... Args>
  R Update(const UpdateRef<R>& ref, const Args&... args) {
    return Update<R>(ref.name, args...);
  }

  void Signal(std::string_view signal_name, const Payloads& args);

  // Encode + send a signal in one call, like StartWorkflow/Query/Update (so callers
  // don't hand-encode Payloads). The requires-clause keeps the pre-encoded
  // `Signal(name, Payloads)` overload above unambiguous for a single Payloads arg.
  template <class... Args>
    requires(!(sizeof...(Args) == 1 && (std::is_same_v<std::decay_t<Args>, Payloads> && ...)))
  void Signal(std::string_view signal_name, const Args&... args) {
    Signal(signal_name, converter_->ToPayloads(args...));
  }

  // Typed-handle overload: the payload type is checked against the SignalRef.
  template <class T>
  void Signal(const SignalRef<T>& ref, const T& value) {
    Signal(ref.name, value);
  }

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

// The assignment rules on a task queue plus the conflict token to pass to a
// subsequent UpdateWorkerVersioningRules call (rules-based worker versioning).
struct WorkerVersioningRules {
  std::vector<std::string> assignment_rule_target_build_ids;
  std::vector<std::pair<std::string, std::string>> redirect_rules;  // (source, target) build ids
  std::string conflict_token;  // bytes; opaque, echoed back on update
};

// The search attributes registered in a namespace, returned by
// Client::ListSearchAttributes. Each map is attribute name -> type string (one
// of "Keyword", "Text", "Int", "Double", "Bool", "Datetime", "KeywordList").
struct SearchAttributes {
  std::map<std::string, std::string> custom;  // user-registered attributes
  std::map<std::string, std::string> system;  // built-in attributes
};

// A point-in-time snapshot of a batch operation, returned by
// Client::DescribeBatchOperation.
struct BatchOperationDescription {
  std::string job_id;
  std::string state;  // e.g. "Running", "Completed", "Failed"
  std::int64_t total_operations = 0;
  std::int64_t completed_operations = 0;
  std::int64_t failed_operations = 0;
};

// Identifying information about the Temporal cluster the client is connected to,
// returned by Client::DescribeCluster.
struct ClusterDescription {
  std::string cluster_name;
  std::string cluster_id;
  std::string server_version;
  std::int64_t history_shard_count = 0;
  std::string persistence_store;   // e.g. "postgres", "cassandra", "sqlite"
  std::string visibility_store;    // e.g. "elasticsearch", "postgres"
};

// A registered Nexus endpoint, returned by Client::GetNexusEndpoint. Endpoints
// are created/listed/described here; workflows call operations on them via
// workflow::Context::ExecuteNexusOperation and a worker serves them via
// Worker::RegisterNexusOperation.
struct NexusEndpointDescription {
  std::string id;                 // server-assigned endpoint id (opaque)
  std::string name;               // unique endpoint name
  std::string target_namespace;   // worker target: namespace that handles ops
  std::string target_task_queue;  // worker target: task queue that handles ops
};

// A worker deployment's name and its current routing config, returned by
// Client::DescribeWorkerDeployment. (Worker Deployments are the modern
// versioning API; the build ids reflect RoutingConfig.current/ramping
// deployment version — empty when no version is current/ramping.)
struct WorkerDeploymentDescription {
  std::string name;                   // deployment name
  std::string current_version_build_id;  // build id of the current version (may be empty)
  std::string ramping_version_build_id;  // build id of the ramping version (may be empty)
  std::string conflict_token;  // bytes; opaque, pass to SetWorkerDeploymentCurrentVersion
};

// A client connection to the Temporal frontend service. Cheap to copy (shared
// gRPC channel). Mirrors the Go SDK's `client.Client`.
class Client {
 public:
  [[nodiscard]] static Client Connect(const ClientOptions& options = {});

  // Start a workflow by type name, encoding `args` through the data converter.
  template <class... Args>
  WorkflowHandle StartWorkflow(const StartWorkflowOptions& options, std::string_view workflow_type,
                               const Args&... args) {
    Payloads input = converter_->ToPayloads(args...);
    return StartWorkflowPayloads(options, workflow_type, input);
  }

  // Signal a workflow, starting it first if it isn't already running (atomic).
  // `signal_input` is the pre-encoded signal argument (use `dc->ToPayloads(v)`);
  // `workflow_args` are the workflow's start arguments.
  template <class... Args>
  WorkflowHandle SignalWithStartWorkflow(const StartWorkflowOptions& options,
                                         std::string_view workflow_type, std::string_view signal_name,
                                         const Payloads& signal_input, const Args&... workflow_args) {
    Payloads input = converter_->ToPayloads(workflow_args...);
    return SignalWithStartWorkflowPayloads(options, workflow_type, signal_name, signal_input, input);
  }

  WorkflowHandle GetHandle(std::string workflow_id, std::string run_id = "");

  // Visibility queries. `query` is a Temporal list filter (e.g.
  // "WorkflowType = 'Foo' AND ExecutionStatus = 'Running'"); empty matches all.
  // ListWorkflows pages through every match internally.
  std::vector<WorkflowDescription> ListWorkflows(const std::string& query = "");
  std::int64_t CountWorkflows(const std::string& query = "");

  // Create a schedule that starts a workflow on a fixed interval. (Minimal
  // surface: interval + start-workflow action — see ScheduleOptions.)
  void CreateSchedule(const std::string& schedule_id, const ScheduleOptions& options);
  // Returns whether the schedule exists; throws on errors other than not-found.
  bool DescribeSchedule(const std::string& schedule_id);
  void DeleteSchedule(const std::string& schedule_id);
  // Replace an existing schedule's spec/action.
  void UpdateSchedule(const std::string& schedule_id, const ScheduleOptions& options);
  // Run a scheduled action immediately, regardless of the spec.
  void TriggerSchedule(const std::string& schedule_id);
  void PauseSchedule(const std::string& schedule_id, const std::string& note = "");
  void UnpauseSchedule(const std::string& schedule_id, const std::string& note = "");
  // All schedule ids in the namespace (pages through results).
  std::vector<std::string> ListSchedules();

  // Reset a workflow back to a point in its history, replaying events after the
  // given WorkflowTaskFinishEventId onto a fresh run. `run_id` empty targets the
  // current run; returns the new run id. (`reason` is recorded on the new run.)
  std::string ResetWorkflow(const std::string& workflow_id, const std::string& run_id,
                            const std::string& reason, std::int64_t workflow_task_finish_event_id);

  // Worker Build-ID compatibility (task-queue versioning). Each inner vector is
  // one compatible set; the last build id in a set is its default.
  std::vector<std::vector<std::string>> GetWorkerBuildIdCompatibility(const std::string& task_queue);
  // Add `build_id` as a brand-new default set on `task_queue`.
  void UpdateWorkerBuildIdCompatibility(const std::string& task_queue, const std::string& build_id);
  // Promote the existing set that already contains `build_id` to be the default.
  void PromoteWorkerBuildIdSet(const std::string& task_queue, const std::string& build_id);

  // Rules-based worker versioning (distinct from the build-id-set API above).
  // Read the assignment + redirect rules currently configured on `task_queue`.
  WorkerVersioningRules GetWorkerVersioningRules(const std::string& task_queue);
  // Insert an assignment rule at the head of the list routing new executions to
  // `target_build_id`. (The server may require worker-versioning dynamic config.)
  void InsertWorkerAssignmentRule(const std::string& task_queue,
                                  const std::string& target_build_id);
  // Add a compatible-redirect rule routing `source_build_id` to `target_build_id`
  // (gradual build-id rollout).
  void AddWorkerRedirectRule(const std::string& task_queue, const std::string& source_build_id,
                             const std::string& target_build_id);

  // Batch operations: act on every workflow matched by a visibility `query`.
  // `job_id` identifies the batch for Describe/Stop; reuse it to poll progress.
  // Terminate every workflow matching `visibility_query`.
  void StartBatchTerminate(const std::string& job_id, const std::string& visibility_query,
                           const std::string& reason);
  // Request cancellation of every workflow matching `visibility_query`.
  void StartBatchCancel(const std::string& job_id, const std::string& visibility_query,
                        const std::string& reason);
  // Snapshot of a batch operation's state and progress counts.
  BatchOperationDescription DescribeBatchOperation(const std::string& job_id);
  // All batch operation job ids in the namespace (pages through results).
  std::vector<std::string> ListBatchOperations();

  // Custom search attributes (OperatorService). Register new attributes by name
  // and type; `type` is one of "Keyword", "Text", "Int", "Double", "Bool",
  // "Datetime", "KeywordList" (an unknown type throws std::invalid_argument
  // before the RPC). Registration is eventually consistent, so an attribute may
  // not appear in ListSearchAttributes immediately after AddSearchAttributes.
  void AddSearchAttributes(const std::map<std::string, std::string>& name_to_type);
  // The custom + system search attributes registered in the namespace.
  SearchAttributes ListSearchAttributes();
  // Remove custom search attributes by name (system attributes can't be removed).
  void RemoveSearchAttributes(const std::vector<std::string>& names);

  // Identifying information about the cluster this client is connected to
  // (server version, persistence/visibility stores, history shard count).
  ClusterDescription DescribeCluster();
  // The names of every cluster registered with this Temporal deployment
  // (OperatorService; pages through results). A single-cluster dev server
  // reports just the active cluster.
  std::vector<std::string> ListClusters();

  // Nexus endpoint management (OperatorService). Operation calls + the worker
  // handler live on workflow::Context::ExecuteNexusOperation and
  // Worker::RegisterNexusOperation; these RPCs just register/list/describe the
  // endpoints they target. The dev server may reject these RPCs unless Nexus is
  // enabled (start-dev --dynamic-config-value system.enableNexus=true), in which
  // case the call throws RpcError.
  //
  // Register a Nexus endpoint named `name` whose target is the worker polling
  // `target_task_queue` in this client's namespace; returns the new endpoint id.
  std::string CreateNexusEndpoint(const std::string& name,
                                  const std::string& target_task_queue);
  // Look up a single endpoint by its server-assigned id.
  NexusEndpointDescription GetNexusEndpoint(const std::string& id);
  // The names of every registered Nexus endpoint (pages through results).
  std::vector<std::string> ListNexusEndpoints();

  // Worker Deployments (the modern worker-versioning API; WorkflowService).
  // The dev server may require deployment dynamic config for the mutating call
  // (start-dev --dynamic-config-value system.enableDeploymentVersions=true).
  //
  // The names of every worker deployment in the namespace (pages through
  // results). Empty on a fresh server with no versioned workers.
  std::vector<std::string> ListWorkerDeployments();
  // Current routing config for a single deployment by name. Throws RpcError
  // (NOT_FOUND) if no deployment with that name exists.
  WorkerDeploymentDescription DescribeWorkerDeployment(const std::string& name);
  // Promote `build_id` to be the current version of `deployment_name`. The
  // server enforces optimistic concurrency: pass the `conflict_token` from a
  // prior DescribeWorkerDeployment (empty is accepted as "no expectation").
  // Returns the new conflict token. (Requires an existing deployment + the
  // deployment-versions dynamic config above; not exercised by a fresh server.)
  std::string SetWorkerDeploymentCurrentVersion(const std::string& deployment_name,
                                                const std::string& build_id,
                                                const std::string& conflict_token = "");

  // Cluster federation / namespace lifecycle (OperatorService, admin-scoped).
  // Register or update a remote Temporal cluster for multi-cluster replication,
  // reachable at `frontend_address` (host:port). Requires a real second cluster
  // and global-namespace config — not exercisable against a single dev server.
  void AddOrUpdateRemoteCluster(const std::string& frontend_address, bool enable_connection);
  // Deregister a previously added remote cluster by its cluster name. Also
  // requires a multi-cluster setup.
  void RemoveRemoteCluster(const std::string& cluster_name);
  // Delete a namespace (asynchronous, irreversible). Returns the server-side
  // name of the namespace marked for deletion (a unique suffix is appended).
  // Throws RpcError (NOT_FOUND) if the namespace does not exist.
  std::string DeleteNamespace(const std::string& namespace_name);

  // Complete or fail an activity that deferred completion via
  // activity::Context::defer_completion(), identified by its task token
  // (activity::Context::GetInfo().task_token).
  template <class... Args>
  void CompleteActivity(const std::string& task_token, const Args&... result) {
    CompleteActivityPayloads(task_token, converter_->ToPayloads(result...));
  }
  void FailActivity(const std::string& task_token, const std::string& message,
                    const std::string& type = "ApplicationError");

  // ---- Time-skipping test server (TestService) ------------------------------
  // These RPCs are served ONLY by the Temporal time-skipping test server
  // (`temporal-test-server`), not the dev server or production. The test server
  // starts with time skipping LOCKED (counter = 1); UnlockTimeSkipping lets it
  // fast-forward through pending timers, so a workflow that sleeps for "days"
  // completes in milliseconds. See testing::TestWorkflowEnvironment for the usual
  // wrapper that connects + unlocks for you. Calling these against a non-test
  // server throws RpcError (UNIMPLEMENTED).
  //
  // The current (possibly skipped-ahead) server clock.
  std::chrono::system_clock::time_point GetCurrentTime();
  // Block until the server clock advances by `duration` (fast-forwarded when time
  // skipping is unlocked).
  void Sleep(std::chrono::system_clock::duration duration);
  // Increment / decrement the time-skipping lock counter. Time skips only while
  // the counter is 0; an unbalanced UnlockTimeSkipping is a server-side error.
  void LockTimeSkipping();
  void UnlockTimeSkipping();

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
  WorkflowHandle SignalWithStartWorkflowPayloads(const StartWorkflowOptions& options,
                                                 std::string_view workflow_type,
                                                 std::string_view signal_name,
                                                 const Payloads& signal_input, const Payloads& input);
  void CompleteActivityPayloads(const std::string& task_token, const Payloads& result);

  std::shared_ptr<internal::GrpcClient> grpc_;
  std::shared_ptr<DataConverter> converter_;
  std::shared_ptr<log::Logger> logger_;
  std::string ns_;
  std::string identity_;
  std::vector<std::shared_ptr<interceptor::Interceptor>> interceptors_;  // client-outbound chain
};

}  // namespace client
}  // namespace temporal
