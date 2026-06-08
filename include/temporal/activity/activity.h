#pragma once

#include <string>
#include <utility>

namespace temporal {

class DataConverter;

namespace activity {

// Metadata about the activity task currently executing.
struct ActivityInfo {
  std::string activity_id;
  std::string activity_type;
  std::string workflow_id;
  std::string run_id;
  std::string task_queue;
  int attempt = 1;
};

// The activity execution context. Unlike a workflow, an activity runs in real
// time with full access to I/O; this context exposes its metadata and (later)
// heartbeating. User activity functions take it by reference as their first
// parameter.
class Context {
 public:
  Context(ActivityInfo info, const DataConverter* converter)
      : info_(std::move(info)), converter_(converter) {}

  const ActivityInfo& GetInfo() const { return info_; }

  // Used by the worker's registration adapter; also available to user code.
  const DataConverter& data_converter() const { return *converter_; }

  // Heartbeating is a no-op in the current slice (see docs/ROADMAP.md).
  void RecordHeartbeat() {}

 private:
  ActivityInfo info_;
  const DataConverter* converter_;
};

}  // namespace activity
}  // namespace temporal
