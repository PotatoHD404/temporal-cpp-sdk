#pragma once

#include <stdexcept>
#include <string>
#include <utility>

namespace temporal {

// Base class for every error surfaced by the SDK.
class TemporalError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// Thrown by activity or workflow code to signal an application-level failure.
// `type` lets retry policies and catch sites discriminate failures by name.
class ApplicationError : public TemporalError {
 public:
  explicit ApplicationError(const std::string& message, std::string type = "",
                            bool non_retryable = false)
      : TemporalError(message), type_(std::move(type)), non_retryable_(non_retryable) {}

  const std::string& type() const noexcept { return type_; }
  bool non_retryable() const noexcept { return non_retryable_; }

 private:
  std::string type_;
  bool non_retryable_;
};

// Raised on the workflow side when an awaited activity ends in failure.
class ActivityError : public TemporalError {
 public:
  ActivityError(std::string type, const std::string& message)
      : TemporalError(message.empty() ? type : message), type_(std::move(type)) {}

  const std::string& type() const noexcept { return type_; }

 private:
  std::string type_;
};

// Raised on the client side when a workflow ends non-successfully
// (failed, timed out, terminated, or canceled).
class WorkflowFailedError : public TemporalError {
 public:
  using TemporalError::TemporalError;
};

// Raised when the data converter cannot encode/decode a value or payload.
class DataConverterError : public TemporalError {
 public:
  using TemporalError::TemporalError;
};

// Raised for transport/RPC failures talking to the Temporal service.
class RpcError : public TemporalError {
 public:
  using TemporalError::TemporalError;
};

}  // namespace temporal
