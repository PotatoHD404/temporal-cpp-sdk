#pragma once

#include <exception>
#include <memory>

// Forward-declared so this public header never has to include the protobuf
// runtime — the proto-touching implementation lives in failure_converter.cpp.
// Mirrors the data converter's "keep protobuf out of the public header" rule.
namespace temporal::api::failure::v1 {
class Failure;
}  // namespace temporal::api::failure::v1

namespace temporal {

// Translates between the SDK's C++ error types and Temporal's wire failure proto
// (`temporal.api.failure.v1.Failure`). Pluggable so deployments can customise how
// errors are encoded (e.g. to carry application-specific detail or redact). Both
// peers must agree on the conversion for cross-language compatibility. Mirrors the
// Go SDK's `temporal.FailureConverter`.
class FailureConverter {
 public:
  FailureConverter() = default;
  virtual ~FailureConverter() = default;
  FailureConverter(const FailureConverter&) = delete;
  FailureConverter& operator=(const FailureConverter&) = delete;
  FailureConverter(FailureConverter&&) = delete;
  FailureConverter& operator=(FailureConverter&&) = delete;

  // Populate `out` from a C++ error. Implementations should recognise the SDK's
  // own error types (e.g. ApplicationError) and fall back to a message-only
  // failure for any other std::exception.
  virtual void ErrorToFailure(const std::exception& error,
                              ::temporal::api::failure::v1::Failure& out) const = 0;

  // Reconstruct a C++ error from a Failure proto. Returned as an exception_ptr so
  // the caller can rethrow it as its concrete type; never null.
  virtual std::exception_ptr FailureToError(
      const ::temporal::api::failure::v1::Failure& failure) const = 0;
};

// Default conversion: round-trips ApplicationError losslessly via
// application_failure_info (type, non_retryable, message, stack trace) and maps
// the remaining Failure variants (timeout/canceled/terminated/server/…) to an
// ApplicationError best-effort, preserving message, source and stack trace.
class DefaultFailureConverter : public FailureConverter {
 public:
  void ErrorToFailure(const std::exception& error,
                      ::temporal::api::failure::v1::Failure& out) const override;
  std::exception_ptr FailureToError(
      const ::temporal::api::failure::v1::Failure& failure) const override;
};

// Process-wide default failure converter.
std::shared_ptr<FailureConverter> DefaultFailureConverterInstance();

}  // namespace temporal
