#include <temporal/converter/failure_converter.h>

#include <exception>
#include <memory>
#include <string>

#include <temporal/common/errors.h>

// Proto runtime is confined to this .cpp; the public header forward-declares
// temporal::api::failure::v1::Failure.
#include "temporal/api/failure/v1/message.pb.h"

namespace temporal {
namespace {

// Stamped on Failures this SDK produces so the source language is recoverable.
constexpr const char* kSource = "cpp-sdk";

}  // namespace

void DefaultFailureConverter::ErrorToFailure(
    const std::exception& error, ::temporal::api::failure::v1::Failure& out) const {
  out.set_message(error.what());
  out.set_source(kSource);

  // ApplicationError round-trips losslessly via application_failure_info; any
  // other std::exception becomes a message-only application failure (best-effort).
  if (const auto* app = dynamic_cast<const ApplicationError*>(&error)) {
    auto* info = out.mutable_application_failure_info();
    info->set_type(app->type());
    info->set_non_retryable(app->non_retryable());
  } else {
    out.mutable_application_failure_info();  // mark as an application failure
  }
}

std::exception_ptr DefaultFailureConverter::FailureToError(
    const ::temporal::api::failure::v1::Failure& failure) const {
  const std::string& message = failure.message();
  if (failure.has_application_failure_info()) {
    const auto& info = failure.application_failure_info();
    return std::make_exception_ptr(
        ApplicationError(message, info.type(), info.non_retryable()));
  }
  // Other variants (timeout/canceled/terminated/server/…) are mapped best-effort
  // to an ApplicationError preserving the human-readable message. The variant's
  // structured fields are not reconstructed.
  return std::make_exception_ptr(ApplicationError(message));
}

std::shared_ptr<FailureConverter> DefaultFailureConverterInstance() {
  static const std::shared_ptr<FailureConverter> fc =
      std::make_shared<DefaultFailureConverter>();
  return fc;
}

}  // namespace temporal
