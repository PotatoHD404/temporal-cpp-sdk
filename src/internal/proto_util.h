#pragma once

#include <chrono>
#include <string>

#include "google/protobuf/duration.pb.h"
#include "temporal/api/common/v1/message.pb.h"
#include "temporal/api/failure/v1/message.pb.h"

#include <temporal/common/options.h>
#include <temporal/common/payload.h>

namespace temporal::internal {

namespace tapi = ::temporal::api;
namespace gpb = ::google::protobuf;

// Conversions between the public Payload type and its protobuf form.
tapi::common::v1::Payload ToProtoPayload(const Payload& p);
Payload FromProtoPayload(const tapi::common::v1::Payload& p);
tapi::common::v1::Payloads ToProtoPayloads(const Payloads& ps);
Payloads FromProtoPayloads(const tapi::common::v1::Payloads& ps);

gpb::Duration ToProtoDuration(std::chrono::nanoseconds d);

tapi::common::v1::RetryPolicy ToProtoRetryPolicy(const RetryPolicy& policy);

// Build a Failure carrying an ApplicationFailureInfo (the shape activity/workflow
// application errors take on the wire). `non_retryable` marks the failure so the
// server stops retrying the activity (mirrors ApplicationError::non_retryable()).
tapi::failure::v1::Failure MakeApplicationFailure(const std::string& message, const std::string& type,
                                                  bool non_retryable = false);

// "<pid>@<hostname>", the default worker/client identity.
std::string DefaultIdentity();

// Random UUID-v4 string, used for workflow ids and request ids.
std::string NewUuid();

}  // namespace temporal::internal
