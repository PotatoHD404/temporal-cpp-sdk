#include <exception>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <temporal/common/errors.h>
#include <temporal/converter/data_converter.h>
#include <temporal/converter/failure_converter.h>

#include "temporal/api/common/v1/message.pb.h"
#include "temporal/api/failure/v1/message.pb.h"

namespace {

using temporal::ApplicationError;
using temporal::DataConverter;
using temporal::DefaultFailureConverter;
using temporal::InMemoryPayloadStorage;
using temporal::Payload;

using ProtoMsg = temporal::api::common::v1::WorkflowExecution;

ProtoMsg MakeExec() {
  ProtoMsg exec;
  exec.set_workflow_id("wf-123");
  exec.set_run_id("run-456");
  return exec;
}

// --- Feature 1: ProtoJSON ----------------------------------------------------

// POSITIVE: a proto message round-trips through the json/protobuf encoding.
TEST(ProtoJson, RoundTrip) {
  auto dc = DataConverter::WithProtoJson();
  EXPECT_TRUE(dc->proto_json());

  const Payload p = dc->ToPayload(MakeExec());
  EXPECT_EQ(p.metadata.at("encoding"), "json/protobuf");
  EXPECT_EQ(p.metadata.at("messageType"),
            "temporal.api.common.v1.WorkflowExecution");
  // Body is JSON text, not protobuf wire bytes.
  EXPECT_NE(p.data.find("workflowId"), std::string::npos);

  const auto decoded = dc->FromPayload<ProtoMsg>(p);
  EXPECT_EQ(decoded.workflow_id(), "wf-123");
  EXPECT_EQ(decoded.run_id(), "run-456");
}

// POSITIVE: the default converter still emits binary protobuf (no behavior change).
TEST(ProtoJson, DefaultStaysBinary) {
  auto dc = DataConverter::Default();
  EXPECT_FALSE(dc->proto_json());
  const Payload p = dc->ToPayload(MakeExec());
  EXPECT_EQ(p.metadata.at("encoding"), "binary/protobuf");
}

// POSITIVE: SetProtoJson toggles encode on an existing instance.
TEST(ProtoJson, SetToggle) {
  DataConverter dc;
  dc.SetProtoJson(true);
  const Payload p = dc.ToPayload(MakeExec());
  EXPECT_EQ(p.metadata.at("encoding"), "json/protobuf");
}

// CROSS-ENCODING: encode binary, decode with a proto-json converter.
TEST(ProtoJson, DecodeBinaryWithProtoJsonConverter) {
  const Payload binary = DataConverter::Default()->ToPayload(MakeExec());
  EXPECT_EQ(binary.metadata.at("encoding"), "binary/protobuf");

  auto pj = DataConverter::WithProtoJson();
  const auto decoded = pj->FromPayload<ProtoMsg>(binary);  // reads binary anyway
  EXPECT_EQ(decoded.workflow_id(), "wf-123");
}

// CROSS-ENCODING: encode json, decode with the default (binary) converter.
TEST(ProtoJson, DecodeJsonWithDefaultConverter) {
  const Payload json = DataConverter::WithProtoJson()->ToPayload(MakeExec());
  EXPECT_EQ(json.metadata.at("encoding"), "json/protobuf");

  const auto decoded = DataConverter::Default()->FromPayload<ProtoMsg>(json);
  EXPECT_EQ(decoded.run_id(), "run-456");
}

// NEGATIVE: malformed json under json/protobuf -> clean throw, no crash.
TEST(ProtoJson, MalformedJsonThrows) {
  Payload bad;
  bad.metadata["encoding"] = "json/protobuf";
  bad.metadata["messageType"] = "temporal.api.common.v1.WorkflowExecution";
  bad.data = "{ this is not valid json ";
  EXPECT_THROW(DataConverter::Default()->FromPayload<ProtoMsg>(bad),
               std::exception);
}

// NEGATIVE: a non-proto encoding decoded as a proto type -> clean throw.
TEST(ProtoJson, UnknownEncodingThrows) {
  const Payload jsonStr = DataConverter::Default()->ToPayload(std::string("x"));
  EXPECT_EQ(jsonStr.metadata.at("encoding"), "json/plain");
  EXPECT_THROW(DataConverter::Default()->FromPayload<ProtoMsg>(jsonStr),
               std::exception);
}

// --- Feature 2: Failure converter -------------------------------------------

// POSITIVE: a retryable ApplicationError round-trips losslessly.
TEST(FailureConverter, RetryableApplicationErrorRoundTrip) {
  DefaultFailureConverter fc;
  temporal::api::failure::v1::Failure f;
  fc.ErrorToFailure(ApplicationError("boom", "MyErr", /*non_retryable=*/false), f);

  EXPECT_EQ(f.message(), "boom");
  ASSERT_TRUE(f.has_application_failure_info());
  EXPECT_EQ(f.application_failure_info().type(), "MyErr");
  EXPECT_FALSE(f.application_failure_info().non_retryable());

  const std::exception_ptr ep = fc.FailureToError(f);
  ASSERT_TRUE(ep != nullptr);
  try {
    std::rethrow_exception(ep);
    FAIL() << "expected ApplicationError";
  } catch (const ApplicationError& e) {
    EXPECT_STREQ(e.what(), "boom");
    EXPECT_EQ(e.type(), "MyErr");
    EXPECT_FALSE(e.non_retryable());
  }
}

// POSITIVE: a non-retryable ApplicationError preserves the flag.
TEST(FailureConverter, NonRetryableApplicationErrorRoundTrip) {
  DefaultFailureConverter fc;
  temporal::api::failure::v1::Failure f;
  fc.ErrorToFailure(ApplicationError("fatal", "Fatal", /*non_retryable=*/true), f);
  ASSERT_TRUE(f.application_failure_info().non_retryable());

  try {
    std::rethrow_exception(fc.FailureToError(f));
    FAIL();
  } catch (const ApplicationError& e) {
    EXPECT_EQ(e.type(), "Fatal");
    EXPECT_TRUE(e.non_retryable());
  }
}

// POSITIVE: a generic std::exception becomes a message-only application failure.
TEST(FailureConverter, GenericExceptionBestEffort) {
  DefaultFailureConverter fc;
  temporal::api::failure::v1::Failure f;
  fc.ErrorToFailure(std::runtime_error("oops"), f);
  EXPECT_EQ(f.message(), "oops");
  EXPECT_TRUE(f.has_application_failure_info());

  try {
    std::rethrow_exception(fc.FailureToError(f));
    FAIL();
  } catch (const ApplicationError& e) {
    EXPECT_STREQ(e.what(), "oops");
    EXPECT_TRUE(e.type().empty());
  }
}

// POSITIVE: the hook stores/returns a failure converter on DataConverter.
TEST(FailureConverter, DataConverterHook) {
  DataConverter dc;
  EXPECT_EQ(dc.failure_converter(), nullptr);
  dc.WithFailureConverter(std::make_shared<DefaultFailureConverter>());
  EXPECT_NE(dc.failure_converter(), nullptr);
}

// NEGATIVE: an empty Failure (no failure_info) still yields a usable error.
TEST(FailureConverter, EmptyFailureBestEffort) {
  DefaultFailureConverter fc;
  temporal::api::failure::v1::Failure f;
  f.set_message("just a message");
  const std::exception_ptr ep = fc.FailureToError(f);
  ASSERT_TRUE(ep != nullptr);
  try {
    std::rethrow_exception(ep);
    FAIL();
  } catch (const ApplicationError& e) {
    EXPECT_STREQ(e.what(), "just a message");
  }
}

// --- Feature 3: Payload storage ---------------------------------------------

// POSITIVE: store -> reference -> resolve preserves bytes and inner metadata.
TEST(PayloadStorage, StoreReferenceResolve) {
  InMemoryPayloadStorage store;
  const Payload original = DataConverter::Default()->ToPayload(std::string("big-body"));

  const Payload ref = store.Store(original);
  EXPECT_EQ(ref.metadata.at("remote-codec-ref"), ref.data);  // body replaced by key
  EXPECT_NE(ref.data, original.data);
  EXPECT_EQ(ref.metadata.at("encoding"), "json/plain");      // inner metadata kept

  const Payload resolved = store.Resolve(ref);
  EXPECT_EQ(resolved.data, original.data);
  EXPECT_EQ(resolved.metadata.count("remote-codec-ref"), 0U);
  EXPECT_EQ(DataConverter::Default()->FromPayload<std::string>(resolved), "big-body");
}

// POSITIVE: Resolve on a non-reference payload is a pass-through.
TEST(PayloadStorage, ResolvePassThrough) {
  InMemoryPayloadStorage store;
  Payload plain;
  plain.metadata["encoding"] = "json/plain";
  plain.data = "\"x\"";
  EXPECT_EQ(store.Resolve(plain).data, "\"x\"");
}

// NEGATIVE: a dangling reference (unknown key, e.g. resolved by a different
// store instance) throws cleanly instead of crashing.
TEST(PayloadStorage, DanglingReferenceThrows) {
  InMemoryPayloadStorage a;
  InMemoryPayloadStorage b;  // separate backing table
  const Payload ref = a.Store(DataConverter::Default()->ToPayload(std::string("y")));
  EXPECT_THROW(b.Resolve(ref), std::exception);
}

}  // namespace
