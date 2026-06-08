#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>

namespace {

// A codec-enabled converter transforms the wire payload (base64) and round-trips.
TEST(PayloadCodec, Base64RoundTrip) {
  auto dc = temporal::DataConverter::WithCodecs({std::make_shared<temporal::Base64PayloadCodec>()});

  const temporal::Payload p = dc->ToPayload(std::string("hello world"));
  EXPECT_EQ(p.metadata.at("codec"), "base64");
  EXPECT_EQ(p.metadata.at("encoding"), "json/plain");  // inner encoding preserved
  EXPECT_NE(p.data, "\"hello world\"");                // data is base64, not raw json

  EXPECT_EQ(dc->FromPayload<std::string>(p), "hello world");
}

// The default converter applies no codec (the codec branch is opt-in / no-op).
TEST(PayloadCodec, DefaultConverterHasNoCodec) {
  auto dc = temporal::DataConverter::Default();
  const temporal::Payload p = dc->ToPayload(std::string("x"));
  EXPECT_EQ(p.metadata.count("codec"), 0U);
  EXPECT_EQ(dc->FromPayload<std::string>(p), "x");
}

}  // namespace
