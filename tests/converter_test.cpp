#include <string>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>

namespace {

using temporal::DataConverter;
using temporal::Payloads;

TEST(DataConverter, StringRoundTrip) {
  const auto dc = DataConverter::Default();
  const auto p = dc->ToPayload(std::string("hello"));
  EXPECT_EQ(p.metadata.at("encoding"), "json/plain");
  EXPECT_EQ(dc->FromPayload<std::string>(p), "hello");
}

TEST(DataConverter, IntRoundTrip) {
  const auto dc = DataConverter::Default();
  const auto p = dc->ToPayload(42);
  EXPECT_EQ(dc->FromPayload<int>(p), 42);
}

TEST(DataConverter, BoolRoundTrip) {
  const auto dc = DataConverter::Default();
  EXPECT_TRUE(dc->FromPayload<bool>(dc->ToPayload(true)));
  EXPECT_FALSE(dc->FromPayload<bool>(dc->ToPayload(false)));
}

TEST(DataConverter, NullUsesBinaryNullEncoding) {
  const auto dc = DataConverter::Default();
  const nlohmann::json null_value = nullptr;
  const auto p = dc->ToPayload(null_value);
  EXPECT_EQ(p.metadata.at("encoding"), "binary/null");
}

TEST(DataConverter, MultiplePayloadsPreserveOrderAndTypes) {
  const auto dc = DataConverter::Default();
  const Payloads ps = dc->ToPayloads(std::string("a"), 7, true);
  ASSERT_EQ(ps.size(), 3U);
  EXPECT_EQ(dc->FromPayload<std::string>(ps[0]), "a");
  EXPECT_EQ(dc->FromPayload<int>(ps[1]), 7);
  EXPECT_TRUE(dc->FromPayload<bool>(ps[2]));
}

}  // namespace
