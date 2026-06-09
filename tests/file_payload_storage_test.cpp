#include <filesystem>
#include <random>
#include <string>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>

namespace {

using temporal::DataConverter;
using temporal::FilePayloadStorage;
using temporal::Payload;

TEST(FilePayloadStorage, RoundTripAndDangling) {
  namespace fs = std::filesystem;
  const fs::path tmp =
      fs::temp_directory_path() / ("tmpl-fps-" + std::to_string(std::random_device{}()));
  fs::remove_all(tmp);  // start clean

  FilePayloadStorage store(tmp.string());
  const Payload original = DataConverter::Default()->ToPayload(std::string("big-body"));

  // Store: data becomes the key, the blob file exists, inner metadata is kept.
  const Payload ref = store.Store(original);
  EXPECT_EQ(ref.metadata.at("remote-codec-ref"), ref.data);
  EXPECT_NE(ref.data, original.data);
  EXPECT_EQ(ref.metadata.at("encoding"), "json/plain");
  EXPECT_TRUE(fs::exists(tmp / ref.data));

  // Resolve: bytes restored, marker stripped, decodes back to the value.
  const Payload resolved = store.Resolve(ref);
  EXPECT_EQ(resolved.data, original.data);
  EXPECT_EQ(resolved.metadata.count("remote-codec-ref"), 0U);
  EXPECT_EQ(DataConverter::Default()->FromPayload<std::string>(resolved), "big-body");

  // Pass-through: a non-reference payload is returned unchanged.
  Payload plain;
  plain.metadata["encoding"] = "json/plain";
  plain.data = "\"x\"";
  EXPECT_EQ(store.Resolve(plain).data, "\"x\"");

  // Dangling: delete the blob, Resolve must throw cleanly.
  fs::remove(tmp / ref.data);
  EXPECT_THROW(store.Resolve(ref), std::exception);

  fs::remove_all(tmp);  // clean up the temp dir
}

}  // namespace
