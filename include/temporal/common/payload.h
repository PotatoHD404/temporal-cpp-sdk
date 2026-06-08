#pragma once

#include <map>
#include <string>
#include <vector>

namespace temporal {

// A Temporal payload: opaque bytes plus string->bytes metadata. Mirrors
// `temporal.api.common.v1.Payload`. The `encoding` metadata key selects the
// PayloadConverter used to decode `data`.
struct Payload {
  std::map<std::string, std::string> metadata;  // values are raw bytes
  std::string data;                              // raw bytes
};

using Payloads = std::vector<Payload>;

namespace metadata_keys {
inline constexpr const char* kEncoding = "encoding";
}  // namespace metadata_keys

namespace encodings {
inline constexpr const char* kNull = "binary/null";
inline constexpr const char* kRaw = "binary/plain";
inline constexpr const char* kJson = "json/plain";
}  // namespace encodings

}  // namespace temporal
