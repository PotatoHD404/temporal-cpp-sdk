#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <temporal/common/payload.h>

namespace temporal::sa {

// Typed search-attribute values, encoded as the `json/plain` Payloads the
// server's visibility store expects — each carries the indexed `type` metadata
// the server validates against the attribute's registered type. Use these to
// populate StartWorkflowOptions::search_attributes or Context::UpsertSearchAttributes.
//
// The named attribute must already be registered on the namespace
// (`temporal operator search-attribute create --name X --type Keyword`).
Payload Keyword(std::string_view value);
Payload Text(std::string_view value);
Payload Int(std::int64_t value);
Payload Double(double value);
Payload Bool(bool value);
Payload KeywordList(const std::vector<std::string>& values);

}  // namespace temporal::sa
