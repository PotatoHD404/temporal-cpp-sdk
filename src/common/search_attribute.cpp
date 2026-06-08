#include <temporal/common/search_attribute.h>

#include <nlohmann/json.hpp>

namespace temporal::sa {
namespace {

// Search attributes are json/plain Payloads tagged with their indexed type.
Payload Encode(const char* type, const nlohmann::json& value) {
  Payload p;
  p.metadata["encoding"] = "json/plain";
  p.metadata["type"] = type;
  p.data = value.dump();
  return p;
}

}  // namespace

Payload Keyword(std::string_view value) { return Encode("Keyword", std::string(value)); }
Payload Text(std::string_view value) { return Encode("Text", std::string(value)); }
Payload Int(std::int64_t value) { return Encode("Int", value); }
Payload Double(double value) { return Encode("Double", value); }
Payload Bool(bool value) { return Encode("Bool", value); }
Payload KeywordList(const std::vector<std::string>& values) { return Encode("KeywordList", values); }

}  // namespace temporal::sa
