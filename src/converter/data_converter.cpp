#include <temporal/converter/data_converter.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace temporal {
namespace {

class NilPayloadConverter final : public PayloadConverter {
 public:
  std::string encoding() const override { return std::string(encodings::kNull); }

  std::optional<Payload> ToPayload(const nlohmann::json& value) const override {
    if (!value.is_null()) {
      return std::nullopt;
    }
    Payload p;
    p.metadata[metadata_keys::kEncoding] = std::string(encodings::kNull);
    return p;
  }

  bool FromPayload(const Payload& /*p*/, nlohmann::json& out) const override {
    out = nullptr;
    return true;
  }
};

class BytesPayloadConverter final : public PayloadConverter {
 public:
  std::string encoding() const override { return std::string(encodings::kRaw); }

  std::optional<Payload> ToPayload(const nlohmann::json& value) const override {
    if (!value.is_binary()) {
      return std::nullopt;
    }
    Payload p;
    p.metadata[metadata_keys::kEncoding] = std::string(encodings::kRaw);
    const auto& bin = value.get_binary();
    p.data.assign(bin.begin(), bin.end());
    return p;
  }

  bool FromPayload(const Payload& p, nlohmann::json& out) const override {
    out = nlohmann::json::binary(
        std::vector<std::uint8_t>(p.data.begin(), p.data.end()));
    return true;
  }
};

class JsonPayloadConverter final : public PayloadConverter {
 public:
  std::string encoding() const override { return std::string(encodings::kJson); }

  std::optional<Payload> ToPayload(const nlohmann::json& value) const override {
    Payload p;
    p.metadata[metadata_keys::kEncoding] = std::string(encodings::kJson);
    p.data = value.dump();
    return p;
  }

  bool FromPayload(const Payload& p, nlohmann::json& out) const override {
    out = nlohmann::json::parse(p.data);
    return true;
  }
};

std::vector<std::shared_ptr<PayloadConverter>> DefaultConverters() {
  return {std::make_shared<NilPayloadConverter>(),
          std::make_shared<BytesPayloadConverter>(),
          std::make_shared<JsonPayloadConverter>()};
}

}  // namespace

DataConverter::DataConverter() : converters_(DefaultConverters()) {}

DataConverter::DataConverter(std::vector<std::shared_ptr<PayloadConverter>> converters)
    : converters_(std::move(converters)) {}

std::shared_ptr<DataConverter> DataConverter::Default() {
  static const std::shared_ptr<DataConverter> dc = std::make_shared<DataConverter>();
  return dc;
}

Payload DataConverter::ToPayloadJson(const nlohmann::json& value) const {
  for (const auto& c : converters_) {
    if (auto p = c->ToPayload(value)) {
      return *p;
    }
  }
  throw DataConverterError("no payload converter could encode the value");
}

nlohmann::json DataConverter::FromPayloadJson(const Payload& payload) const {
  const auto it = payload.metadata.find(std::string(metadata_keys::kEncoding));
  const std::string enc = it == payload.metadata.end() ? std::string() : it->second;
  for (const auto& c : converters_) {
    if (c->encoding() == enc) {
      nlohmann::json out;
      if (c->FromPayload(payload, out)) {
        return out;
      }
    }
  }
  throw DataConverterError("no payload converter for encoding: " + enc);
}

}  // namespace temporal
