#include <temporal/converter/data_converter.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// Protobuf runtime is included only here, never in the public header (see the
// forward declaration of google::protobuf::Message in data_converter.h).
#include "google/protobuf/message.h"
#include "google/protobuf/util/json_util.h"

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

Payload DataConverter::ToProtoPayload(const std::string& serialized,
                                      const std::string& message_type) const {
  Payload p;
  p.metadata[metadata_keys::kEncoding] = encodings::kProto;
  p.metadata[metadata_keys::kMessageType] = message_type;
  p.data = serialized;
  return p;
}

std::string DataConverter::ProtoBytes(const Payload& payload) const {
  const auto it = payload.metadata.find(std::string(metadata_keys::kEncoding));
  const std::string enc = it == payload.metadata.end() ? std::string() : it->second;
  if (enc != encodings::kProto) {
    throw DataConverterError("expected binary/protobuf payload, got encoding: " + enc);
  }
  return payload.data;
}

Payload DataConverter::ToProtoJsonPayload(const google::protobuf::Message& message,
                                          const std::string& message_type) const {
  std::string json;
  const auto status = google::protobuf::util::MessageToJsonString(message, &json);
  if (!status.ok()) {
    throw DataConverterError("failed to encode protobuf payload as json/protobuf: " +
                             std::string(status.message()));
  }
  Payload p;
  p.metadata[metadata_keys::kEncoding] = encodings::kProtoJson;
  p.metadata[metadata_keys::kMessageType] = message_type;
  p.data = std::move(json);
  return p;
}

void DataConverter::FromProtoPayload(const Payload& payload,
                                     google::protobuf::Message& out) const {
  const auto it = payload.metadata.find(std::string(metadata_keys::kEncoding));
  const std::string enc = it == payload.metadata.end() ? std::string() : it->second;
  if (enc == encodings::kProto) {
    if (!out.ParseFromString(payload.data)) {
      throw DataConverterError("failed to parse protobuf payload as " +
                               std::string(out.GetTypeName()));
    }
    return;
  }
  if (enc == encodings::kProtoJson) {
    const auto status = google::protobuf::util::JsonStringToMessage(payload.data, &out);
    if (!status.ok()) {
      throw DataConverterError("failed to parse json/protobuf payload as " +
                               std::string(out.GetTypeName()) + ": " +
                               std::string(status.message()));
    }
    return;
  }
  throw DataConverterError("expected a protobuf payload, got encoding: " + enc);
}

namespace {

constexpr const char* kB64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string Base64Encode(const std::string& in) {
  std::string out;
  out.reserve(((in.size() + 2) / 3) * 4);
  int val = 0;
  int bits = -6;
  for (const unsigned char c : in) {
    val = (val << 8) + c;
    bits += 8;
    while (bits >= 0) {
      out.push_back(kB64[(val >> bits) & 0x3F]);
      bits -= 6;
    }
  }
  if (bits > -6) {
    out.push_back(kB64[((val << 8) >> (bits + 8)) & 0x3F]);
  }
  while (out.size() % 4 != 0) {
    out.push_back('=');
  }
  return out;
}

std::string Base64Decode(const std::string& in) {
  std::vector<int> rev(256, -1);
  for (int i = 0; i < 64; ++i) {
    rev[static_cast<unsigned char>(kB64[i])] = i;
  }
  std::string out;
  int val = 0;
  int bits = -8;
  for (const unsigned char c : in) {
    if (rev[c] == -1) {
      break;  // padding '=' or non-alphabet
    }
    val = (val << 6) + rev[c];
    bits += 6;
    if (bits >= 0) {
      out.push_back(static_cast<char>((val >> bits) & 0xFF));
      bits -= 8;
    }
  }
  return out;
}

constexpr const char* kCodecKey = "codec";

}  // namespace

Payload Base64PayloadCodec::Encode(const Payload& payload) const {
  Payload out = payload;  // preserve all inner metadata (encoding, type, …)
  out.data = Base64Encode(payload.data);
  out.metadata[kCodecKey] = "base64";
  return out;
}

Payload Base64PayloadCodec::Decode(const Payload& payload) const {
  const auto it = payload.metadata.find(kCodecKey);
  if (it == payload.metadata.end() || it->second != "base64") {
    return payload;  // not encoded by this codec
  }
  Payload out = payload;
  out.data = Base64Decode(payload.data);
  out.metadata.erase(kCodecKey);
  return out;
}

Payload DataConverter::ApplyCodecsEncode(Payload payload) const {
  for (const auto& codec : codecs_) {
    payload = codec->Encode(payload);
  }
  return payload;
}

Payload DataConverter::ApplyCodecsDecode(Payload payload) const {
  for (auto it = codecs_.rbegin(); it != codecs_.rend(); ++it) {
    payload = (*it)->Decode(payload);
  }
  return payload;
}

std::shared_ptr<DataConverter> DataConverter::WithCodecs(
    std::vector<std::shared_ptr<PayloadCodec>> codecs) {
  auto dc = std::make_shared<DataConverter>();
  dc->codecs_ = std::move(codecs);
  return dc;
}

std::shared_ptr<DataConverter> DataConverter::WithProtoJson() {
  auto dc = std::make_shared<DataConverter>();
  dc->proto_json_ = true;
  return dc;
}

void DataConverter::SetProtoJson(bool enabled) { proto_json_ = enabled; }

void DataConverter::WithFailureConverter(std::shared_ptr<FailureConverter> failure_converter) {
  failure_converter_ = std::move(failure_converter);
}

namespace {

// Metadata key flagging a payload whose `data` is an external-store reference
// rather than the body itself (see InMemoryPayloadStorage).
constexpr const char* kRemoteRefKey = "remote-codec-ref";

}  // namespace

Payload InMemoryPayloadStorage::Store(const Payload& payload) const {
  // Key the blob by a stable hash of the body so identical bodies coalesce.
  const std::string key =
      std::to_string(std::hash<std::string>{}(payload.data)) + "-" +
      std::to_string(payload.data.size());
  blobs_[key] = payload.data;
  Payload out = payload;  // preserve inner metadata (encoding, message type, …)
  out.metadata[kRemoteRefKey] = key;
  out.data = key;  // body replaced by its reference
  return out;
}

Payload InMemoryPayloadStorage::Resolve(const Payload& payload) const {
  const auto it = payload.metadata.find(kRemoteRefKey);
  if (it == payload.metadata.end()) {
    return payload;  // not a reference produced by this store
  }
  const auto blob = blobs_.find(it->second);
  if (blob == blobs_.end()) {
    throw DataConverterError("payload storage reference not found: " + it->second);
  }
  Payload out = payload;
  out.data = blob->second;  // restore body
  out.metadata.erase(kRemoteRefKey);
  return out;
}

}  // namespace temporal
