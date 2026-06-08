#pragma once

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <temporal/common/errors.h>
#include <temporal/common/payload.h>

namespace temporal {

// Converts a single value to/from a Payload for one encoding. The default stack
// chains a few of these; the first that accepts a value wins on encode, and the
// `encoding` metadata picks the converter on decode. Mirrors the Go SDK's
// `converter.PayloadConverter`.
class PayloadConverter {
 public:
  PayloadConverter() = default;
  virtual ~PayloadConverter() = default;
  PayloadConverter(const PayloadConverter&) = delete;
  PayloadConverter& operator=(const PayloadConverter&) = delete;
  PayloadConverter(PayloadConverter&&) = delete;
  PayloadConverter& operator=(PayloadConverter&&) = delete;

  // The value of the `encoding` metadata key this converter owns.
  virtual std::string encoding() const = 0;

  // Returns a Payload if this converter handles `value`, else std::nullopt.
  virtual std::optional<Payload> ToPayload(const nlohmann::json& value) const = 0;

  // Decodes `p` (already matched by encoding) into `out`; returns false on failure.
  virtual bool FromPayload(const Payload& p, nlohmann::json& out) const = 0;
};

// Transforms an already-converted payload on its way to/from the server — e.g.
// compression or encryption "at rest". Applied after the PayloadConverter on
// encode, and before it on decode. Mirrors the Go SDK's converter.PayloadCodec.
// Both peers must share the same codec chain.
class PayloadCodec {
 public:
  PayloadCodec() = default;
  virtual ~PayloadCodec() = default;
  PayloadCodec(const PayloadCodec&) = delete;
  PayloadCodec& operator=(const PayloadCodec&) = delete;
  PayloadCodec(PayloadCodec&&) = delete;
  PayloadCodec& operator=(PayloadCodec&&) = delete;

  virtual Payload Encode(const Payload& payload) const = 0;
  virtual Payload Decode(const Payload& payload) const = 0;
};

// Reference codec: base64-encodes the payload bytes (a stand-in demonstrating the
// extension point — real deployments plug in compression/encryption here).
class Base64PayloadCodec : public PayloadCodec {
 public:
  Payload Encode(const Payload& payload) const override;
  Payload Decode(const Payload& payload) const override;
};

namespace detail {
// Detects a protobuf-generated message via its own member functions, so this
// header never has to include the protobuf runtime. Proto values then encode as
// `binary/protobuf` instead of going through the JSON stack.
template <class T, class = void>
struct is_proto_message : std::false_type {};
template <class T>
struct is_proto_message<
    T, std::void_t<decltype(std::declval<const T&>().SerializeAsString()),
                   decltype(std::declval<const T&>().GetTypeName()),
                   decltype(std::declval<T&>().ParseFromString(std::declval<std::string>()))>>
    : std::true_type {};
}  // namespace detail

// Ordered set of PayloadConverters. Equivalent to the Go SDK's default composite
// converter: Nil, ByteSlice, JSON. Values cross the public API as any type that
// nlohmann::json can (de)serialize; protobuf messages are encoded as binary protobuf.
class DataConverter {
 public:
  DataConverter();  // default stack
  explicit DataConverter(std::vector<std::shared_ptr<PayloadConverter>> converters);

  // Shared process-wide default instance.
  static std::shared_ptr<DataConverter> Default();

  // The default converter stack wrapped in a codec chain (applied to every
  // payload). Set the result on ClientOptions::data_converter.
  static std::shared_ptr<DataConverter> WithCodecs(std::vector<std::shared_ptr<PayloadCodec>> codecs);

  Payload ToPayloadJson(const nlohmann::json& value) const;
  nlohmann::json FromPayloadJson(const Payload& payload) const;

  // Binary-protobuf payloads (used by the proto branch of ToPayload/FromPayload).
  // ProtoBytes returns the wrapped message bytes; throws if the encoding mismatches.
  Payload ToProtoPayload(const std::string& serialized, const std::string& message_type) const;
  std::string ProtoBytes(const Payload& payload) const;

  template <class T>
  Payload ToPayload(const T& value) const {
    Payload p;
    if constexpr (detail::is_proto_message<T>::value) {
      p = ToProtoPayload(value.SerializeAsString(), std::string(value.GetTypeName()));
    } else if constexpr (std::is_same_v<std::decay_t<T>, nlohmann::json>) {
      p = ToPayloadJson(value);
    } else {
      p = ToPayloadJson(nlohmann::json(value));
    }
    return ApplyCodecsEncode(std::move(p));
  }

  template <class T>
  T FromPayload(const Payload& payload) const {
    const Payload decoded = ApplyCodecsDecode(payload);
    if constexpr (detail::is_proto_message<T>::value) {
      T msg;
      if (!msg.ParseFromString(ProtoBytes(decoded))) {
        throw DataConverterError("failed to parse protobuf payload as " +
                                 std::string(msg.GetTypeName()));
      }
      return msg;
    } else {
      nlohmann::json j = FromPayloadJson(decoded);
      if constexpr (std::is_same_v<T, nlohmann::json>) {
        return j;
      } else {
        return j.get<T>();
      }
    }
  }

  template <class... Args>
  Payloads ToPayloads(const Args&... args) const {
    Payloads out;
    out.reserve(sizeof...(Args));
    (out.push_back(ToPayload(args)), ...);
    return out;
  }

 private:
  // Apply the codec chain in order on encode, reverse on decode (no-op if empty).
  Payload ApplyCodecsEncode(Payload payload) const;
  Payload ApplyCodecsDecode(Payload payload) const;

  std::vector<std::shared_ptr<PayloadConverter>> converters_;
  std::vector<std::shared_ptr<PayloadCodec>> codecs_;
};

}  // namespace temporal
