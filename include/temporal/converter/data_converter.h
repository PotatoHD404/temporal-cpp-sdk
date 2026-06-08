#pragma once

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
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

// Ordered set of PayloadConverters. Equivalent to the Go SDK's default composite
// converter: Nil, ByteSlice, JSON. Values cross the public API as any type that
// nlohmann::json can (de)serialize.
class DataConverter {
 public:
  DataConverter();  // default stack
  explicit DataConverter(std::vector<std::shared_ptr<PayloadConverter>> converters);

  // Shared process-wide default instance.
  static std::shared_ptr<DataConverter> Default();

  Payload ToPayloadJson(const nlohmann::json& value) const;
  nlohmann::json FromPayloadJson(const Payload& payload) const;

  template <class T>
  Payload ToPayload(const T& value) const {
    if constexpr (std::is_same_v<std::decay_t<T>, nlohmann::json>) {
      return ToPayloadJson(value);
    } else {
      return ToPayloadJson(nlohmann::json(value));
    }
  }

  template <class T>
  T FromPayload(const Payload& payload) const {
    nlohmann::json j = FromPayloadJson(payload);
    if constexpr (std::is_same_v<T, nlohmann::json>) {
      return j;
    } else {
      return j.get<T>();
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
  std::vector<std::shared_ptr<PayloadConverter>> converters_;
};

}  // namespace temporal
