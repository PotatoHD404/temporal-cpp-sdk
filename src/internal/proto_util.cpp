#include "internal/proto_util.h"

#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>

namespace temporal::internal {

tapi::common::v1::Payload ToProtoPayload(const Payload& p) {
  tapi::common::v1::Payload out;
  for (const auto& [k, v] : p.metadata) {
    (*out.mutable_metadata())[k] = v;
  }
  out.set_data(p.data);
  return out;
}

Payload FromProtoPayload(const tapi::common::v1::Payload& p) {
  Payload out;
  for (const auto& [k, v] : p.metadata()) {
    out.metadata[k] = v;
  }
  out.data = p.data();
  return out;
}

tapi::common::v1::Payloads ToProtoPayloads(const Payloads& ps) {
  tapi::common::v1::Payloads out;
  for (const auto& p : ps) {
    *out.add_payloads() = ToProtoPayload(p);
  }
  return out;
}

Payloads FromProtoPayloads(const tapi::common::v1::Payloads& ps) {
  Payloads out;
  out.reserve(static_cast<std::size_t>(ps.payloads_size()));
  for (const auto& p : ps.payloads()) {
    out.push_back(FromProtoPayload(p));
  }
  return out;
}

gpb::Duration ToProtoDuration(std::chrono::nanoseconds d) {
  gpb::Duration out;
  const auto secs = std::chrono::duration_cast<std::chrono::seconds>(d);
  out.set_seconds(secs.count());
  out.set_nanos(static_cast<std::int32_t>((d - secs).count()));
  return out;
}

tapi::failure::v1::Failure MakeApplicationFailure(const std::string& message,
                                                  const std::string& type) {
  tapi::failure::v1::Failure f;
  f.set_message(message);
  auto* info = f.mutable_application_failure_info();
  if (!type.empty()) {
    info->set_type(type);
  }
  return f;
}

std::string DefaultIdentity() {
  std::array<char, 256> host{};
  if (::gethostname(host.data(), host.size()) != 0) {
    host[0] = '\0';
  }
  host[host.size() - 1] = '\0';
  return std::to_string(static_cast<long>(::getpid())) + "@" + std::string(host.data());
}

std::string NewUuid() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<std::uint64_t> dist;
  const std::uint64_t a = dist(rng);
  const std::uint64_t b = dist(rng);
  std::array<char, 37> buf{};
  std::snprintf(buf.data(), buf.size(), "%08x-%04x-4%03x-%04x-%012llx",
                static_cast<std::uint32_t>(a >> 32),
                static_cast<std::uint32_t>((a >> 16) & 0xffffU),
                static_cast<std::uint32_t>(a & 0x0fffU),
                static_cast<std::uint32_t>(((b >> 48) & 0x3fffU) | 0x8000U),
                static_cast<unsigned long long>(b & 0xffffffffffffULL));
  return std::string(buf.data());
}

}  // namespace temporal::internal
