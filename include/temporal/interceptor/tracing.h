#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <temporal/common/payload.h>
#include <temporal/interceptor/interceptor.h>

// Distributed-tracing interceptor for the C++ Temporal SDK.
//
// This is OpenTelemetry-SHAPED but carries NO OpenTelemetry dependency: it
// defines its own abstract `Tracer`/`Span` interfaces (mirroring the Go SDK's
// interceptor.Tracer in third_party/reference/sdk-go/interceptor/
// tracing_interceptor.go). A real OTel/OpenTracing adapter would implement
// `Tracer` by bridging to its own SDK. No exporter is bundled here — the
// default `NoopTracer` does nothing, and `InMemoryTracer` (test-only, defined
// below) records spans in-process so the inject/extract round-trip is testable.
//
// PROPAGATION FORMAT (matches the Go SDK exactly in spirit): a span's wire
// context is a flat map<string,string> (Tracer::Inject). The TracingInterceptor
// serializes that map into a SINGLE Payload (JSON, encoding "json/plain") and
// stores it on the Temporal Header under one key (default "_tracer-data"). On
// the inbound side it reads that key back, deserializes the map, and asks the
// Tracer to Extract a parent span context. This keeps the on-the-wire shape
// identical to other SDKs so cross-language traces connect.
//
// The interceptor chain IS wired into the live paths (set via
// ClientOptions/WorkerOptions::interceptors; worker_impl.cpp installs it and the
// TracingInterceptorPropagatesTraceWorkflowToActivity integration test exercises
// workflow->activity propagation). What is bring-your-own is the backend: you
// supply a Tracer/Span adapter — no OpenTelemetry/Jaeger exporter is bundled.
namespace temporal::interceptor {

// An opaque, immutable reference to a span's context — enough to be a parent of
// a new span. Implementations carry whatever ids their backend needs
// (trace/span id, flags, baggage). The base holds the propagated key/value map
// so a NoopTracer / InMemoryTracer can round-trip without extra state.
class SpanContext {
 public:
  SpanContext() = default;
  explicit SpanContext(std::map<std::string, std::string> data) : data_(std::move(data)) {}
  virtual ~SpanContext() = default;
  SpanContext(const SpanContext&) = default;
  SpanContext& operator=(const SpanContext&) = default;
  SpanContext(SpanContext&&) = default;
  SpanContext& operator=(SpanContext&&) = default;

  // The flat propagation map this context was extracted from / will inject as.
  const std::map<std::string, std::string>& data() const { return data_; }
  std::map<std::string, std::string>& mutable_data() { return data_; }

 private:
  std::map<std::string, std::string> data_;
};

// A live span. End() finalizes it; SetTag attaches a key/value. context()
// returns a reference usable as a parent for child spans.
class Span {
 public:
  Span() = default;
  virtual ~Span() = default;
  Span(const Span&) = delete;
  Span& operator=(const Span&) = delete;
  Span(Span&&) = delete;
  Span& operator=(Span&&) = delete;

  virtual void SetTag(const std::string& key, const std::string& value) = 0;
  // Mark the span finished. `error` records that the traced operation failed.
  virtual void End(bool error = false) = 0;
  // The span's own context (so it can parent a child / be injected).
  virtual const SpanContext& context() const = 0;
};

// Options for starting a span.
struct StartSpanOptions {
  // High-level operation ("RunWorkflow", "StartActivity", "RunActivity", ...).
  std::string operation;
  // The specific workflow/activity/signal name.
  std::string name;
  // Optional parent (e.g. extracted from an inbound header). Null => root span.
  const SpanContext* parent = nullptr;
  // Span tags (workflow id, run id, ...).
  std::map<std::string, std::string> tags;
};

// Abstract tracer. A real adapter (OpenTelemetry, OpenTracing, Jaeger, ...)
// implements this. Mirrors the Go SDK's interceptor.Tracer minus framework-only
// bits (idempotency keys, logger correlation).
class Tracer {
 public:
  Tracer() = default;
  virtual ~Tracer() = default;
  Tracer(const Tracer&) = delete;
  Tracer& operator=(const Tracer&) = delete;
  Tracer(Tracer&&) = delete;
  Tracer& operator=(Tracer&&) = delete;

  // Start (and return) a span. Never returns null.
  virtual std::unique_ptr<Span> StartSpan(const StartSpanOptions& options) = 0;

  // Serialize a span's context to a flat map for header propagation. An empty
  // result means "nothing to propagate" (the interceptor then writes no header).
  virtual std::map<std::string, std::string> Inject(const Span& span) const = 0;

  // Reconstruct a parent span context from a propagation map. Returns nullopt if
  // the map carries no usable context.
  virtual std::optional<SpanContext> Extract(
      const std::map<std::string, std::string>& data) const = 0;
};

// Default tracer: every span is a no-op and nothing is propagated. Installed
// when tracing is "enabled" but no backend is configured, so call sites need no
// null checks. (This is the only Tracer the SDK ships — real backends are
// adapters the user provides.)
class NoopTracer : public Tracer {
 public:
  std::unique_ptr<Span> StartSpan(const StartSpanOptions& options) override {
    (void)options;
    return std::make_unique<NoopSpan>();
  }
  std::map<std::string, std::string> Inject(const Span& span) const override {
    (void)span;
    return {};
  }
  std::optional<SpanContext> Extract(
      const std::map<std::string, std::string>& data) const override {
    (void)data;
    return std::nullopt;
  }

 private:
  class NoopSpan : public Span {
   public:
    void SetTag(const std::string& key, const std::string& value) override {
      (void)key;
      (void)value;
    }
    void End(bool error) override { (void)error; }
    const SpanContext& context() const override { return ctx_; }

   private:
    SpanContext ctx_;
  };
};

// The Header key under which the serialized tracer context Payload is stored.
inline constexpr const char* kDefaultTracerHeaderKey = "_tracer-data";

// TracingInterceptor — starts a span around inbound ExecuteWorkflow /
// ExecuteActivity (extracting any parent from the inbound header) and injects the
// current span into the outbound header on ExecuteActivity / ExecuteChildWorkflow
// / SignalExternalWorkflow / StartWorkflow / SignalWorkflow.
//
// It is itself an `Interceptor` (factory): its Intercept* methods produce the
// per-call inbound/outbound wrappers. The Tracer is shared (non-owning) — the
// caller owns it and must keep it alive for the interceptor's lifetime. The
// header (de)serialization helpers are public + static so the lead's wiring (and
// tests) can reuse the exact wire format.
class TracingInterceptor : public Interceptor {
 public:
  explicit TracingInterceptor(Tracer* tracer, std::string header_key = kDefaultTracerHeaderKey)
      : tracer_(tracer), header_key_(std::move(header_key)) {}

  Tracer* tracer() const { return tracer_; }
  const std::string& header_key() const { return header_key_; }

  // --- wire-format helpers (shared by the interceptors below and by tests) ---

  // Serialize a tracer map into the Header under `header_key`. Empty map => no
  // write (mirrors the Go SDK skipping empty span data).
  static void WriteToHeader(const std::string& header_key,
                            const std::map<std::string, std::string>& data, Header& header);

  // Read & deserialize the tracer map from the Header (empty if absent/bad).
  static std::map<std::string, std::string> ReadFromHeader(const std::string& header_key,
                                                           const Header& header);

  // --- Interceptor factory surface ---
  std::unique_ptr<WorkflowInboundInterceptor> InterceptWorkflow(
      WorkflowInboundInterceptor* next) override;
  std::unique_ptr<ActivityInboundInterceptor> InterceptActivity(
      ActivityInboundInterceptor* next) override;
  std::unique_ptr<ClientOutboundInterceptor> InterceptClient(
      ClientOutboundInterceptor* next) override;

 private:
  // Per-call wrappers, defined in tracing.cpp. Each holds the shared root so it
  // can reach the Tracer + header key.
  class WorkflowInbound;
  class WorkflowOutbound;
  class ActivityInbound;
  class ClientOutbound;

  Tracer* tracer_;
  std::string header_key_;
};

// ---------------------------------------------------------------------------
// InMemoryTracer — a minimal, dependency-free Tracer for tests and local runs.
// It assigns incrementing trace/span ids, records started/ended spans in a
// process-local vector, and round-trips the (trace_id, span_id) pair through the
// propagation map so inject→extract→start-child links correctly. NOT a real
// exporter; purely a wiring/round-trip example (analogous to InMemoryPayloadStorage
// in data_converter.h).
// ---------------------------------------------------------------------------
class InMemoryTracer : public Tracer {
 public:
  struct Record {
    std::string operation;
    std::string name;
    std::string trace_id;
    std::string span_id;
    std::string parent_span_id;  // empty if root
    std::map<std::string, std::string> tags;
    bool ended = false;
    bool error = false;
  };

  std::unique_ptr<Span> StartSpan(const StartSpanOptions& options) override;
  std::map<std::string, std::string> Inject(const Span& span) const override;
  std::optional<SpanContext> Extract(
      const std::map<std::string, std::string>& data) const override;

  // Test introspection: every span ever started, in start order.
  const std::vector<Record>& records() const { return records_; }

 private:
  class MemSpan;
  // Wire keys for the propagation map.
  static constexpr const char* kTraceIdKey = "trace_id";
  static constexpr const char* kSpanIdKey = "span_id";

  // mutable: ids advance and records grow from logically-const StartSpan callers
  // (StartSpan is non-const here, but Inject is const and must not mutate).
  std::vector<Record> records_;
  long next_trace_id_ = 1;
  long next_span_id_ = 1;
};

}  // namespace temporal::interceptor
