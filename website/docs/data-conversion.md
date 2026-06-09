---
title: Data conversion
description: How workflow/activity arguments and results are serialized.
---

# Data conversion

Every value that crosses the boundary — workflow/activity arguments and results, signal/query/update
payloads — is serialized to a Temporal **Payload** by a `DataConverter`.

## The default converter

The default converter chains three `PayloadConverter`s, matching the Go SDK's default stack:

| Encoding | Handles | `encoding` metadata |
|----------|---------|---------------------|
| Nil | `null` values | `binary/null` |
| ByteSlice | raw bytes (`nlohmann::json::binary`) | `binary/plain` |
| JSON | everything else (the catch-all) | `json/plain` |

In practice almost everything goes through **JSON**, via [nlohmann/json](https://github.com/nlohmann/json).
That means any type nlohmann can (de)serialize works out of the box: `std::string`, arithmetic types,
`bool`, `std::vector`, `std::map`, `std::optional`, and your own structs once you teach nlohmann about
them.

## Using your own types

Give your struct nlohmann's `to_json`/`from_json` (the `NLOHMANN_DEFINE_TYPE_*` macros are easiest):

```cpp
struct Order {
  std::string id;
  double amount;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Order, id, amount)

// now it flows through workflows and activities transparently:
Order ProcessOrder(temporal::workflow::Context& ctx, Order order) {
  // ...
  return order;
}
auto handle = client.StartWorkflow(opts, "ProcessOrder", Order{"o-1", 9.99});
Order result = handle.Result<Order>();
```

## A custom converter

Supply your own converter (e.g. a different default stack) via `ClientOptions::data_converter`:

```cpp
auto converter = std::make_shared<temporal::DataConverter>(
    std::vector<std::shared_ptr<temporal::PayloadConverter>>{ /* your converters, in order */ });

temporal::ClientOptions opts;
opts.data_converter = converter;
```

A `PayloadConverter` implements:

```cpp
class PayloadConverter {
 public:
  virtual std::string encoding() const = 0;
  virtual std::optional<Payload> ToPayload(const nlohmann::json& value) const = 0;  // nullopt to pass
  virtual bool FromPayload(const Payload& p, nlohmann::json& out) const = 0;
};
```

## Protobuf messages

Protobuf-generated messages are detected automatically and encoded as binary
protobuf (`binary/protobuf`) instead of going through the JSON stack — no extra
wiring. To emit the proto-JSON mapping (`json/protobuf`) instead, use a converter
built with `DataConverter::WithProtoJson()` (or call `SetProtoJson(true)`).
Decoding always accepts **both** encodings regardless of the toggle, so a peer
can read either form.

## Payload codecs

A `PayloadCodec` transforms an already-converted payload on its way to/from the
server — compression or encryption "at rest" — applied after the converter on
encode and before it on decode. Both peers must share the same codec chain. Wrap
the default stack in a chain with `DataConverter::WithCodecs`:

```cpp
auto dc = temporal::DataConverter::WithCodecs(
    {std::make_shared<temporal::GzipPayloadCodec>()});  // e.g. gzip-compress every payload
opts.data_converter = dc;
```

The SDK bundles `GzipPayloadCodec` (zlib), `Base64PayloadCodec` (reference
stand-in), and `PayloadStorage` implementations (`InMemoryPayloadStorage`,
`FilePayloadStorage`) for offloading oversized bodies to an external store.

## Failure converters

A `FailureConverter` translates C++ error types to/from Temporal's wire failure
proto (`temporal.api.failure.v1.Failure`), so you can control how application
failures are encoded (carry extra detail, redact, etc.). Install one on a
`DataConverter`:

```cpp
auto dc = temporal::DataConverter::Default();
dc->WithFailureConverter(std::make_shared<MyFailureConverter>());
opts.data_converter = dc;
```

A `FailureConverter` implements two methods:

```cpp
class FailureConverter {
 public:
  virtual void ErrorToFailure(const std::exception& error,
                              temporal::api::failure::v1::Failure& out) const = 0;
  virtual std::exception_ptr FailureToError(
      const temporal::api::failure::v1::Failure& failure) const = 0;
};
```

The default converter (`DefaultFailureConverter`) round-trips `ApplicationError`
losslessly (type, non-retryable flag, message, stack trace). The configured
converter is used to encode **both** activity failures and a workflow's own
failure. On the client side, a failed workflow surfaces as `WorkflowFailedError`,
whose `type()` carries the decoded application-failure type — see
[Errors, retries & timeouts](/error-handling#workflow-failure).
