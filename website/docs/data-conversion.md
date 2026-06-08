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

:::note
**Protobuf / ProtoJSON** payload converters and **payload codecs** (for end-to-end encryption or
compression) are not yet implemented. See the [parity matrix](/parity).
:::
