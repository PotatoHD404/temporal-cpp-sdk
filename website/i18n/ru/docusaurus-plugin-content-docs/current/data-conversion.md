---
title: Конвертация данных
description: Как сериализуются аргументы и результаты воркфлоу/активностей.
---

# Конвертация данных

Каждое значение, пересекающее границу — аргументы и результаты воркфлоу/активностей, payload-ы сигналов, запросов (query) и обновлений (update) — сериализуется в Temporal **Payload** с помощью `DataConverter`.

## Конвертер по умолчанию

Конвертер по умолчанию выстраивает цепочку из трёх `PayloadConverter`-ов, повторяя стандартный стек Go SDK:

| Кодировка | Обрабатывает | Метаданные `encoding` |
|-----------|-------------|-----------------------|
| Nil | значения `null` | `binary/null` |
| ByteSlice | сырые байты (`nlohmann::json::binary`) | `binary/plain` |
| JSON | всё остальное (catch-all) | `json/plain` |

На практике почти всё проходит через **JSON** посредством [nlohmann/json](https://github.com/nlohmann/json).
Это означает, что любой тип, который nlohmann умеет сериализовать и десериализовать, работает «из коробки»: `std::string`, арифметические типы, `bool`, `std::vector`, `std::map`, `std::optional`, а также ваши собственные структуры — после того как вы научите nlohmann с ними работать.

## Использование собственных типов

Снабдите вашу структуру функциями `to_json`/`from_json` для nlohmann (проще всего использовать макросы `NLOHMANN_DEFINE_TYPE_*`):

```cpp
struct Order {
  std::string id;
  double amount;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Order, id, amount)

// теперь тип прозрачно передаётся через воркфлоу и активности:
Order ProcessOrder(temporal::workflow::Context& ctx, Order order) {
  // ...
  return order;
}
auto handle = client.StartWorkflow(opts, "ProcessOrder", Order{"o-1", 9.99});
Order result = handle.Result<Order>();
```

## Собственный конвертер

Передайте свой конвертер (например, с другим стеком по умолчанию) через `ClientOptions::data_converter`:

```cpp
auto converter = std::make_shared<temporal::DataConverter>(
    std::vector<std::shared_ptr<temporal::PayloadConverter>>{ /* ваши конвертеры в нужном порядке */ });

temporal::ClientOptions opts;
opts.data_converter = converter;
```

`PayloadConverter` реализует следующий интерфейс:

```cpp
class PayloadConverter {
 public:
  virtual std::string encoding() const = 0;
  virtual std::optional<Payload> ToPayload(const nlohmann::json& value) const = 0;  // nullopt — передать дальше
  virtual bool FromPayload(const Payload& p, nlohmann::json& out) const = 0;
};
```

:::note
Конвертеры payload-ов **Protobuf / ProtoJSON** и **кодеки payload-ов** (для сквозного шифрования или сжатия) пока не реализованы. См. [матрицу паритета](/parity).
:::
