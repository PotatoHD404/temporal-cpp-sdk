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

## Protobuf-сообщения

Сообщения, сгенерированные Protobuf, определяются автоматически и кодируются как двоичный
protobuf (`binary/protobuf`) вместо прохождения через JSON-стек — без дополнительной
обвязки. Чтобы вместо этого использовать proto-JSON-отображение (`json/protobuf`), применяйте конвертер,
построенный с помощью `DataConverter::WithProtoJson()` (или вызовите `SetProtoJson(true)`).
Декодирование всегда принимает **обе** кодировки независимо от переключателя, так что пир
может прочитать любую из форм.

## Кодеки payload-ов

`PayloadCodec` преобразует уже сконвертированный payload на пути к серверу и обратно —
сжатие или шифрование «при хранении» — применяясь после конвертера при кодировании и перед
ним при декодировании. Оба пира должны использовать одну и ту же цепочку кодеков. Оберните
стек по умолчанию в цепочку с помощью `DataConverter::WithCodecs`:

```cpp
auto dc = temporal::DataConverter::WithCodecs(
    {std::make_shared<temporal::GzipPayloadCodec>()});  // например, gzip-сжатие каждого payload-а
opts.data_converter = dc;
```

SDK поставляется с `GzipPayloadCodec` (zlib), `Base64PayloadCodec` (эталонная
заглушка) и реализациями `PayloadStorage` (`InMemoryPayloadStorage`,
`FilePayloadStorage`) для выгрузки слишком больших тел во внешнее хранилище.

## Конвертеры ошибок

`FailureConverter` транслирует C++-типы ошибок в проводной failure-proto Temporal
(`temporal.api.failure.v1.Failure`) и обратно, так что вы можете управлять тем, как кодируются
ошибки приложения (нести дополнительные данные, редактировать и т.д.). Установите его на
`DataConverter`:

```cpp
auto dc = temporal::DataConverter::Default();
dc->WithFailureConverter(std::make_shared<MyFailureConverter>());
opts.data_converter = dc;
```

`FailureConverter` реализует два метода:

```cpp
class FailureConverter {
 public:
  virtual void ErrorToFailure(const std::exception& error,
                              temporal::api::failure::v1::Failure& out) const = 0;
  virtual std::exception_ptr FailureToError(
      const temporal::api::failure::v1::Failure& failure) const = 0;
};
```

Конвертер по умолчанию (`DefaultFailureConverter`) выполняет round-trip `ApplicationError`
без потерь (тип, флаг non-retryable, сообщение, трассировка стека). Сконфигурированный
конвертер используется для кодирования **как** ошибок активностей, **так и** собственной ошибки
воркфлоу. На стороне клиента упавший воркфлоу проявляется как `WorkflowFailedError`,
чей `type()` несёт декодированный тип ошибки приложения — см.
[Ошибки, повторы и таймауты](/error-handling#workflow-failure).
