---
title: Ошибки, повторы и таймауты
description: Как сигнализировать о сбоях из активностей, настраивать политики повторов и таймауты, а также обрабатывать ошибки в коде воркфлоу.
---

# Ошибки, повторы и таймауты

На этой странице описано, как SDK передаёт сбои между активностями и воркфлоу,
как управлять поведением повторов и таймаутами, а также что происходит, когда
воркфлоу завершается с ошибкой или обнаруживает нарушение недетерминизма.

Все типы ошибок находятся в `<temporal/common/errors.h>`.

## Иерархия ошибок

```
std::runtime_error
  └── temporal::TemporalError
        ├── temporal::ApplicationError     — бросается кодом вашей активности/воркфлоу
        ├── temporal::ActivityError        — принимается в воркфлоу при сбое активности
        ├── temporal::WorkflowFailedError  — принимается клиентом при неудачном завершении воркфлоу
        ├── temporal::DataConverterError   — ошибка кодирования/декодирования payload
        └── temporal::RpcError             — транспортная / gRPC ошибка связи с сервисом Temporal
              └── temporal::NotFoundError  — сервер вернул NOT_FOUND (неизвестный воркфлоу / расписание / namespace)
```

`NotFoundError` наследуется от `RpcError`, поэтому `catch (const temporal::RpcError&)`
по-прежнему перехватывает её (а `not_found()` возвращает `true`); новый код может
`catch (const temporal::NotFoundError&)` напрямую, чтобы отличать «отсутствует» от
транспортного сбоя.

## Генерация сбоев из активности

Внутри активности бросайте `temporal::ApplicationError`, чтобы сообщить об
именованном, типизированном сбое. Конструктор принимает сообщение для человека,
необязательную **строку типа** и необязательный **флаг неповторяемости**:

```cpp
// По умолчанию повторяемая (сервер повторит попытку согласно политике повторов):
throw temporal::ApplicationError("downstream service unavailable", "ServiceUnavailable");

// Неповторяемая: сервер не будет планировать следующую попытку.
throw temporal::ApplicationError("card declined — do not retry", "CardDeclined",
                                 /*non_retryable=*/true);

// Только сообщение (строка типа пуста, повторяемая):
throw temporal::ApplicationError("unexpected nil response");
```

Строка типа — это произвольный идентификатор, который вы выбираете сами. Она
используется двумя способами:

1. **`RetryPolicy::non_retryable_error_types`** — если тип брошенного
   `ApplicationError` присутствует в этом списке, активность считается
   неповторяемой независимо от флага `non_retryable` в конструкторе.
2. **Точки перехвата в воркфлоу** — вы можете проверить `ActivityError::type()`,
   чтобы различать виды сбоев.

Вы также можете бросать `ApplicationError` из воркфлоу или валидатора обновления;
см. [Валидаторы обновлений](/advanced#update-validators) для соответствующего
примера.

## Перехват сбоев активности в воркфлоу

Когда вызов `Future<R>::Get()` возвращает результат упавшей активности (той,
что исчерпала все попытки повтора или бросила неповторяемую ошибку), он бросает
`temporal::ActivityError`. Метод `type()` возвращает строку типа ошибки из
исходного `ApplicationError`.

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};

try {
  std::string receipt =
      ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, amount).Get();
  // активность выполнена успешно
} catch (const temporal::ActivityError& e) {
  ctx.GetLogger().Error("ChargeCard failed", {{"type", e.type()}, {"what", e.what()}});

  if (e.type() == "CardDeclined") {
    // неповторяемая ошибка, обрабатываем корректно
    return "payment-declined";
  }
  throw;  // перебрасываем другие сбои, чтобы провалить воркфлоу
}
```

:::note
`Future::Get()` для сбоев активностей бросает только `temporal::ActivityError`.
В коде воркфлоу не нужно перехватывать `ApplicationError` — этот тип исключения
предназначен для броска на *стороне активности*, а не для приёма на стороне
воркфлоу.
:::

## Настройка повторов с помощью `RetryPolicy`

`ActivityOptions::retry_policy` — это `std::optional<RetryPolicy>`. Оставьте его
неустановленным (по умолчанию), и сервер применит свои встроенные значения по
умолчанию; присвойте значение, чтобы переопределить их —
`opts.retry_policy = temporal::RetryPolicy{...};`. То же optional-поле есть у
`ChildWorkflowOptions`, `LocalActivityOptions` и `StartWorkflowOptions`.

```cpp
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};  // задержка первого повтора (по умолчанию 1 с)
  double backoff_coefficient{2.0};                   // множитель на каждом повторе
  std::chrono::milliseconds maximum_interval{0};     // 0 => 100 × initial_interval
  int maximum_attempts{0};                           // 0 => без ограничений
  std::vector<std::string> non_retryable_error_types;
};

// В ActivityOptions:
std::optional<RetryPolicy> retry_policy;  // не установлено => поведение повторов по умолчанию сервера
```

### Типичные паттерны

**Ограничить количество попыток и настроить backoff:**

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);

opts.retry_policy = temporal::RetryPolicy{
    .initial_interval    = std::chrono::milliseconds(500),
    .backoff_coefficient = 1.5,
    .maximum_interval    = std::chrono::seconds(30),
    .maximum_attempts    = 5,
};
```

**Помечать определённые типы ошибок как неповторяемые по имени:**

```cpp
opts.retry_policy = temporal::RetryPolicy{
    .non_retryable_error_types = {"CardDeclined", "InvalidInput"},
};
```

Если активность бросает `ApplicationError("...", "CardDeclined")`, сервер
немедленно прекращает повторы — даже если `maximum_attempts` ещё не достигнуто
и флаг `non_retryable` в конструкторе равен `false`.

**Провалиться на первой попытке (без повторов):**

Это паттерн, используемый в интеграционных тестах для случая `FailWorkflow`:

```cpp
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 1};
```

:::note
Оставить `retry_policy` неустановленным — не то же самое, что политика с одной
попыткой: неустановленный optional означает *значения по умолчанию сервера*
(неограниченные повторы с экспоненциальным backoff). Чтобы ограничить или
отключить повторы, нужно присвоить `RetryPolicy` с нужными вам полями.
:::

## Таймауты активностей

`ActivityOptions` предоставляет четыре поля таймаутов:

```cpp
struct ActivityOptions {
  std::chrono::milliseconds schedule_to_close_timeout{0}; // общий бюджет от планирования до завершения
  std::chrono::milliseconds schedule_to_start_timeout{0}; // максимальное время ожидания в очереди задач
  std::chrono::milliseconds start_to_close_timeout{0};    // время выполнения одной попытки (фактически обязательно)
  std::chrono::milliseconds heartbeat_timeout{0};         // максимальный интервал между вызовами RecordHeartbeat
  // ...
};
```

`start_to_close_timeout` — это тот таймаут, который вы почти всегда задаёте.
Он ограничивает, как долго может выполняться одна попытка, прежде чем сервер
сочтёт активность упавшей и запланирует повтор (или окончательный сбой, если
повторы исчерпаны). В заголовке он помечен как *фактически обязательный* —
сервер отклонит команду планирования, в которой вообще не задан таймаут.

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);  // каждая попытка имеет 30 с
```

### Heartbeat-таймаут

Для длительных активностей, вызывающих `ctx.RecordHeartbeat()`, задавайте
`heartbeat_timeout` меньше, чем максимальный интервал между вызовами heartbeat.
Если активность не отправит heartbeat в течение этого окна, сервер считает её
завершившейся по таймауту и планирует повтор:

```cpp
opts.start_to_close_timeout  = std::chrono::minutes(10);
opts.heartbeat_timeout        = std::chrono::seconds(15);  // heartbeat обязателен каждые 15 с
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};
```

`RecordHeartbeat` автоматически дросселирует фактические обращения к серверу
(примерно до 80% от `heartbeat_timeout`), поэтому плотный цикл может вызывать его
свободно — на провод попадают только периодические отчёты, и кэшированное
состояние отмены обновляется при каждом реальном отчёте.

Внутри активности регулярно отправляйте heartbeat и проверяйте `IsCancelled()`,
чтобы активность могла корректно остановиться по запросу сервера:

```cpp
std::string ProcessLargeFile(temporal::activity::Context& ctx, std::string path) {
  for (int chunk = 0; chunk < total_chunks; ++chunk) {
    process(path, chunk);
    ctx.RecordHeartbeat(chunk);      // сбрасывает таймер heartbeat; возвращает флаг отмены
    if (ctx.IsCancelled()) {
      return "cancelled";            // быстро останавливаемся по запросу
    }
  }
  return "done";
}
```

### Как таймаут выглядит для воркфлоу

Активность, завершившаяся по таймауту, проходит тот же путь повторов, что и
любой другой сбой. После исчерпания последнего повтора (или если
`maximum_attempts` равно 1), `Future::Get()` бросает
`temporal::ActivityError`. Строка `type()` в этом случае будет равна
`"TimeoutType_SCHEDULE_TO_CLOSE"`, `"TimeoutType_START_TO_CLOSE"` или
`"TimeoutType_HEARTBEAT"` — вы можете сопоставить её, чтобы отличить таймаут
от прикладного сбоя:

```cpp
try {
  result = ctx.ExecuteActivity<std::string>(opts, "ProcessLargeFile", path).Get();
} catch (const temporal::ActivityError& e) {
  if (e.type().find("Timeout") != std::string::npos) {
    ctx.GetLogger().Warn("activity timed out", {{"type", e.type()}});
  }
  throw;
}
```

## Сбой воркфлоу {#workflow-failure}

Функция воркфлоу, **бросающая необработанное исключение** (любое, кроме
внутренних управляющих сигналов SDK для `ContinueAsNew`, таймеров и т.д.),
завершает выполнение воркфлоу с ошибкой. Сервер Temporal помечает выполнение
как `FAILED`, и при вызове `WorkflowHandle::Result<R>()` на стороне клиента
бросается `WorkflowFailedError`.

На стороне **клиента** перехват сбоя воркфлоу выглядит так:

```cpp
try {
  std::string result = handle.Result<std::string>();
} catch (const temporal::WorkflowFailedError& e) {
  std::cerr << "Workflow failed: " << e.what() << '\n';
  if (e.type() == "CardDeclined") { /* тип прикладного сбоя, декодируется на стороне клиента */ }
}
```

`WorkflowFailedError::type()` повторяет *тип* сбоя прикладной ошибки, которую
бросил воркфлоу, декодированный на стороне клиента из события закрытия, — так что
вы можете различать виды сбоев без разбора сообщения. Он пуст для закрытий, не
связанных с прикладной ошибкой (таймаут, прерывание, отмена). Та же ошибка
возникает, когда воркфлоу завершается по таймауту, прерывается или отменяется.

:::note
Если вы намеренно хотите завершить воркфлоу с ошибкой из его кода, бросайте
`temporal::ApplicationError`. Любое другое необработанное исключение также
завершит воркфлоу с ошибкой, но `ApplicationError` — это идиоматичный,
типизированный способ сигнализировать о намеренном терминальном сбое.
:::

## Недетерминизм и `WorkflowPanicPolicy`

Особый класс сбоев возникает, когда движок воркфлоу воспроизводит историю, а
команды воркфлоу не совпадают с записанными. Это — **нарушение
недетерминизма** — означает, что код воркфлоу изменился несовместимым способом,
пока выполнения находились в процессе.

SDK реагирует согласно `WorkflowPanicPolicy`, настроенной на воркере:

```cpp
temporal::WorkerOptions wopts;
wopts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;  // по умолчанию
temporal::worker::Worker worker(client, "my-task-queue", wopts);
```

- **`BlockWorkflow`** (по умолчанию) — проваливает только текущую **задачу
  воркфлоу**. Сервер повторяет задачу, поэтому развёртывание исправленного
  воркера восстанавливает воркфлоу без потери данных. Соответствует поведению
  `BlockWorkflow` в SDK для Go/Java.
- **`FailWorkflow`** — проваливает всё **выполнение воркфлоу**. Необратимо;
  используйте только когда застрявший, невосстанавливаемый воркфлоу хуже, чем
  упавший.

См. [Расширенные возможности: Обнаружение недетерминизма](/advanced#non-determinism-detection)
для полного описания, включая `GetVersion` для безопасной эволюции кода и паттерн
[Тестирование воспроизведения](/advanced#replay-testing) для выявления нарушений
в CI до попадания в продакшн.

## Сопутствующие возможности обработки сбоев

Несколько возможностей, связанных с обработкой ошибок, описаны в других разделах
документации:

- **Пользовательские конвертеры сбоев** — подключите `FailureConverter`, чтобы
  управлять тем, как ошибки кодируются/декодируются на проводе; см.
  [Конвертация данных](/data-conversion).
- **Асинхронное (ручное) завершение** — активность может отложить результат через
  `ctx.defer_completion()` и быть завершена вне основного потока через
  `Client::CompleteActivity` / `Client::FailActivity`; см. [Активности](#асинхронное-ручное-завершение-активности)
  ниже.
- **Отмена на стороне активности** — вызов `Future::Cancel()` воркфлоу на future
  активности запрашивает отмену; активность наблюдает её через
  `Context::IsCancelled()` при следующем heartbeat.

См. [матрицу паритета](/parity) для полного статуса возможностей.

## Асинхронное (ручное) завершение активности

Активность не обязана возвращать свой результат на месте. Вызов
`ctx.defer_completion()` сообщает воркеру **не** отправлять результат, когда
функция возвращается, и отдаёт обратно токен задачи, чтобы завершить активность
позже:

```cpp
std::string ChargeCard(temporal::activity::Context& ctx, std::string order_id) {
  const std::string token = ctx.defer_completion();  // включает async; возвращает токен задачи
  EnqueueExternalWork(order_id, token);              // другая система завершит её позже
  return {};                                         // возвращаемое значение игнорируется после отложения
}
```

Из любого места, где есть `Client`, завершите её или провалите по токену:

```cpp
client.CompleteActivity(token, std::string("receipt-123"));      // воркфлоу получает этот результат
client.FailActivity(token, "card declined", "CardDeclined");     // воркфлоу видит ActivityError
```

Третий аргумент `FailActivity` — это тип сбоя (по умолчанию `"ApplicationError"`),
который проявляется в воркфлоу как `ActivityError::type()` точно так же, как
брошенный `ApplicationError`.
