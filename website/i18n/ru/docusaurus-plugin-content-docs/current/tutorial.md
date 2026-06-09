---
title: "Туториал — ваш первый воркфлоу"
description: Создайте реальный воркфлоу обработки заказов от начала до конца с помощью Temporal C++ SDK.
---

# Туториал — ваш первый воркфлоу

В этом туториале вы шаг за шагом построите реалистичный многоэтапный воркфлоу: **пайплайн
обработки заказов**, который проверяет заказ, списывает оплату с карты и отправляет подтверждение,
а затем расширяет его с помощью сигнала, позволяющего оператору одобрять заказы до списания средств.

Вы напишете настоящий C++-код, который компилируется и запускается против локального сервера
Temporal. Если вы ещё не собрали SDK, начните с раздела [Начало работы](./getting-started).

## Что вы построите

- Две активности: `ValidateOrder` и `ChargeCard`
- Воркфлоу `ProcessOrder`, который вызывает их последовательно и возвращает квитанцию
- Процесс-воркер, который регистрирует и запускает всё это
- Клиент, который запускает воркфлоу и выводит результат
- Сигнал одобрения, который приостанавливает воркфлоу до получения команды оператора «вперёд»

Готовый код умещается в одном файле `main.cpp` и использует только заголовки, подключаемые через
`<temporal/temporal.h>`.

---

## Часть 1 — активности

Активности — это рабочая лошадка Temporal: они выполняются в реальном времени, могут делать I/O и
автоматически повторяются при сбое. Активность — это обычная функция C++, первый параметр которой —
`temporal::activity::Context&`.

```cpp
#include <temporal/temporal.h>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace std::chrono_literals;

// ── активности ────────────────────────────────────────────────────────────────

// Валидация заказа. Возвращает проверенную сумму или бросает исключение при некорректных данных.
double ValidateOrder(temporal::activity::Context& ctx,
                     std::string order_id, double amount) {
  ctx.GetInfo();  // доступно, если нужен activity_id, attempt и т.д.
  if (amount <= 0) {
    // Не повторяется: воркфлоу получает это как фатальную ActivityError.
    throw temporal::ApplicationError(
        "order amount must be positive", "InvalidOrder", /*non_retryable=*/true);
  }
  // В реальном коде здесь было бы обращение к базе данных, внутреннему API и т.д.
  return amount;
}

// Списание с карты. Возвращает строку-квитанцию.
std::string ChargeCard(temporal::activity::Context& ctx,
                       std::string order_id, double amount) {
  // Имитация вызова платёжного провайдера. В реальном коде — обращение к Stripe и т.п.
  return "receipt-" + order_id + "-" + std::to_string(static_cast<int>(amount * 100));
}
```

:::note
`temporal::ApplicationError` содержит строковый **тип** (`"InvalidOrder"`), по которому политики
повторов и обработчики исключений могут различать ошибки. Передайте `non_retryable = true`, чтобы
сервер не делал повторных попыток — в данном случае некорректная сумма не будет успешной ни при
каком количестве попыток.
:::

---

## Часть 2 — воркфлоу

Воркфлоу — тоже обычная функция, но её первый параметр — `temporal::workflow::Context&`, а тело
должно быть **детерминированным**: никакого чтения системного времени, случайных чисел, прямого I/O.
Всё это живёт в активностях.

`ctx.ExecuteActivity<R>(opts, "ActivityName", args...)` планирует выполнение активности и возвращает
`Future<R>`. Вызов `.Get()` на future приостанавливает воркфлоу до завершения активности (или её
сбоя).

```cpp
// ── воркфлоу ──────────────────────────────────────────────────────────────────

std::string ProcessOrder(temporal::workflow::Context& ctx,
                         std::string order_id, double amount) {
  // Опции применяются к каждому вызову ExecuteActivity; можно переиспользовать или задавать отдельно.
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = 30s;  // каждая активность получает до 30 с на выполнение
  // retry_policy необязательна: оставьте её неустановленной для значений по умолчанию сервера или задайте свою.
  opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};  // до 3 попыток

  // Шаг 1 — валидация.
  // Future<double>::Get() либо возвращает проверенную сумму, либо бросает ActivityError.
  const double validated =
      ctx.ExecuteActivity<double>(opts, "ValidateOrder", order_id, amount).Get();

  // Шаг 2 — списание.
  const std::string receipt =
      ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, validated).Get();

  ctx.GetLogger().Info("order processed", {{"order_id", order_id}, {"receipt", receipt}});
  return receipt;
}
```

:::tip
Активности, запланированные **до** первого `.Get()`, выполняются параллельно. Здесь они
последовательны, потому что каждый шаг зависит от предыдущего. Для паттернов fan-out (например,
одновременное списание нескольких позиций) сначала запланируйте все future, а затем вызовите `.Get()`
на каждом из них — см. пример параллельного выполнения в разделе
[Активности и таймеры](./workflows/overview).
:::

:::note
Предпочитаете синтаксис корутин C++20? Воркфлоу может вместо этого возвращать
`temporal::workflow::workflow_task<R>` и делать `co_await` future / `co_return` его результата —
`co_await ctx.ExecuteActivity<R>(...)` в точности эквивалентен `.Get()` (те же команды, то же
воспроизведение). В этом туториале повсюду используется форма `.Get()`; оба стиля написания
совместимы между собой.
:::

---

## Часть 3 — воркер и клиент

`Worker` опрашивает очередь задач и направляет задачи к вашим зарегистрированным функциям. `Client`
запускает воркфлоу и взаимодействует с ними. Клиент и воркер используют одно и то же имя очереди
задач — именно так Temporal маршрутизирует задачи.

```cpp
// ── воркер + клиент ───────────────────────────────────────────────────────────

int main() {
  // Подключение к локальному dev-серверу. По умолчанию: localhost:7233, namespace "default".
  auto client = temporal::client::Client::Connect(
      {.target = "localhost:7233", .ns = "default"});

  // Создание воркера на очереди задач "orders".
  temporal::worker::Worker worker(client, "orders");

  // Регистрация всех типов воркфлоу и активностей, которые должен обрабатывать воркер.
  // Строковое имя — это «тип», используемый в StartWorkflow / ExecuteActivity.
  worker.RegisterWorkflow("ProcessOrder", ProcessOrder);
  worker.RegisterActivity("ValidateOrder", ValidateOrder);
  worker.RegisterActivity("ChargeCard",    ChargeCard);

  // Start() запускает потоки опроса и возвращает управление немедленно.
  // Run() блокирует до получения SIGINT — удобно для самостоятельного процесса-воркера.
  worker.Start();

  // Запуск выполнения воркфлоу. id необязателен; если не указан, генерируется UUID.
  temporal::StartWorkflowOptions wf_opts;
  wf_opts.id         = "order-001";
  wf_opts.task_queue = "orders";

  auto handle = client.StartWorkflow(wf_opts, "ProcessOrder",
                                     std::string("order-001"), 49.99);

  // Result<R>() выполняет long-poll до закрытия воркфлоу, затем декодирует возвращаемое значение.
  // Бросает WorkflowFailedError, если воркфлоу завершился с ошибкой, таймаутом или был прерван.
  try {
    const std::string receipt = handle.Result<std::string>();
    std::cout << "Receipt: " << receipt << "\n";
  } catch (const temporal::WorkflowFailedError& e) {
    std::cerr << "Workflow failed: " << e.what() << "\n";
    worker.Stop();
    return 1;
  }

  worker.Stop();
  return 0;
}
```

### Запуск

```bash
# Терминал 1 — запуск локального dev-сервера
temporal server start-dev

# Терминал 2 — сборка и запуск
cmake --preset default && cmake --build build -j
./build/examples/order_pipeline/order_pipeline
# Receipt: receipt-order-001-4999
```

Откройте Temporal Web UI по адресу **http://localhost:8233** и найдите `order-001` в списке
воркфлоу. Кликните, чтобы просмотреть полную историю событий: `WorkflowExecutionStarted`, две пары
`ActivityTaskScheduled` / `ActivityTaskCompleted` и `WorkflowExecutionCompleted`.

---

## Часть 4 — добавление сигнала одобрения

Ваш пайплайн заказов списывает средства немедленно, но некоторые заказы требуют предварительного
одобрения человеком. Сигналы позволяют приостановить воркфлоу и ждать внешнего события.

Канал сигнала типизирован: `ctx.GetSignalChannel<T>("signal-name")` возвращает `ReceiveChannel<T>`.
Вызов `.Receive()` детерминированно приостанавливает воркфлоу — так же, как `.Get()` на future, —
до получения сигнала с этим именем.

```cpp
// ── переработанный воркфлоу со шлюзом одобрения ──────────────────────────────

std::string ProcessOrderWithApproval(temporal::workflow::Context& ctx,
                                     std::string order_id, double amount) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = 30s;
  opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};

  // Шаг 1 — валидация (как прежде).
  const double validated =
      ctx.ExecuteActivity<double>(opts, "ValidateOrder", order_id, amount).Get();

  // Шаг 2 — ожидание одобрения оператором.
  // Воркфлоу приостанавливается здесь; сервер надёжно хранит сигнал до его получения.
  auto approval_ch = ctx.GetSignalChannel<std::string>("approve");
  const std::string decision = approval_ch.Receive();  // "yes" или "no"

  if (decision != "yes") {
    // Возвращаем не-квитанцию в знак отказа — или бросаем ApplicationError.
    return "rejected";
  }

  // Шаг 3 — списание (только после одобрения).
  const std::string receipt =
      ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, validated).Get();

  return receipt;
}
```

### Отправка сигнала из клиента

`WorkflowHandle::Signal` вариативен — передайте payload напрямую, и SDK закодирует его через
[конвертер данных](./data-conversion) за вас (не нужно собирать `Payloads` вручную):

```cpp
// Зарегистрируйте новый тип воркфлоу рядом со старым или замените его:
worker.RegisterWorkflow("ProcessOrderWithApproval", ProcessOrderWithApproval);

temporal::StartWorkflowOptions wf_opts;
wf_opts.id         = "order-002";
wf_opts.task_queue = "orders";

auto handle = client.StartWorkflow(wf_opts, "ProcessOrderWithApproval",
                                   std::string("order-002"), 120.00);

// Воркфлоу сейчас приостановлен, ожидая сигнала "approve".
// В реальной системе это делает отдельный процесс (например, панель проверки).
handle.Signal("approve", std::string("yes"));  // кодируется за вас

const std::string receipt = handle.Result<std::string>();
std::cout << "Receipt: " << receipt << "\n";
```

:::tip
Чтобы имя не могло разойтись, объявите типизированную ссылку `temporal::SignalRef<T>` один раз и
используйте её на обоих концах — `ctx.GetSignalChannel(ref)` в воркфлоу и `handle.Signal(ref, value)`
из клиента. Тип payload тогда проверяется на этапе компиляции. См.
[Сигналы, запросы и обновления](./workflows/messaging).
:::

:::tip
`Signal()` работает по принципу «выстрелил и забыл»: он возвращает управление сразу после успешного
выполнения RPC; воркфлоу может ещё не обработать сигнал. Если вам нужно подтверждение того, что
сигнал **применён** к состоянию воркфлоу, зарегистрируйте обработчик запроса (query) и делайте
опрос — см. [Сигналы, запросы и обновления](./workflows/messaging).
:::

---

## Часть 5 — таймер для дедлайна одобрения

Что если никто не одобряет заказ в течение 24 часов? Можно устроить гонку сигнала против таймера с
помощью `Selector`. `Selector` ждёт несколько случаев — future (активности, таймеры, дочерние
воркфлоу) **и** приёмы из каналов сигналов — и запускает обработчик того из них, который становится
готов первым.

```cpp
#include <temporal/workflow/selector.h>

std::string ProcessOrderWithDeadline(temporal::workflow::Context& ctx,
                                     std::string order_id, double amount) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = 30s;
  opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};

  const double validated =
      ctx.ExecuteActivity<double>(opts, "ValidateOrder", order_id, amount).Get();

  // Гонка сигнала одобрения против 24-часового дедлайна.
  auto approval_ch  = ctx.GetSignalChannel<std::string>("approve");
  auto deadline_fut = ctx.NewTimer(24h);  // устойчивый: переживает перезапуски воркера

  std::string decision;
  bool timed_out = false;

  temporal::workflow::Selector sel(ctx);
  // Обработчик AddReceive получает значение потреблённого сигнала, когда он буферизован.
  sel.AddReceive<std::string>(approval_ch, [&](std::string v) { decision = std::move(v); });
  // AddFuture на void-future таймера срабатывает по истечении дедлайна.
  sel.AddFuture(deadline_fut, [&]() { timed_out = true; });
  sel.Select();  // приостанавливается до прихода сигнала ИЛИ срабатывания таймера

  if (timed_out || decision != "yes") {
    return timed_out ? "expired" : "rejected";
  }

  // Одобрено вовремя — списываем.
  return ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, validated).Get();
}
```

:::tip
Добавьте случай `sel.AddDefault([&]{ ... })`, чтобы сделать `Select()` неблокирующим — он запускает
обработчик по умолчанию немедленно, когда ни один другой случай не готов, вместо приостановки. Также
можно устроить гонку с отменой через `sel.AddFuture(ctx.AwaitCancellation(), ...)`.
:::

---

## Общая картина

Вот как все части соединяются вместе:

```
Client                            Temporal Server               Worker
──────                            ───────────────               ──────
StartWorkflow("ProcessOrder") ──► route to "orders" queue ──► ProcessOrder(ctx, ...)
                                                                  │
                                                                  ├─ ExecuteActivity("ValidateOrder")
                                                                  │    ValidateOrder(ctx, ...) ◄── activity task
                                                                  │
                                                                  ├─ GetSignalChannel("approve").Receive()
                                                                  │    (workflow parked in sticky cache)
                                                                  │
handle.Signal("approve", "yes") ─►  deliver signal              │
                                                                  │    resume → charge step
                                                                  └─ ExecuteActivity("ChargeCard")
                                                                       ChargeCard(ctx, ...) ◄── activity task

handle.Result<string>() ◄─────── workflow closed (receipt)
```

Ключевые инварианты:
- Воркер регистрирует каждую функцию **один раз** по имени. Строка имени должна точно совпадать с
  тем, что вы передаёте в `StartWorkflow` / `ExecuteActivity`.
- Воркфлоу детерминированы; активности — нет. Если вам нужен случайный идентификатор, текущая
  метка времени или любые внешние данные — получайте их из активности.
- Sticky-кэш хранит работающие воркфлоу в памяти, поэтому сигналы и обновления применяются к живому
  состоянию без полного воспроизведения истории. Когда кэш «холодный» (новый воркер, перезапуск)
  движок воспроизводит историю для восстановления состояния — именно поэтому код воркфлоу должен
  быть детерминированным.

---

## Что читать дальше

| Тема | Документ |
|---|---|
| Активности подробно (таймауты, повторы, heartbeat) | [Активности и таймеры](./workflows/overview) |
| Сигналы, запросы и обновления | [Сигналы, запросы и обновления](./workflows/messaging) |
| Опции клиента, `GetHandle`, `Describe` | [Клиент и воркер](./client-and-worker) |
| Паритет с официальными SDK (что реализовано, что нет) | [Возможности и паритет](./parity) |
| JSON-кодирование, пользовательские конвертеры | [Конвертация данных](./data-conversion) |
| Тестирование воспроизведением без живого сервера | [Тестирование](./testing) |
