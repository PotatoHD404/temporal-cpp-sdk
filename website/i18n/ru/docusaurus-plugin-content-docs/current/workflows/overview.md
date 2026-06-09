---
title: Активности и таймеры
description: Вызов активностей, ожидание future, таймеры, политики повторов и правила детерминизма.
---

# Активности и таймеры

Функция воркфлоу принимает `temporal::workflow::Context&` первым параметром и любое количество
(поддающихся конвертации данных) аргументов после него:

```cpp
std::string OrderWorkflow(temporal::workflow::Context& ctx, std::string order_id) {
  // ... оркестрация ...
}
```

Зарегистрируйте её на воркере по имени:

```cpp
worker.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
```

## Выполнение активностей

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);

temporal::workflow::Future<std::string> fut =
    ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, amount);

std::string receipt = fut.Get();   // блокируется (паркуется) до завершения активности
```

- `ExecuteActivity<R>(options, activity_type, args...)` планирует активность и возвращает
  `Future<R>`. Аргументы и результат сериализуются через [конвертер данных](/data-conversion).
- Активность выполняется на том воркере, который опрашивает её очередь задач (по умолчанию —
  очередь задач воркфлоу).
- Если активность завершается ошибкой (и не допускает повторов, либо исчерпала их), `Future::Get()`
  выбрасывает `temporal::ActivityError`.

### Типизированные дескрипторы активностей

Вместо строкового имени и явного `<R>` можно объявить типизированный дескриптор через
`TEMPORAL_ACTIVITY` и позволить имени типа *и* типу результата выводиться из C++-функции — опечатка в
имени или неверный тип аргумента становится ошибкой компиляции:

```cpp
int Increment(temporal::activity::Context& ctx, int n) { return n + 1; }
TEMPORAL_ACTIVITY(Increment);   // объявляет Increment_activity, имя типа "Increment"

int Add2(temporal::workflow::Context& ctx, int base) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);
  int a = ctx.ExecuteActivity(o, Increment_activity, base).Get();   // R выводится, без <int>
  return ctx.ExecuteActivity(o, Increment_activity, a).Get();
}
```

Зарегистрируйте тот же дескриптор на воркере через `worker.Register(Increment_activity)`.
Типизированные дескрипторы сводятся к идентичному строковому имени, поэтому свободно
взаимодействуют со строковым API.

### Параллельное выполнение активностей

Запланируйте несколько активностей *до* ожидания любой из них — они выполнятся конкурентно:

```cpp
auto a = ctx.ExecuteActivity<int>(opts, "Fetch", 1);
auto b = ctx.ExecuteActivity<int>(opts, "Fetch", 2);
auto c = ctx.ExecuteActivity<int>(opts, "Fetch", 3);
int total = a.Get() + b.Get() + c.Get();   // все три выполнились параллельно
```

### Параметры активности

```cpp
struct ActivityOptions {
  std::string task_queue;                                  // по умолчанию: очередь задач воркфлоу
  std::chrono::milliseconds schedule_to_close_timeout{0};
  std::chrono::milliseconds schedule_to_start_timeout{0};
  std::chrono::milliseconds start_to_close_timeout{0};     // фактически обязателен
  std::chrono::milliseconds heartbeat_timeout{0};
  std::optional<RetryPolicy> retry_policy;                 // не задан => поведение повторов по умолчанию на сервере
};
```

Чтобы управлять повторами, задайте весь optional `retry_policy` — незаданный optional означает
поведение повторов по умолчанию на сервере:

```cpp
opts.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};
```

```cpp
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};
  double backoff_coefficient{2.0};
  std::chrono::milliseconds maximum_interval{0};   // 0 => 100 × initial_interval
  int maximum_attempts{0};                         // 0 => без ограничений
  std::vector<std::string> non_retryable_error_types;
};
```

## Написание активностей

```cpp
std::string ChargeCard(temporal::activity::Context& ctx, std::string order_id, double amount) {
  // настоящий ввод-вывод здесь допустим
  return charge(order_id, amount);
}
worker.RegisterActivity("ChargeCard", ChargeCard);
```

Выбросьте `temporal::ApplicationError`, чтобы сообщить об ошибке (при необходимости — без права на повтор):

```cpp
throw temporal::ApplicationError("card declined", "CardDeclined", /*non_retryable=*/true);
```

### Heartbeat

Долгоработающие активности должны отправлять heartbeat, чтобы сервер знал, что они живы (и чтобы быстро обнаружить смерть воркера). Задайте `heartbeat_timeout` в параметрах активности и периодически вызывайте `RecordHeartbeat`:

```cpp
std::string ProcessBatch(temporal::activity::Context& ctx, int n) {
  for (int i = 0; i < n; ++i) {
    do_chunk(i);
    ctx.RecordHeartbeat(i);   // необязательная деталь прогресса
  }
  return "done";
}
```

Если активность не отправляет heartbeat в течение `heartbeat_timeout`, сервер завершает её по таймауту и запускает повтор.

## Локальные активности

**Локальная активность** выполняется встроенно в воркере воркфлоу — без отдельного обхода задачи
активности — и записывает свой результат как маркер; повторы происходят встроенно в рамках задачи
воркфлоу. Лучше всего подходит для коротких идемпотентных шагов, где накладные расходы на
планирование преобладали бы. В отличие от `ExecuteActivity`, она разрешается прямо в вызове и
возвращает значение напрямую (без `Future`):

```cpp
temporal::LocalActivityOptions o;
o.start_to_close_timeout = std::chrono::seconds(5);
o.retry_policy = temporal::RetryPolicy{.maximum_attempts = 3};

int total = ctx.ExecuteLocalActivity<int>(o, "LocalAdd", base, 5);
```

## Таймеры

```cpp
using namespace temporal::literals;            // подключает chrono-литералы длительностей

ctx.Sleep(5s);                                  // блокировка на 5с

auto timer = ctx.NewTimer(30s);
// ... делаем что-то ещё ...
timer.Get();                                    // блокировка до срабатывания
```

`Sleep`/`NewTimer` принимают любую длительность `std::chrono`; `temporal::literals` реэкспортирует
стандартные литералы длительностей (`5s`, `30s`, `24h`, …), чтобы параметры и таймеры читались
естественно. Таймеры долговечны: воркфлоу, ожидающий 30 дней, переживёт перезапуски воркера.

## Обход детерминизма: побочные эффекты и версионирование

Код воркфлоу воспроизводится через реплей, поэтому он не может напрямую вызывать `rand()`, читать
часы или генерировать UUID. Контекст предоставляет записываемые «лазейки» именно для этих случаев:

```cpp
// Выполняет fn один раз, записывает результат в историю; при реплее возвращается записанное значение.
std::string id = ctx.SideEffect<std::string>([] { return new_uuid(); });

// Как SideEffect, но с ключом и записывает новый маркер только при изменении значения.
// R выводится из вызываемого объекта — без явного шаблонного аргумента.
int cfg = ctx.MutableSideEffect("config", [&] { return load_config_version(); });
```

Когда вы меняете логику задеплоенного воркфлоу, `GetVersion` позволяет старым и новым историям
сосуществовать при реплее. Он записывает маркер версии при первом выполнении и возвращает
`kDefaultVersion` (`-1`) при реплее истории, записанной до того, как этот вызов существовал:

```cpp
int v = ctx.GetVersion("greeting-change", temporal::workflow::kDefaultVersion, 1);
if (v == temporal::workflow::kDefaultVersion) {
  // исходное поведение
} else {
  // новое поведение (v == 1)
}
```

## Атрибуты поиска

Сделайте работающий воркфлоу доступным для обнаружения в запросах видимости, добавив (upsert)
индексированные атрибуты поиска. Стройте типизированные значения с помощью хелперов
`temporal::sa::` (атрибут уже должен быть зарегистрирован в неймспейсе):

```cpp
#include <temporal/common/search_attribute.h>

ctx.UpsertSearchAttributes({{"Tier", temporal::sa::Keyword("gold")}});
```

## Правила детерминизма

Код воркфлоу воспроизводится через реплей, поэтому он должен быть детерминированным. Внутри воркфлоу:

- ✅ Используйте `ctx.ExecuteActivity`, `ctx.Sleep`/`NewTimer`, сигналы, запросы (query), селекторы, дочерние воркфлоу.
- ✅ Используйте `ctx.GetLogger()` для логирования и `ctx.GetInfo()` для получения метаданных.
- ✅ Прибегайте к `ctx.SideEffect` / `ctx.MutableSideEffect`, когда нужно однократно записанное
  значение (идентификатор, случайность, чтение часов), и к `ctx.GetVersion`, чтобы безопасно
  развивать логику задеплоенного воркфлоу.
- ❌ Не вызывайте `rand()`, не читайте системные часы, не засыпайте через `std::this_thread::sleep_for`, не выполняйте сетевой или дисковый ввод-вывод и не порождайте потоки. Всё это выносите в **активности**.
- ❌ Не используйте `catch (...)` вокруг ожидающих вызовов так, чтобы могли быть проглочены внутренние исключения потока управления движка.

:::note
Воркер обнаруживает недетерминизм при реплее: если воспроизведённые команды воркфлоу расходятся с
записанной историей, он применяет `WorkflowPanicPolicy` (по умолчанию `BlockWorkflow` — провалить
задачу, чтобы исправленная сборка могла восстановиться). Используйте `ctx.GetVersion`, чтобы менять
логику задеплоенного воркфлоу, не ломая выполняющиеся исполнения. Смотрите [матрицу паритета](/parity).
:::
