---
title: Активности и таймеры
description: Вызов активностей, ожидание future, таймеры, политики повторов и правила детерминизма.
---

# Активности и таймеры

Функция воркфлоу принимает `temporal::workflow::Context&` первым параметром и любое количество
(поддающихся конвертации данных) аргументов после него:

```cpp
std::string OrderWorkflow(temporal::workflow::Context& ctx, std::string order_id) {
  // ... orchestrate ...
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

std::string receipt = fut.Get();   // blocks (parks) until the activity completes
```

- `ExecuteActivity<R>(options, activity_type, args...)` планирует активность и возвращает
  `Future<R>`. Аргументы и результат сериализуются через [конвертер данных](/data-conversion).
- Активность выполняется на том воркере, который опрашивает её очередь задач (по умолчанию —
  очередь задач воркфлоу).
- Если активность завершается ошибкой (и не допускает повторов, либо исчерпала их), `Future::Get()`
  выбрасывает `temporal::ActivityError`.

### Параллельное выполнение активностей

Запланируйте несколько активностей *до* ожидания любой из них — они выполнятся конкурентно:

```cpp
auto a = ctx.ExecuteActivity<int>(opts, "Fetch", 1);
auto b = ctx.ExecuteActivity<int>(opts, "Fetch", 2);
auto c = ctx.ExecuteActivity<int>(opts, "Fetch", 3);
int total = a.Get() + b.Get() + c.Get();   // all three ran in parallel
```

### Параметры активности

```cpp
struct ActivityOptions {
  std::string task_queue;                                  // default: the workflow's task queue
  std::chrono::milliseconds schedule_to_close_timeout{0};
  std::chrono::milliseconds schedule_to_start_timeout{0};
  std::chrono::milliseconds start_to_close_timeout{0};     // effectively required
  std::chrono::milliseconds heartbeat_timeout{0};
  RetryPolicy retry_policy;
  bool retry_policy_set = false;                           // set true to apply retry_policy
};
```

Чтобы управлять повторами, задайте `retry_policy` и установите `retry_policy_set = true`:

```cpp
opts.retry_policy.maximum_attempts = 3;
opts.retry_policy_set = true;
```

```cpp
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};
  double backoff_coefficient{2.0};
  std::chrono::milliseconds maximum_interval{0};   // 0 => 100 × initial_interval
  int maximum_attempts{0};                         // 0 => unlimited
  std::vector<std::string> non_retryable_error_types;
};
```

## Написание активностей

```cpp
std::string ChargeCard(temporal::activity::Context& ctx, std::string order_id, double amount) {
  // real I/O is fine here
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
    ctx.RecordHeartbeat(i);   // optional progress detail
  }
  return "done";
}
```

Если активность не отправляет heartbeat в течение `heartbeat_timeout`, сервер завершает её по таймауту и запускает повтор.

## Таймеры

```cpp
ctx.Sleep(std::chrono::seconds(5));            // block for 5s

auto timer = ctx.NewTimer(std::chrono::seconds(30));
// ... do other things ...
timer.Get();                                    // block until it fires
```

Таймеры долговечны: воркфлоу, ожидающий 30 дней, переживёт перезапуски воркера.

## Правила детерминизма

Код воркфлоу воспроизводится через реплей, поэтому он должен быть детерминированным. Внутри воркфлоу:

- ✅ Используйте `ctx.ExecuteActivity`, `ctx.Sleep`/`NewTimer`, сигналы, запросы (query), селекторы, дочерние воркфлоу.
- ✅ Используйте `ctx.GetLogger()` для логирования и `ctx.GetInfo()` для получения метаданных.
- ❌ Не вызывайте `rand()`, не читайте системные часы, не засыпайте через `std::this_thread::sleep_for`, не выполняйте сетевой или дисковый ввод-вывод и не порождайте потоки. Всё это выносите в **активности**.
- ❌ Не используйте `catch (...)` вокруг ожидающих вызовов так, чтобы могли быть проглочены внутренние исключения потока управления движка.

:::note
В данном SDK ещё не реализованы автоматическое *обнаружение* недетерминизма и *версионирование* воркфлоу
(`GetVersion`/patching). До их появления изменение логики задеплоенного воркфлоу может нарушить
выполнение in-flight executions при реплее — та же опасность, что есть в любом Temporal SDK, но без
защитных механизмов. Смотрите [матрицу паритета](/parity).
:::
