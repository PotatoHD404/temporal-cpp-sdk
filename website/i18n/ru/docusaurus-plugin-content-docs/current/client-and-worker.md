---
title: Клиент и воркер
description: Подключение клиента, запуск воркфлоу и запуск воркеров.
---

# Клиент и воркер

## Клиент

Подключение к Temporal frontend:

```cpp
temporal::ClientOptions opts;
opts.target = "localhost:7233";   // host:port
opts.ns = "default";              // Temporal namespace
auto client = temporal::client::Client::Connect(opts);
```

```cpp
struct ClientOptions {
  std::string target = "localhost:7233";
  std::string ns = "default";
  std::string identity;                            // default: "<pid>@<host>"
  std::shared_ptr<log::Logger> logger;             // default: console logger
  std::shared_ptr<DataConverter> data_converter;   // default: JSON converter
};
```

:::note
TLS/mTLS и аутентификация по API-ключу **пока не поддерживаются** — соединения устанавливаются без шифрования. См.
[матрицу паритета](/parity).
:::

### Запуск воркфлоу

```cpp
temporal::StartWorkflowOptions wf;
wf.id = "order-123";              // optional; default is a random UUID
wf.task_queue = "orders";        // required
wf.run_timeout = std::chrono::hours(1);   // optional

auto handle = client.StartWorkflow(wf, "OrderWorkflow", order_id, amount);
```

`StartWorkflow(options, workflow_type, args...)` возвращает `WorkflowHandle`.

### Работа с дескриптором

```cpp
handle.id();                                  // workflow id
handle.run_id();                              // run id

R result = handle.Result<R>();                // block until close; follows continue-as-new
R answer = handle.Query<R>("queryName", a);   // synchronous query
R out    = handle.Update<R>("updateName", a); // synchronous update

handle.Signal("signalName", payloads);        // fire-and-forget
handle.Cancel();                              // request cancellation
handle.Terminate("reason");                   // force-terminate
```

`Result<R>()` выполняет long-poll истории событий воркфлоу в ожидании события завершения и выбрасывает
`temporal::WorkflowFailedError`, если воркфлоу завершился с ошибкой, превысил таймаут, был принудительно
остановлен или отменён. Дескриптор существующего выполнения можно получить через
`client.GetHandle(workflow_id, run_id)`.

## Воркер

Воркер опрашивает очередь задач и передаёт управление зарегистрированным функциям:

```cpp
temporal::worker::Worker worker(client, "orders");
worker.RegisterWorkflow("OrderWorkflow", OrderWorkflow);
worker.RegisterActivity("ChargeCard", ChargeCard);

worker.Start();   // non-blocking: spawns poller threads
// ... run your program ...
worker.Stop();    // signal pollers to stop and join

// or, for a long-running worker process:
worker.Run();     // blocks until SIGINT/SIGTERM, then stops
```

Функции регистрируются по имени и могут быть любым обычным callable вида
`R(workflow::Context&, Args...)` / `R(activity::Context&, Args...)` — аргументы декодируются, а
результат кодируется автоматически с помощью [конвертера данных](/data-conversion).

### Наблюдаемость sticky-кэша

Воркер держит выполняющиеся воркфлоу в памяти ([sticky-кэш](/architecture)). Вы можете узнать,
сколько задач было обработано как продолжения из кэша, а сколько потребовали полного реплея:

```cpp
worker.cache_hits();   // continuation tasks served from the in-memory cache
worker.replays();      // full-history replays (first tasks + cache misses)
```

```cpp
struct WorkerOptions {
  int max_concurrent_activities = 0;      // 0 => library default
  int max_concurrent_workflow_tasks = 0;
  int workflow_task_pollers = 1;
  int activity_task_pollers = 1;
};
```
