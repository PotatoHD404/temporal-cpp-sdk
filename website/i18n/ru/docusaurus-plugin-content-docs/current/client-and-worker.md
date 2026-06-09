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
  std::string identity;                            // по умолчанию: "<pid>@<host>"
  std::shared_ptr<log::Logger> logger;             // по умолчанию: консольный логгер
  std::shared_ptr<DataConverter> data_converter;   // по умолчанию: JSON-конвертер
  TlsConfig tls;                                   // по умолчанию отключён (незащищённый канал)
  std::string api_key;                             // передаётся как "Authorization: Bearer <key>" на каждый RPC
  std::vector<std::shared_ptr<interceptor::Interceptor>> interceptors;  // цепочка client-outbound
};
```

Для защищённого соединения заполните `tls` (PEM-*содержимое*, а не пути к файлам — для
взаимного TLS задайте также `client_cert` + `client_key`) и/или `api_key`:

```cpp
opts.tls.enabled = true;
opts.tls.server_ca_cert = ca_pem;   // пусто => системное хранилище доверенных сертификатов
opts.api_key = std::getenv("TEMPORAL_API_KEY");
```

:::note
TLS/mTLS и аутентификация по API-ключу подключены (gRPC `SslCredentials` и метаданные-заголовок
`Bearer`) и проверены юнит-тестами против внутрипроцессного TLS-сервера, но **ещё не проверены
против реального Temporal-сервера**. См. [матрицу паритета](/parity).
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

R result = handle.Result<R>();                // блокировка до завершения; следует за continue-as-new
R answer = handle.Query<R>("queryName", a);   // синхронный query (аргументы кодируются за вас)
R out    = handle.Update<R>("updateName", a); // синхронный update

handle.Signal("signalName", a, b);            // fire-and-forget; аргументы кодируются вариативно
handle.Cancel();                              // запрос отмены
handle.Terminate("reason");                   // принудительное завершение
```

`Result<R>()` выполняет long-poll истории событий воркфлоу в ожидании события завершения и выбрасывает
`temporal::WorkflowFailedError`, если воркфлоу завершился с ошибкой, превысил таймаут, был принудительно
остановлен или отменён. Дескриптор существующего выполнения можно получить через
`client.GetHandle(workflow_id, run_id)`. Вызов, нацеленный на неизвестный workflow id, выбрасывает
`temporal::NotFoundError` (подтип `RpcError`, у которого `not_found()` равно `true`).

### Типизированные дескрипторы signal / query / update

Объявите типизированную ссылку один раз — и имя на проводе, тип payload-а и тип результата
проверяются и выводятся автоматически, без повторного указания строки или явного `<R>`:

```cpp
inline constexpr temporal::SignalRef<bool> kStop{"stop"};
inline constexpr temporal::QueryRef<int>   kSum{"sum"};
inline constexpr temporal::UpdateRef<int>  kBump{"bump"};

int sum   = handle.Query(kSum);        // тип результата int выводится
int after = handle.Update(kBump, 5);   // тип аргумента и результата проверяется
handle.Signal(kStop, true);            // тип payload-а bool проверяется
```

Они сводятся к тем же строковым именам, что и нетипизированные вызовы, поэтому обе формы
совместимы и воспроизводятся идентично.

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

Для активностей можно также зарегистрироваться через типизированный дескриптор, чтобы имя типа
не могло разойтись с местом вызова (`TEMPORAL_ACTIVITY(fn)` объявляет `fn_activity`):

```cpp
TEMPORAL_ACTIVITY(ChargeCard);          // в области имён, рядом с функцией
// ...
worker.Register(ChargeCard_activity);   // имя берётся из дескриптора
```

Зарегистрируйте обработчик Nexus-операции `R Fn(Arg)` для пары `(service, operation)`
(Nexus-операция принимает один вход и возвращает одно значение):

```cpp
worker.RegisterNexusOperation("payments", "charge", ChargeOperation);
```

### Наблюдаемость sticky-кэша

Воркер держит выполняющиеся воркфлоу в памяти ([sticky-кэш](/architecture)). Вы можете узнать,
сколько задач было обработано как продолжения из кэша, а сколько потребовали полного реплея:

```cpp
worker.cache_hits();   // continuation tasks served from the in-memory cache
worker.replays();      // full-history replays (first tasks + cache misses)
```

```cpp
struct WorkerOptions {
  int max_concurrent_activities = 0;      // 0 => значение библиотеки по умолчанию
  int max_concurrent_workflow_tasks = 0;
  int workflow_task_pollers = 1;
  int activity_task_pollers = 1;
  // Если > 0, задача воркфлоу, превысившая этот дедлайн (блокирующий вызов или
  // неуступающий цикл в коде воркфлоу), ПРЕРЫВАЕТСЯ: задача завершается с ошибкой,
  // чтобы сервер повторил её, и воркер продолжает обслуживать другие воркфлоу,
  // а не зависает на застрявшей задаче.
  std::chrono::milliseconds deadlock_detection_timeout{0};
  // ... плюс ограничения конкурентности, panic-политика, граница sticky-кэша, метрики,
  // interceptors, автомасштабирование поллеров, сессии — см. <temporal/common/options.h>.
};
```
