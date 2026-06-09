---
title: Операции Nexus
description: Межсервисные операции — управление эндпоинтами, обработчик на воркере и детерминированный вызов из воркфлоу.
---

# Операции Nexus

Nexus позволяет одному воркфлоу вызвать операцию, обслуживаемую **другим** воркером —
возможно, принадлежащим иной команде и работающим в другом неймспейсе — без привязки
к типам воркфлоу этой команды. Вызывающей стороне известны три строки (эндпоинт,
сервис, операция) и форма входа/результата; всё остальное скрыто за эндпоинтом. Вызов
долговечен и безопасен для реплея — в точности как активность.

SDK поддерживает полный round-trip:

1. **Воркер** регистрирует обработчик операции.
2. **Клиент** регистрирует **эндпоинт**, маршрутизирующий к очереди задач этого воркера.
3. **Воркфлоу** вызывает операцию через эндпоинт.

См. [Концепции → Nexus](/concepts#nexus), чтобы понять, где это находится относительно
активностей и дочерних воркфлоу, и [Клиент и воркер](/client-and-worker) — про настройку
клиента и воркера, используемую во всех примерах ниже.

## Эндпоинт против сервиса против операции

- **Эндпоинт** — серверная регистрация (создаётся через клиент), именующая **цель**
  воркера: неймспейс + очередь задач, воркер которой обрабатывает входящие Nexus-задачи.
  Вызывающие стороны адресуют эндпоинт по его уникальному **имени**.
- **Сервис** — неймспейс для связанных операций, выбираемый обработчиком (обычная
  строка, например `"greeting"`).
- **Операция** — одна именованная операция внутри сервиса (например, `"say-hello"`),
  реализуемая одной функцией-обработчиком.

Вызывающая сторона маршрутизирует по `(endpoint, service, operation)`; эндпоинт решает,
*какой воркер*, а зарегистрированная у воркера пара `(service, operation)` решает,
*какой обработчик*.

## Включение Nexus на dev-сервере

Nexus закрыт серверным feature-флагом. Запускайте dev-сервер с включённым флагом:

```bash
temporal server start-dev --dynamic-config-value system.enableNexus=true
```

:::note
Без `system.enableNexus=true` RPC управления эндпоинтами отклоняются, а Nexus-операции
не диспетчеризируются. `CreateNexusEndpoint` (и аналогичные) в этом случае выбрасывают
`temporal::RpcError` — обработайте его, если хотите аккуратно пропустить шаг, а не
упасть.
:::

## Управление эндпоинтами (клиент)

Реестр эндпоинтов принадлежит клиенту (это RPC `OperatorService` — они лишь
регистрируют/описывают/перечисляют эндпоинты; сам вызов операции и обработчик живут на
контексте воркфлоу и на воркере соответственно).

```cpp
// Register an endpoint named "my-endpoint" whose target is the worker polling
// "nexus-handler-tq" in this client's namespace. Returns the new endpoint id.
std::string endpoint_id =
    client.CreateNexusEndpoint("my-endpoint", "nexus-handler-tq");

// Describe a single endpoint by its server-assigned id.
temporal::client::NexusEndpointDescription d = client.GetNexusEndpoint(endpoint_id);
// d.id              — server-assigned, opaque
// d.name            — "my-endpoint"
// d.target_namespace
// d.target_task_queue == "nexus-handler-tq"

// Enumerate every registered endpoint by name (pages through results internally).
std::vector<std::string> names = client.ListNexusEndpoints();
```

`NexusEndpointDescription` выглядит так:

```cpp
struct NexusEndpointDescription {
  std::string id;                 // server-assigned endpoint id (opaque)
  std::string name;               // unique endpoint name
  std::string target_namespace;   // worker target: namespace that handles ops
  std::string target_task_queue;  // worker target: task queue that handles ops
};
```

:::note
Создание эндпоинта согласовано лишь в конечном счёте: эндпоинт может не появиться в
`ListNexusEndpoints()` в тот же миг, когда `CreateNexusEndpoint` вернул управление, даже
если `GetNexusEndpoint(id)` уже его разрешает. Сделайте короткий опрос, если перечисляете
сразу после создания.
:::

## Реализация обработчика (воркер)

Зарегистрируйте обработчик `R Fn(Arg)` для пары `(service, operation)` на том воркере,
который должен её обслуживать. **В отличие от активности**, Nexus-операция принимает
*одно* входное значение и возвращает *одно* значение (каждое кодируется в один
`Payload`), а обработчик — это обычная функция: параметра `Context` нет:

```cpp
// Handler: R Fn(Arg) — one input, one result.
std::string Greet(std::string name) { return "Hello, " + name; }

temporal::worker::Worker worker(client, "nexus-handler-tq");
worker.RegisterNexusOperation("greeting", "say-hello", Greet);
worker.Start();
```

Обработчик выполняется на том воркере, который опрашивает **целевую очередь задач**
эндпоинта (здесь `"nexus-handler-tq"`). Он диспетчеризируется синхронно и завершает
операцию встроенно — верните результат (или выбросьте исключение, чтобы провалить
операцию).

Обработчик, возвращающий `void`, допустим (он порождает пустой результат):

```cpp
void Audit(std::string event) { /* side effect, no return value */ }
worker.RegisterNexusOperation("audit", "record", Audit);
```

:::note
Сигнатура обработчика проверяется на этапе компиляции: он обязан принимать **ровно один**
аргумент. `R Fn()` или `R Fn(A, B)` не скомпилируется.
:::

## Вызов операции (воркфлоу)

Изнутри воркфлоу вызовите операцию по **имени эндпоинта**:

```cpp
template <class R, class Arg>
Future<R> Context::ExecuteNexusOperation(
    const std::string& endpoint, const std::string& service,
    const std::string& operation, const Arg& input,
    std::chrono::nanoseconds schedule_to_close = {});
```

`Arg` выводится из `input`, поэтому на практике вы указываете только `R`:

```cpp
std::string WorkflowUsingNexus(temporal::workflow::Context& ctx, std::string who) {
  return ctx.ExecuteNexusOperation<std::string>(
      "my-endpoint",   // endpoint NAME (not id)
      "greeting",      // service
      "say-hello",     // operation
      who              // single input value
  ).Get();             // block on the Future; or co_await in coroutine style
}
```

Передайте явный `schedule_to_close`, чтобы ограничить весь вызов; по умолчанию (`{}` /
`0`) используется значение по умолчанию на сервере:

```cpp
using namespace std::chrono_literals;
auto result = ctx.ExecuteNexusOperation<std::string>(
    "my-endpoint", "greeting", "say-hello", who, /*schedule_to_close=*/30s).Get();
```

Он возвращает `Future<R>`. Как и с активностями и дочерними воркфлоу, вы можете сохранить
`Future` и вызвать `.Get()` позже (или `co_await` его в воркфлоу-корутине), чтобы тем
временем выполнять работу параллельно.

### Детерминизм

`ExecuteNexusOperation` — это записываемая **команда** воркфлоу, а не побочный эффект.
Планирование записывается в историю (`NexusOperationScheduled`), а результат считывается
из истории (`NexusOperationCompleted`) при реплее — обработчик **не** запускается
повторно во время реплея, в точности как результат активности. Поэтому вызов безопасен
для реплея; вы можете вызывать его прямо из кода воркфлоу.

## Сквозной пример

Воркер, который одновременно **обслуживает** операцию и **выполняет вызывающий
воркфлоу** (эндпоинт нацелен на ту же очередь задач), затем клиент, который создаёт
эндпоинт и запускает воркфлоу. Это зеркалит интеграционный тест в
`tests/integration/nexus_operation_edge_test.cpp`.

```cpp
#include <string>
#include <temporal/temporal.h>

// 1. The Nexus operation handler: R Fn(Arg) — one input, one result, no Context.
std::string GreetOp(std::string name) { return "hello " + name; }

// 2. The caller workflow: invokes the operation on `endpoint` and returns its
//    result. Passing the endpoint name as input lets the caller pick it at runtime.
std::string CallerWorkflow(temporal::workflow::Context& ctx, std::string endpoint) {
  return ctx.ExecuteNexusOperation<std::string>(
      endpoint, "svc", "op", std::string("world")).Get();
}

int main() {
  temporal::ClientOptions opts;
  opts.target = "localhost:7233";
  opts.ns = "default";
  auto client = temporal::client::Client::Connect(opts);

  const std::string task_queue = "nexus-handler-tq";
  const std::string endpoint = "demo-endpoint";

  // 3. Create the endpoint targeting the worker's task queue. Requires Nexus to be
  //    enabled on the server, otherwise this throws RpcError.
  client.CreateNexusEndpoint(endpoint, task_queue);

  // 4. One worker serves the operation AND runs the caller workflow.
  temporal::worker::Worker worker(client, task_queue);
  worker.RegisterWorkflow("CallerWorkflow", CallerWorkflow);
  worker.RegisterNexusOperation("svc", "op", GreetOp);
  worker.Start();  // non-blocking

  // 5. Start the workflow; the result flows back from the handler.
  temporal::StartWorkflowOptions wo;
  wo.task_queue = task_queue;
  auto handle = client.StartWorkflow(wo, "CallerWorkflow", endpoint);
  std::string result = handle.Result<std::string>();  // "hello world"

  worker.Stop();
  return 0;
}
```

В реальном кросс-командном развёртывании воркер-обработчик, регистрация эндпоинта и
вызывающий воркфлоу живут в **разных** процессах (а часто и в разных неймспейсах);
`target_task_queue` эндпоинта — единственное, что связывает вызывающую сторону с
обработчиком.

## Ограничения

Что SDK реализует на сегодня, честно:

- **Только синхронный запуск операции.** Обработчик выполняется встроенно и завершает
  операцию немедленно; модели асинхронной / долгоработающей Nexus-операции нет (нет
  токена операции, нет отдельного колбэка завершения).
- **Один вход, один результат.** Обработчик — это `R Fn(Arg)`: ровно один аргумент и одно
  возвращаемое значение, каждое — один `Payload`. Это уже, чем у активности (которая
  принимает несколько аргументов).
- **Нет `Context` у обработчика.** Обработчик — обычная функция; контекста Nexus-операции,
  из которого можно прочитать метаданные, заголовки или сигнал отмены, нет.
- Управление эндпоинтами покрывает create / get (describe) / list. Вызовов
  update-endpoint или delete-endpoint в этом SDK нет.

См. [матрицу паритета](/parity), чтобы понять, как эта строка соотносится с официальными
SDK.
