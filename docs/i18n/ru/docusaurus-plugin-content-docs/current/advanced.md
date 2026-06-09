---
title: Расширенные возможности
description: Обнаружение недетерминизма, SideEffect/MutableSideEffect, GetVersion, валидаторы обновлений, отмена, co_await, Nexus, cron, parent-close-policy, асинхронное завершение, реплей-тестирование и memo.
---

# Расширенные возможности

Помимо базовой модели программирования, SDK реализует перечисленные ниже возможности, критически важные с точки зрения детерминизма и готовности к эксплуатации. Каждая из них проверяется в тестовом наборе против реального `temporal server start-dev`.

## Режим написания через `co_await` {#co-await}

Воркфлоу можно писать в стиле корутин C++20 — возвращая `workflow::workflow_task<R>` и применяя `co_await` к `Future` (затем `co_return`) вместо вызова `.Get()`. Он выполняется на **том же** стекозависимом диспетчере, что и форма обычной функции: `co_await` делегирует блокировку существующему движку, поэтому порядок испускаемых команд и реплей идентичны эквивалентному воркфлоу с `.Get()`. Отдельного планировщика нет, и детерминизм не меняется.

```cpp
#include <temporal/temporal.h>

temporal::workflow::workflow_task<std::string> CoAwaitWorkflow(
    temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);

  const std::string a = co_await ctx.ExecuteActivity<std::string>(o, "Echo", s);
  const std::string b = co_await ctx.ExecuteActivity<std::string>(o, "Echo", a + "!");
  co_return b;
}
```

Регистрируйте и реплейте его в точности как любой другой воркфлоу — `RegisterWorkflow` принимает обе формы, а записанная история воркфлоу-корутины реплеится детерминированно:

```cpp
worker.RegisterWorkflow("CoAwaitWorkflow", CoAwaitWorkflow);
// ... позже, против его реальной истории:
replayer.ReplayWorkflowHistory(history_json);  // чисто — те же команды, что и у .Get()
```

Внутри воркфлоу-корутины вы по-прежнему можете вызывать `.Get()` у future или `.Receive()` у канала; они блокируются точно так же. Используйте тот стиль, который читается лучше, — оба идентичны на проводе.

## Обнаружение недетерминизма {#non-determinism-detection}

Воркфлоу должен быть детерминированным: при реплее по записанной истории он обязан воспроизводить ровно те же оркестрирующие команды в том же порядке. Движок записывает упорядоченный поток команд, которые генерирует воркфлоу, и сопоставляет их с событиями истории, порождающими команды (по образцу `matchReplayWithHistory` из Go SDK). История является авторитетным источником; воркфлоу вправе только добавлять *дополнительные* команды в конце (реальное продвижение вперёд).

Несоответствие обрабатывается согласно `WorkflowPanicPolicy`, задаваемому на воркере:

```cpp
temporal::WorkerOptions opts;
opts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;  // по умолчанию
temporal::worker::Worker worker(client, "my-task-queue", opts);
```

- **`BlockWorkflow`** (по умолчанию) — завершить *задачу* воркфлоу с ошибкой. Сервер повторит её, поэтому развёртывание исправленного воркера восстановит воркфлоу без потери данных.
- **`FailWorkflow`** — завершить *выполнение* воркфлоу с ошибкой. Терминальное действие.

Проверка выполняется только при полном реплее истории (постоянная sticky-корутина является источником истины и повторной валидации не подвергается). Чтобы выявить недетерминированное изменение *до* развёртывания, используйте [Replay testing](#replay-testing).

## SideEffect

`SideEffect` фиксирует результат недетерминированной операции ровно один раз. При первом выполнении ваша функция вызывается, а её результат записывается в историю; при каждом последующем реплее возвращается записанное значение **без** повторного вызова функции.

```cpp
int id = ctx.SideEffect<int>([] { return generate_random_id(); });
```

Используйте его для идентификаторов, генерации случайных чисел или чтения часов — но никогда для операций с внешне наблюдаемыми эффектами (они должны выполняться внутри активности).

## MutableSideEffect

`MutableSideEffect` похож на `SideEffect`, но снабжён ключом `id`: новый маркер записывается, только когда значение *меняется* (сравнение через `operator==` или пользовательский предикат). При реплее возвращается записанное значение без вызова функции. Используйте его для значения, которое пересчитывается часто, но обычно остаётся прежним, — чтобы не писать маркер при каждом вызове.

```cpp
// Записывает маркер, только когда вычисленный tier действительно отличается от прошлого.
// Тип результата выводится из функции (без явного шаблонного аргумента).
std::string tier = ctx.MutableSideEffect("tier", [&]() -> std::string {
  return compute_tier(state);
});

// Пользовательское сравнение: значения в пределах эпсилон считаются неизменными.
double price = ctx.MutableSideEffect(
    "price",
    [&]() -> double { return quote_price(); },
    [](double a, double b) { return std::abs(a - b) < 0.01; });
```

Как и `SideEffect`, функция обязана возвращать значение (она не может быть `void`).

## Локальные активности

`ExecuteLocalActivity` выполняет *зарегистрированную* активность встроенно внутри воркера воркфлоу — без round-trip задачи активности — записывая её результат как маркер; при реплее записанный результат возвращается без повторного выполнения. Повторы происходят встроенно согласно политике повторов. Лучше всего подходит для коротких идемпотентных шагов, где накладные расходы на диспетчеризацию задачи доминировали бы. В отличие от `ExecuteActivity`, результат разрешается внутри вызова (без `Future`):

```cpp
temporal::LocalActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(5);
// retry_policy имеет тип std::optional<RetryPolicy>; оставьте его незаданным для
// поведения повторов по умолчанию.

int total = ctx.ExecuteLocalActivity<int>(opts, "Tally", items);
```

## GetVersion (версионирование / патчинг)

`GetVersion` позволяет изменять код воркфлоу, пока ещё выполняются старые экземпляры. При первом вызове выбранная версия записывается в историю; при реплее возвращается записанное значение — тем самым и старые, и новые истории остаются детерминированными.

```cpp
int v = ctx.GetVersion("greeting-change", temporal::workflow::kDefaultVersion, 1);
if (v == temporal::workflow::kDefaultVersion) {
  // исходное поведение — для экземпляров, стартовавших до этого изменения
  greet_v0(ctx);
} else {
  // новое поведение
  greet_v1(ctx);
}
```

`kDefaultVersion` (-1) возвращается при реплее истории, записанной *до* появления вызова `GetVersion`. Как только все экземпляры, стартовавшие до изменения, завершатся, можно убрать старую ветку и повысить `min_supported`.

## Валидаторы обновлений {#update-validators}

Обработчик обновления (update handler) может принимать необязательный **валидатор только для чтения**, выполняемый *до* того, как обновление будет принято. Если валидатор выбрасывает исключение, обновление отклоняется и обработчик никогда не запускается — а поскольку отклонение **не записывается в историю**, состояние воркфлоу остаётся нетронутым.

```cpp
ctx.SetUpdateHandler(
    "deposit",
    [&](int amount) { balance += amount; return balance; },  // обработчик
    [](int amount) {                                          // валидатор
      if (amount <= 0) {
        throw temporal::ApplicationError("amount must be positive", "InvalidDeposit");
      }
    });
```

На стороне клиента отклонённое обновление проявляется как выброшенное исключение:

```cpp
handle.Update<int>("deposit", -5);  // бросает исключение — валидатор отклонил запрос
```

Валидаторы должны быть доступны только для чтения (никаких активностей, таймеров или мутации состояния) — в точности как обработчики запросов (query handlers).

## Отмена операций

Фьючерсы операций предоставляют метод `Cancel()`. Для **таймера** отмена генерирует команду `CancelTimer` и немедленно разрешает фьючерс, так что ожидающий выполнения код разблокируется сразу, не дожидаясь истечения таймера:

```cpp
auto timer = ctx.NewTimer(std::chrono::minutes(5));
// ... произошло что-то другое ...
timer.Cancel();   // воркфлоу не будет ждать полные 5 минут
timer.Get();      // возвращается немедленно
```

Отмена детерминирована — при реплее воркфлоу воспроизводит команду `CancelTimer`.

Чтобы **отреагировать** на отмену на уровне воркфлоу (паттерн «прибраться и остановиться»), ожидайте `ctx.AwaitCancellation()` как случай (case) в `Selector`, конкурируя с вашей работой:

```cpp
auto work = ctx.NewTimer(std::chrono::minutes(5));
std::string out;
temporal::workflow::Selector sel(ctx);
sel.AddFuture(work, [&] { out = "done"; });
sel.AddFuture(ctx.AwaitCancellation(), [&] {
  work.Cancel();
  out = "cancelled";
});
sel.Select();
```

Когда воркфлоу отменяют, `AwaitCancellation` завершается, селектор выбирает эту ветку, и воркфлоу отменяет свой таймер и быстро завершается.

**Активности** отменяются точно так же: вызовите `Cancel()` у `Future` активности, чтобы сгенерировать `RequestCancelActivityTask`. Активность с heartbeat видит запрос через `activity::Context::IsCancelled()` и быстро завершается:

```cpp
std::string MyActivity(temporal::activity::Context& ctx, int n) {
  for (int i = 0; i < n; ++i) {
    do_some_work();
    ctx.RecordHeartbeat(i);
    if (ctx.IsCancelled()) return "cancelled";  // увидели запрос на отмену
  }
  return "done";
}
```

Отмена доставляется через heartbeat, поэтому увидеть её может только активность с heartbeat.

## Сигналы и отмена внешних воркфлоу {#external-workflows}

Воркфлоу может обратиться к *другому* выполняющемуся воркфлоу по его id — fire-and-forget, без необходимости в handle. `SignalExternalWorkflow` кодирует свои аргументы через конвертер данных и доставляет сигнал; `CancelExternalWorkflow` запрашивает отмену цели.

```cpp
// Доставить сигнал "setName" другому воркфлоу по id.
ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));

// Запросить отмену другого воркфлоу по id.
ctx.CancelExternalWorkflow(target_id);
```

Это же — стандартный способ послать сигнал и **дочернему** воркфлоу: передайте id дочернего воркфлоу (задайте `ChildWorkflowOptions::id` при его запуске или считайте его обратно) в `SignalExternalWorkflow`. Handle дочернего воркфлоу, возвращаемый `ExecuteChildWorkflow`, является `Future` — он несёт `Get()` и `Cancel()`, а не метод сигнала.

## Replay testing {#replay-testing}

Выполните реплей записанной истории по текущему коду воркфлоу — **без запуска сервера** — чтобы выявить недетерминированное изменение до того, как оно попадёт в продакшн. Это самый ценный юнит-тест воркфлоу, который только можно написать.

```cpp
// Экспортируйте реальную историю (например, из только что запущенного воркфлоу или
// `temporal workflow show -o json`):
std::string history_json = handle.FetchHistoryJson();

// Выполните реплей по зарегистрированному воркфлоу; при недетерминизме — исключение.
temporal::worker::Worker replayer(client, "replay");
replayer.RegisterWorkflow("MyWorkflow", MyWorkflow);
replayer.ReplayWorkflowHistory(history_json);  // бросает исключение, если MyWorkflow отклонился
```

Храните несколько репрезентативных историй в виде тестовых фикстур и воспроизводите их в CI; любое несовместимое изменение воркфлоу (переупорядоченная активность, удалённый таймер) приведёт к провалу теста вместо падения воркфлоу в продакшне.

## Nexus-операции {#nexus}

:::tip
Полное руководство: **[Операции Nexus](/nexus)** — эндпоинты, обработчик на воркере и полный сквозной пример.
:::

Nexus позволяет одному воркфлоу вызывать операцию, обслуживаемую воркером другой команды — возможно, в другом namespace — без привязки к типам воркфлоу этой команды. SDK поддерживает полный round-trip: воркер регистрирует обработчик операции, клиент регистрирует endpoint, маршрутизирующий к нему, а воркфлоу вызывает его.

Зарегистрируйте обработчик `R Fn(Arg)` для пары `(service, operation)` на том воркере, который должен её обслуживать. В отличие от активности, Nexus-операция принимает **одно** входное значение и возвращает **одно** значение (каждое — `Payload`):

```cpp
// Обработчик: R Fn(Arg) — один вход, один результат.
std::string Greet(std::string name) { return "Hello, " + name; }

worker.RegisterNexusOperation("greeting", "say-hello", Greet);
```

Зарегистрируйте endpoint на клиенте. Он именует целевой воркер — очередь задач (в namespace этого клиента), воркер которой обслуживает операцию, — и возвращает id нового endpoint:

```cpp
std::string endpoint_id =
    client.CreateNexusEndpoint("my-endpoint", "nexus-handler-tq");

// Поиск / перечисление зарегистрированных endpoint:
temporal::client::NexusEndpointDescription d = client.GetNexusEndpoint(endpoint_id);
std::vector<std::string> names = client.ListNexusEndpoints();
```

Вызовите операцию из воркфлоу. `ExecuteNexusOperation<R, Arg>` принимает *имя* endpoint, сервис, операцию и единственный вход; необязательный `schedule_to_close` ограничивает весь вызов (`0`/по умолчанию = значение по умолчанию на сервере). Он возвращает `Future<R>`:

```cpp
std::string WorkflowUsingNexus(temporal::workflow::Context& ctx, std::string who) {
  return ctx.ExecuteNexusOperation<std::string, std::string>(
      "my-endpoint",   // имя endpoint
      "greeting",      // сервис
      "say-hello",     // операция
      who              // единственное входное значение
  ).Get();             // или: co_await ... в стиле корутин
}
```

:::note
Dev-сервер принимает RPC управления endpoint (и диспетчеризирует Nexus-операции) только при включённом Nexus, например
`temporal server start-dev --dynamic-config-value system.enableNexus=true`.
Иначе `CreateNexusEndpoint` выбрасывает `RpcError`.
:::

## Cron / календарные расписания {#cron}

`Client::CreateSchedule` запускает воркфлоу по периодической спецификации. Наряду с фиксированным `interval` расписание может нести одно или несколько **cron / календарных выражений** в `ScheduleOptions::cron_expressions`: стандартный cron из 5 полей, опционально с ведущим полем секунд и/или завершающим `CRON_TZ=<zone>`. Каждая строка — это один триггер; комбинируйте их с `interval` или используйте по отдельности.

```cpp
temporal::ScheduleOptions opts;
opts.cron_expressions = {"0 9 * * MON-FRI"};  // по будням в 09:00
opts.workflow_type = "ReportWorkflow";
opts.task_queue = "reports";

client.CreateSchedule("daily-report", opts);
bool exists = client.DescribeSchedule("daily-report");
client.DeleteSchedule("daily-report");
```

```cpp
// Поле секунд + таймзона: каждые 30 минут, вычисляется в Europe/Berlin.
opts.cron_expressions = {"0 0,30 * * * * CRON_TZ=Europe/Berlin"};
```

## Дочерние воркфлоу: политика закрытия родителя {#parent-close-policy}

`ChildWorkflowOptions::parent_close_policy` определяет, что происходит с *выполняющимся* дочерним воркфлоу при закрытии родителя:

- **`ParentClosePolicy::Terminate`** (по умолчанию) — сервер завершает дочерний воркфлоу.
- **`ParentClosePolicy::Abandon`** — дочерний воркфлоу продолжает работать независимо.
- **`ParentClosePolicy::RequestCancel`** — у дочернего воркфлоу запрашивается отмена.

```cpp
std::string StartAbandonedChild(temporal::workflow::Context& ctx,
                                std::string child_id) {
  temporal::ChildWorkflowOptions co;
  co.id = child_id;
  co.parent_close_policy = temporal::ParentClosePolicy::Abandon;

  // Fire-and-forget: запускаем дочерний воркфлоу и возвращаемся, не ожидая его. При
  // Abandon дочерний воркфлоу переживает этого родителя.
  ctx.ExecuteChildWorkflow<std::string>(co, "LongChild");
  return "parent-done";
}
```

Чтобы послать сигнал дочернему воркфлоу позже, используйте его id с `SignalExternalWorkflow` (см. [Сигналы и отмена внешних воркфлоу](#external-workflows)).

## Асинхронное / ручное завершение активности {#async-completion}

Активность может завершиться *вне основного потока*: вместо возврата результата она откладывает завершение и передаёт свой токен задачи какой-либо внешней системе (callback webhook, подтверждение человеком, другой сервис), которая позже завершает её через клиент.

Вызовите `defer_completion()` в активности — это помечает задачу как завершающуюся асинхронно и возвращает токен задачи. Собственное возвращаемое значение активности затем игнорируется; `Future` воркфлоу остаётся в ожидании, пока токен не будет разрешён:

```cpp
std::string ApprovalActivity(temporal::activity::Context& ctx, std::string req) {
  const std::string token = ctx.defer_completion();  // асинхронно; возвращает токен задачи
  enqueue_for_human_review(req, token);              // передаём его в другое место
  return {};                                          // игнорируется — завершено позже
}
```

Позже — из любого места, где есть `Client` и токен, — завершите её успехом или ошибкой:

```cpp
// Успех: закодированный результат — это то, что выдаёт Future воркфлоу.
client.CompleteActivity(token, std::string("approved"));

// Или ошибка (сообщение + необязательный тип ошибки):
client.FailActivity(token, "rejected by reviewer", "ApprovalRejected");
```

Токен — это `activity::Context::GetInfo().task_token`; `defer_completion()` — это сокращение в один вызов для `SetWillCompleteAsync()` + чтение этого токена.

## Search attributes, memo и сессии {#visibility-sessions}

Воркфлоу может в рантайме обновлять (upsert) **индексируемые search attributes** для запросов видимости (`Client::ListWorkflows` / `CountWorkflows`). Стройте типизированные значения с помощью хелперов `temporal::sa::`; именованный атрибут должен быть зарегистрирован в namespace.

```cpp
ctx.UpsertSearchAttributes({{"Tier", temporal::sa::Keyword("gold")}});
```

Search attributes можно также задать при старте (`StartWorkflowOptions::search_attributes`) тем же способом; неиндексируемые метаданные помещаются в `memo` (см. [Memo & Describe](#memo--describe)).

**Сессии** закрепляют последовательность активностей за одним хостом-воркером — полезно, когда они делят локальное для хоста состояние (скачанный файл, прогретый кэш). Создайте сессию, планируйте активности на возвращённой очереди задач, затем завершите её, чтобы освободить слот. Воркер должен быть запущен с `WorkerOptions::enable_sessions = true`.

```cpp
auto session = ctx.CreateSession();           // резервирует слот на каком-то хосте
temporal::ActivityOptions opts;
opts.task_queue = session.task_queue;         // закрепляем за этим хостом
opts.start_to_close_timeout = std::chrono::seconds(30);
ctx.ExecuteActivity<void>(opts, "Download", url).Get();
ctx.ExecuteActivity<std::string>(opts, "Process").Get();  // тот же хост
ctx.CompleteSession(session);                 // освобождаем слот
```

## Memo & Describe {#memo--describe}

Прикрепите неиндексируемые метаданные к воркфлоу при старте и считайте их снимок в произвольный момент времени:

```cpp
temporal::StartWorkflowOptions o;
o.task_queue = "my-task-queue";
o.memo["owner"] = dc->ToPayload(std::string("alice"));
auto handle = client.StartWorkflow(o, "MyWorkflow");

temporal::client::WorkflowDescription d = handle.Describe();
// d.status == "RUNNING", d.run_id, d.memo["owner"] -> "alice"
```

`Describe()` возвращает идентификатор воркфлоу, run id, статус (например, `RUNNING`, `COMPLETED`) и memo. Memo не индексируется для поиска; типизированные/индексируемые search attributes ещё не реализованы.
