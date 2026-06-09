---
title: Паттерны и рецепты
description: Самодостаточные C++ рецепты для наиболее распространённых паттернов воркфлоу Temporal, основанные на реальном API SDK.
---

# Паттерны и рецепты

Каждый рецепт ниже можно скопировать и вставить — он компилируется против реального API SDK.
Набор интеграционных тестов (`tests/integration/integration_test.cpp`) является
авторитетным источником; представленные паттерны адаптированы непосредственно из него.

Основы описаны в разделах [Активности и таймеры](workflows/overview.md),
[Сигналы, запросы и обновления](workflows/messaging.md),
[Селекторы, дочерние воркфлоу и другое](workflows/composition.md), а также
[Продвинутые возможности](advanced.md).

:::note
Здесь рассматриваются только возможности, отмеченные ✅ в [матрице паритета](parity.md).
Паттерны, требующие нереализованных функций (автоматическое распространение отмены
на дочерние воркфлоу и т. п.), намеренно опущены.
:::

---

## 1. Последовательная цепочка активностей

Запуск последовательности активностей одна за другой с передачей результата каждой
в следующую.

```cpp
#include <temporal/temporal.h>

// Активность: увеличивает целое число на единицу.
int AddOneActivity(temporal::activity::Context&, int n) { return n + 1; }

// Воркфлоу: последовательно выполняет активность N раз.
int ChainWorkflow(temporal::workflow::Context& ctx, int steps) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);

  int value = 0;
  for (int i = 0; i < steps; ++i) {
    // Планируем и сразу ожидаем: следующая итерация начинается
    // только после завершения предыдущей активности.
    value = ctx.ExecuteActivity<int>(opts, "AddOne", value).Get();
  }
  return value;  // равно `steps` после N вызовов "+1"
}
```

Регистрация и запуск:

```cpp
worker.RegisterWorkflow("ChainWorkflow", ChainWorkflow);
worker.RegisterActivity("AddOne", AddOneActivity);

temporal::StartWorkflowOptions o;
o.task_queue = "my-queue";
auto handle = client.StartWorkflow(o, "ChainWorkflow", 5);
int result = handle.Result<int>();  // 5
```

---

## 2. Параллельный fan-out / fan-in

Планируйте несколько активностей *до* того, как ожидать какую-либо из них, чтобы
они выполнялись параллельно. Собирайте все результаты после их завершения.

```cpp
int ParallelWorkflow(temporal::workflow::Context& ctx, int base) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);

  // Fan-out: все три активности планируются в одной задаче воркфлоу.
  auto f1 = ctx.ExecuteActivity<int>(opts, "AddOne", base);
  auto f2 = ctx.ExecuteActivity<int>(opts, "AddOne", base + 10);
  auto f3 = ctx.ExecuteActivity<int>(opts, "AddOne", base + 100);

  // Fan-in: каждый Get() ждёт, пока конкретный future не будет готов.
  return f1.Get() + f2.Get() + f3.Get();
}
```

:::tip
Планирование N активностей перед вызовом `Get()` — правильный способ добиться
параллелизма: SDK записывает все N команд планирования в одной задаче воркфлоу,
и сервер может диспетчеризировать их параллельно.
:::

---

## 3. Гонка активности и таймаута с `Selector`

Используйте `Selector`, чтобы продолжить работу с тем, что завершится первым:
результатом активности или таймером дедлайна. Это канонический паттерн «вызов с дедлайном».

```cpp
#include <temporal/workflow/selector.h>

std::string SlowActivity(temporal::activity::Context&, int sleep_ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
  return "done";
}

std::string WithTimeoutWorkflow(temporal::workflow::Context& ctx,
                                int activity_ms, int timeout_ms) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(30);

  // Планируем оба; ни один не блокирует выполнение.
  auto activity = ctx.ExecuteActivity<std::string>(opts, "SlowActivity", activity_ms);
  auto deadline = ctx.NewTimer(std::chrono::milliseconds(timeout_ms));

  std::string result;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(activity, [&](std::string r) {
    result = "activity: " + r;
  });
  sel.AddFuture(deadline, [&]() {
    result = "timed out";
  });
  sel.Select();  // блокирует до тех пор, пока первый готовый case не вызовет свой обработчик
  return result;
}
```

Оба future планируются до вызова `Select()`, поэтому гонка настоящая.
Проигравший future продолжает выполняться в фоне; если вы хотите его отменить,
вызовите `activity.Cancel()` или `deadline.Cancel()` внутри обработчика победителя.

---

## 4. Долго живущий воркфлоу-«сущность»

Принимает сигналы для накопления состояния, отвечает на запросы наблюдателей и
завершается корректно по сигналу-часовому. Это паттерн entity/actor в Temporal.

```cpp
int EntityWorkflow(temporal::workflow::Context& ctx) {
  int balance = 0;

  // Предоставляем доступ к текущему состоянию для вызывающих только чтение —
  // никаких активностей и мутаций состояния внутри обработчика запроса.
  ctx.SetQueryHandler("balance", [&]() -> int { return balance; });

  // Принимаем обновления "deposit", которые мутируют состояние и возвращают новый баланс.
  ctx.SetUpdateHandler("deposit", [&](int amount) -> int {
    balance += amount;
    return balance;
  });

  // Обрабатываем сигналы "credit" (fire-and-forget) для случаев, когда вызывающему
  // не нужно синхронное подтверждение.
  auto credits = ctx.GetSignalChannel<int>("credit");

  // Сигнал "close" завершает цикл.
  auto close = ctx.GetSignalChannel<std::string>("close");

  while (true) {
    // Неблокирующий сброс очереди кредитных сигналов перед проверкой close.
    int c = 0;
    while (credits.ReceiveAsync(c)) {
      balance += c;
    }

    std::string cmd;
    if (close.ReceiveAsync(cmd)) {
      break;
    }

    // Ничего не готово — паркуемся до следующего сигнала или обновления.
    // Используем очень короткий таймер как точку выхода планировщика, чтобы
    // он мог обработать буферизованные сигналы, поступившие до этой задачи.
    ctx.Sleep(std::chrono::milliseconds(10));
  }

  return balance;
}
```

Со стороны клиента:

```cpp
auto dc = temporal::DataConverter::Default();

// Мутирующий вызов — блокируется до применения обновления.
int new_balance = handle.Update<int>("deposit", 100);

// Сигнал — fire-and-forget.
handle.Signal("credit", dc->ToPayloads(50));

// Запрос только для чтения к живому состоянию.
int snapshot = handle.Query<int>("balance");

// Завершаем воркфлоу.
handle.Signal("close", dc->ToPayloads(std::string("done")));
int final_balance = handle.Result<int>();
```

:::note
Сигналы буферизуются и упорядочены. `ReceiveAsync`, вернувший `false`, означает,
что канал не содержит сообщений в данный момент, — но не то, что сигнал не придёт
никогда. Для более простого блокирующего варианта замените тело цикла на
`balance += credits.Receive()` и обработайте значение-часовой явно.
:::

---

## 5. Цикл с continue-as-new

Для воркфлоу, который должен выполняться бесконечно, `ContinueAsNew` атомарно
завершает текущий запуск и начинает новый с ограниченной историей. Используйте
его всякий раз, когда ожидаете бесконечный рост истории.

```cpp
// Ведёт обратный отсчёт через continue-as-new, возвращает 0 на последнем запуске.
int CountdownWorkflow(temporal::workflow::Context& ctx, int n) {
  if (n <= 0) {
    return 0;
  }
  // Терминальный вызов: не возвращает управление. Запускает новый запуск
  // того же типа воркфлоу с уменьшенным аргументом.
  ctx.ContinueAsNew("CountdownWorkflow", n - 1);
}
```

`ContinueAsNew` никогда не возвращает управление — помечайте всё после него
`[[unreachable]]` или просто возвращайте без значения (компилятор может
предупреждать; исключение выбрасывается внутренне).
`Result<R>()` клиента прозрачно следует по цепочке запусков до финального выполнения:

```cpp
auto handle = client.StartWorkflow(o, "CountdownWorkflow", 3);
int result = handle.Result<int>();  // 0, после запусков 3 -> 2 -> 1 -> 0
```

:::tip
Распространённый паттерн для цикла обработки — вызывать `ContinueAsNew` после
фиксированного числа итераций (например, каждые 1 000 элементов), передавая
накопленное состояние как аргументы нового запуска.
:::

---

## 6. Фиксация недетерминизма с помощью `SideEffect` и версионирование ветвей с `GetVersion`

### `SideEffect` — записать случайное/внешнее значение ровно один раз

`SideEffect` выполняет свою функцию при первом исполнении и записывает результат
в историю. При каждом воспроизведении возвращается записанное значение без повторного
вызова функции. Используйте для случайных идентификаторов, чтения системного времени
или любого значения, которое должно быть стабильным при воспроизведении, но не может
поступить из активности.

```cpp
#include <random>

std::string CreateOrderWorkflow(temporal::workflow::Context& ctx) {
  // Случайный ID генерируется один раз и фиксируется на весь срок этого запуска.
  int order_id = ctx.SideEffect<int>([] {
    std::mt19937 rng(std::random_device{}());
    return std::uniform_int_distribution<int>(1, 1'000'000)(rng);
  });

  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(30);
  return ctx.ExecuteActivity<std::string>(
      opts, "ReserveInventory", order_id).Get();
}
```

:::note
Функция `SideEffect` должна возвращать значение — `R` не может быть `void`. Для
всего, что имеет внешне видимые побочные эффекты, используйте активность.
:::

### `GetVersion` — безопасные изменения кода для выполняющихся воркфлоу

`GetVersion` позволяет деплоить изменения кода, пока старые выполнения ещё работают.
При первом выполнении записывает выбранную версию и возвращает её при каждом
воспроизведении, делая обе ветви детерминированными.

```cpp
std::string ProcessOrderWorkflow(temporal::workflow::Context& ctx,
                                 std::string order_id) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(30);

  // `kDefaultVersion` (-1) возвращается для историй, записанных *до* добавления
  // этого вызова. Когда все запуски до изменения завершатся, можно убрать
  // ветку v0 и поднять min_supported до 1.
  int v = ctx.GetVersion(
      "add-fraud-check",
      temporal::workflow::kDefaultVersion,  // min_supported
      1);                                   // max_supported

  if (v == temporal::workflow::kDefaultVersion) {
    // Исходный путь: без проверки на мошенничество.
    return ctx.ExecuteActivity<std::string>(
        opts, "ChargeCard", order_id).Get();
  } else {
    // Новый путь: сначала проверка на мошенничество.
    ctx.ExecuteActivity<void>(opts, "FraudCheck", order_id).Get();
    return ctx.ExecuteActivity<std::string>(
        opts, "ChargeCard", order_id).Get();
  }
}
```

Каждое независимое изменение получает собственную строку `change_id`. Маркеры
версий записываются в историю в порядке первого вызова `GetVersion`, поэтому
добавляйте их в одном и том же месте кода каждый раз.

---

## 7. Реакция на отмену воркфлоу

Организуйте гонку между активной работой и `ctx.AwaitCancellation()` в `Selector`.
При отмене воркфлоу future отмены срабатывает первым; отмените таймер (или активность),
затем быстро завершите выполнение.

### Отмена таймера при отмене воркфлоу

```cpp
std::string CancelAwareWorkflow(temporal::workflow::Context& ctx) {
  // Долго работающий таймер, имитирующий фоновую работу.
  auto timer = ctx.NewTimer(std::chrono::minutes(60));
  auto cancelled = ctx.AwaitCancellation();

  std::string result;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture(timer, [&]() {
    result = "timer-fired";
  });
  sel.AddFuture(cancelled, [&]() {
    timer.Cancel();   // немедленно разрешает таймер; не нужно ждать 60 минут
    result = "cancelled";
  });
  sel.Select();
  return result;
}
```

### Отмена активности при отмене воркфлоу

```cpp
// Активность: отправляет heartbeat, чтобы отмена от сервера дошла быстро.
std::string LongRunningActivity(temporal::activity::Context& ctx, int) {
  for (int i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ctx.RecordHeartbeat(i);
    if (ctx.IsCancelled()) {
      return "cancelled";  // корректный выход при отмене
    }
  }
  return "finished";
}

std::string ActivityCancelWorkflow(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(60);
  opts.heartbeat_timeout = std::chrono::seconds(5);

  auto act = ctx.ExecuteActivity<std::string>(opts, "LongRunningActivity", 0);
  auto cancelled = ctx.AwaitCancellation();

  bool cancel_requested = false;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(act, [&](std::string) { /* завершилась штатно */ });
  sel.AddFuture(cancelled, [&]() {
    act.Cancel();           // отправляет RequestCancelActivityTask на сервер
    cancel_requested = true;
  });
  sel.Select();

  // Если запросили отмену, ждём финального результата активности.
  // Активность возвращает "cancelled", как только обнаруживает IsCancelled()
  // в своём цикле heartbeat.
  return cancel_requested ? act.Get() : "finished";
}
```

Со стороны клиента:

```cpp
auto handle = client.StartWorkflow(o, "ActivityCancelWorkflow");
// ... даём немного поработать ...
handle.Cancel();
std::string result = handle.Result<std::string>();  // "cancelled"
```

:::note
Распространение отмены до активности требует, чтобы активность отправляла heartbeat
(`RecordHeartbeat`) и опрашивала `IsCancelled()`. Активность, не отправляющая
heartbeat, никогда не узнает о запросе отмены через `ctx.IsCancelled()`. Задавайте
`heartbeat_timeout` в `ActivityOptions`, чтобы сервер своевременно завершал
неотвечающую активность при исчезновении воркера. См.
[Возможности и паритет](parity.md) для актуального описания области действия отмены.
:::

---

## 8. Обновление с валидацией

Отклоняйте некорректные данные *до* того, как обновление будет принято и записано
в историю. Сначала выполняется валидатор; исключение предотвращает выполнение
обработчика и оставляет состояние воркфлоу нетронутым.

```cpp
int AccountWorkflow(temporal::workflow::Context& ctx) {
  int balance = 0;

  ctx.SetUpdateHandler(
    "deposit",
    // Обработчик: мутирует состояние и возвращает новый баланс.
    [&](int amount) -> int {
      balance += amount;
      return balance;
    },
    // Валидатор (только чтение): исключение отклоняет обновление; ничего не записывается.
    [](int amount) {
      if (amount <= 0) {
        throw temporal::ApplicationError(
            "deposit amount must be positive", "InvalidDeposit",
            /*non_retryable=*/true);
      }
    });

  ctx.GetSignalChannel<std::string>("close").Receive();
  return balance;
}
```

```cpp
int b1 = handle.Update<int>("deposit", 100);   // 100 — принято
int b2 = handle.Update<int>("deposit",  50);   // 150 — принято
handle.Update<int>("deposit", -5);             // выбрасывает исключение — валидатор отклонил
int b3 = handle.Update<int>("deposit",  20);   // 170 — состояние не изменено отклонением
```

---

## 9. Детерминированное тестирование истории воспроизведением

Экспортируйте историю реального воркфлоу и воспроизводите её против своего кода
без обращения к серверу. Это позволяет обнаружить недетерминированные изменения
до попадания в production.

```cpp
// 1. Запускаем воркфлоу и экспортируем его историю.
std::string history_json = handle.FetchHistoryJson();

// 2. Воспроизводим тот же код: исключений не ожидается.
{
  temporal::worker::Worker replayer(client, "replay-queue");
  replayer.RegisterWorkflow("MyWorkflow", MyWorkflowV1);
  replayer.RegisterActivity("MyActivity", MyActivity);
  replayer.ReplayWorkflowHistory(history_json);  // чисто
}

// 3. Воспроизводим изменённый код: должно выброситься исключение о недетерминизме.
{
  temporal::worker::Worker replayer(client, "replay-queue");
  replayer.RegisterWorkflow("MyWorkflow", MyWorkflowV2);  // несовместимое изменение
  replayer.RegisterActivity("MyActivity", MyActivity);
  // выбрасывает std::exception — последовательность команд расходится с записанной историей
  replayer.ReplayWorkflowHistory(history_json);
}
```

:::tip
Сохраните несколько экспортированных историй в качестве тестовых фикстур и
воспроизводите их в CI. Любая перестановка, добавление или удаление активностей/таймеров
провалит тест фикстуры, вместо того чтобы незаметно сломать выполняющееся в
production воркфлоу.
Полное руководство по тестированию воспроизведением — в разделе [Тестирование](testing.md).
:::

---

## 10. Воркфлоу на корутинах (`co_await`)

Пишите воркфлоу в стиле корутин C++20: возвращайте `workflow::workflow_task<R>`
и применяйте `co_await` к future вместо вызова `.Get()`. Он понижается в **те же**
команды, что и форма с `.Get()`, — идентичное поведение на проводе и реплей, —
поэтому используйте тот стиль, который читается лучше.

```cpp
#include <temporal/temporal.h>

temporal::workflow::workflow_task<std::string> CoAwaitWorkflow(
    temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);

  // co_await заменяет .Get(); воркфлоу блокируется точно так же.
  const std::string a = co_await ctx.ExecuteActivity<std::string>(o, "Echo", s);
  const std::string b = co_await ctx.ExecuteActivity<std::string>(o, "Echo", a + "!");
  co_return b;
}
```

Регистрируйте и запускайте его как любой воркфлоу:

```cpp
worker.RegisterWorkflow("CoAwaitWorkflow", CoAwaitWorkflow);
worker.RegisterActivity("Echo", EchoActivity);

temporal::StartWorkflowOptions o;
o.task_queue = "my-queue";
auto handle = client.StartWorkflow(o, "CoAwaitWorkflow", std::string("hi"));
std::string result = handle.Result<std::string>();  // "hi!"
```

---

## 11. Вызов Nexus-операции

Nexus вызывает операцию, обслуживаемую другим воркером (возможно, в другом
namespace), без привязки к его типам воркфлоу. Зарегистрируйте обработчик `R Fn(Arg)`
(один вход, один результат), зарегистрируйте endpoint, маршрутизирующий на его
очередь задач, затем вызовите его из воркфлоу.

```cpp
// Обработчик на обслуживающем воркере: R Fn(Arg).
std::string Greet(std::string name) { return "Hello, " + name; }

// Вызывающий воркфлоу.
std::string GreetViaNexus(temporal::workflow::Context& ctx, std::string who) {
  return ctx.ExecuteNexusOperation<std::string, std::string>(
      "greeting-endpoint",  // имя endpoint
      "greeting",           // сервис
      "say-hello",          // операция
      who                   // единственное входное значение
  ).Get();
}
```

Свяжите всё: зарегистрируйте операцию на обслуживающем воркере, зарегистрируйте
endpoint на клиенте (его цель — очередь задач, которую опрашивает этот воркер):

```cpp
serving_worker.RegisterNexusOperation("greeting", "say-hello", Greet);

std::string endpoint_id =
    client.CreateNexusEndpoint("greeting-endpoint", "nexus-handler-tq");

worker.RegisterWorkflow("GreetViaNexus", GreetViaNexus);
auto handle = client.StartWorkflow(o, "GreetViaNexus", std::string("World"));
std::string result = handle.Result<std::string>();  // "Hello, World"
```

:::note
Управление endpoint (и диспетчеризация Nexus) требует включённого Nexus на сервере,
например `temporal server start-dev --dynamic-config-value system.enableNexus=true`;
иначе `CreateNexusEndpoint` выбрасывает `RpcError`.
:::

---

## 12. Запуск воркфлоу по cron / календарному расписанию

`Client::CreateSchedule` запускает воркфлоу периодически. Используйте `cron_expressions`
для календарных триггеров (стандартный cron из 5 полей, необязательное ведущее поле
секунд и/или завершающий `CRON_TZ=<zone>`) вместо фиксированного `interval` — или
вместе с ним.

```cpp
temporal::ScheduleOptions opts;
opts.cron_expressions = {"0 9 * * MON-FRI"};  // по будням в 09:00
opts.workflow_type = "ReportWorkflow";
opts.task_queue = "reports";

client.CreateSchedule("daily-report", opts);

// Управление:
bool exists = client.DescribeSchedule("daily-report");
client.DeleteSchedule("daily-report");
```

---

## 13. Дочерний воркфлоу, переживающий родителя (`ParentClosePolicy::Abandon`)

По умолчанию выполняющийся дочерний воркфлоу завершается при закрытии родителя.
Установите `parent_close_policy` в `Abandon`, чтобы он продолжал работать
независимо (или `RequestCancel`, чтобы запросить его отмену).

```cpp
std::string StartAbandonedChild(temporal::workflow::Context& ctx,
                                std::string child_id) {
  temporal::ChildWorkflowOptions co;
  co.id = child_id;  // задаём известный id, чтобы другие могли слать ему сигналы
  co.parent_close_policy = temporal::ParentClosePolicy::Abandon;

  // Fire-and-forget: запускаем дочерний воркфлоу и возвращаемся, не ожидая его.
  ctx.ExecuteChildWorkflow<std::string>(co, "LongChild");
  return "parent-done";  // дочерний воркфлоу продолжает работать после этого return
}
```

Чтобы позже послать сигнал покинутому дочернему воркфлоу, адресуйте его по id через
`ctx.SignalExternalWorkflow(child_id, "signalName", value)` — handle, возвращённый
`ExecuteChildWorkflow`, является `Future` (у него есть `Get()`/`Cancel()`, метода
сигнала нет).

---

## 14. Асинхронное (ручное) завершение активности

Завершите активность вне основного потока: отложите её завершение, передайте токен
задачи во внешнюю систему и завершите её позже через клиент. `Future` воркфлоу
остаётся в ожидании, пока токен не будет разрешён.

```cpp
// Активность: откладывает завершение и передаёт свой токен; её возвращаемое значение игнорируется.
std::string ApprovalActivity(temporal::activity::Context& ctx, std::string req) {
  const std::string token = ctx.defer_completion();  // асинхронно; возвращает токен задачи
  enqueue_for_external_review(req, token);            // например, webhook / шаг с человеком
  return {};                                          // игнорируется — завершено в другом месте
}

std::string ApprovalWorkflow(temporal::workflow::Context& ctx, std::string req) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(300);  // с запасом: завершается вне основного потока
  // Этот Get() блокируется, пока с токеном не будет вызван CompleteActivity/FailActivity.
  return ctx.ExecuteActivity<std::string>(o, "ApprovalActivity", req).Get();
}
```

Позже — из любого места, где есть `Client` и токен:

```cpp
// Успех — закодированный результат становится тем, что выдаёт Future воркфлоу.
client.CompleteActivity(token, std::string("approved"));

// …или ошибка (сообщение + необязательный тип ошибки):
client.FailActivity(token, "rejected by reviewer", "ApprovalRejected");
```

:::note
`defer_completion()` — это сокращение в один вызов для `SetWillCompleteAsync()` плюс
чтение `GetInfo().task_token`. Задавайте `start_to_close_timeout` с запасом (или
heartbeat), поскольку активность завершается внешней стороной, а не возвратом значения.
:::
