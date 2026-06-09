---
title: Селекторы, дочерние воркфлоу и не только
description: Селекторы, дочерние воркфлоу, continue-as-new и отмена.
---

# Селекторы, дочерние воркфлоу и не только

## Селекторы

`Selector` ожидает несколько future и продолжает выполнение, как только **любой** из них станет готов — классический паттерн «активность ИЛИ таймаут»:

```cpp
#include <temporal/workflow/selector.h>

std::string WithTimeout(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);

  auto work    = ctx.ExecuteActivity<std::string>(o, "SlowWork", "x");
  auto timeout = ctx.NewTimer(std::chrono::seconds(5));

  std::string out;
  temporal::workflow::Selector selector(ctx);
  selector.AddFuture<std::string>(work, [&](std::string r) { out = "done: " + r; });
  selector.AddFuture(timeout, [&]() { out = "timed out"; });
  selector.Select();   // выполняет обработчик первого готового варианта
  return out;
}
```

- `AddFuture<T>(future, handler)` регистрирует future со значением; перегрузка
  `AddFuture(future, handler)` для `Future<void>` (таймер или `AwaitCancellation()`) **не**
  шаблонная — без `<void>`.
- `AddReceive<T>(channel, handler)` добавляет вариант с каналом сигналов: готов, когда сигнал
  буферизован, потребляя его и передавая значение в обработчик.
- `AddDefault(handler)` делает `Select()` неблокирующим (обработчик по умолчанию вызывается, если ни один future ещё не готов).
- `Select()` приостанавливает воркфлоу (через корутину) до тех пор, пока один из вариантов не станет готов.

Один и тот же селектор может сочетать future, таймеры и приёмы сигналов — например, «первый сигнал ИЛИ таймаут»:

```cpp
std::string out;
temporal::workflow::Selector selector(ctx);
selector.AddReceive<std::string>(ctx.GetSignalChannel<std::string>("go"),
                                 [&](std::string s) { out = "signal:" + s; });
selector.AddFuture(ctx.NewTimer(std::chrono::seconds(5)), [&] { out = "timeout"; });
selector.Select();
```

## Дочерние воркфлоу

Запустите другой воркфлоу как дочерний и дождитесь его результата:

```cpp
std::string Parent(temporal::workflow::Context& ctx, std::string name) {
  temporal::ChildWorkflowOptions o;
  o.task_queue = ctx.GetInfo().task_queue;   // по умолчанию: очередь задач родителя
  return ctx.ExecuteChildWorkflow<std::string>(o, "GreetChild", name).Get();
}

std::string GreetChild(temporal::workflow::Context& ctx, std::string name) {
  return "Hello, " + name;
}
```

Дочерний воркфлоу выполняется как самостоятельное исполнение воркфлоу; future родительского воркфлоу разрешается, когда дочерний завершается (или выбрасывает `ActivityError` в случае ошибки). Идентификатор дочернего воркфлоу формируется детерминированно на основе идентификатора родительского (переопределяется через `ChildWorkflowOptions::id`).

### Политика закрытия родителя (parent-close policy)

`ChildWorkflowOptions::parent_close_policy` решает, что произойдёт с ещё работающим дочерним
воркфлоу, когда родитель закрывается:

```cpp
temporal::ChildWorkflowOptions o;
o.parent_close_policy = temporal::ParentClosePolicy::Abandon;   // дать ему пережить родителя
ctx.ExecuteChildWorkflow<std::string>(o, "SleeperChild");       // запустить и забыть (не ждать)
```

- `Terminate` *(по умолчанию)* — сервер убивает дочерний воркфлоу, когда родитель закрывается.
- `Abandon` — дочерний воркфлоу продолжает работать независимо.
- `RequestCancel` — сервер запрашивает отмену дочернего воркфлоу.

Чтобы отправить сигнал или отменить конкретный дочерний воркфлоу, пока он работает, задайте ему
известный `id` и используйте `ctx.SignalExternalWorkflow(child_id, ...)` /
`ctx.CancelExternalWorkflow(child_id)` (см. [сигналы, запросы и обновления](/workflows/messaging)).

## Операции Nexus

Воркфлоу может вызвать **операцию Nexus**, обслуживаемую другим неймспейсом/командой, через
зарегистрированный endpoint. В отличие от активности, у операции Nexus вход и результат — каждый
*единственное* значение:

```cpp
auto fut = ctx.ExecuteNexusOperation<std::string, std::string>(
    "payments-endpoint", "billing.v1", "Charge", order_id);
std::string receipt = fut.Get();
```

`ExecuteNexusOperation<R, Arg>(endpoint, service, operation, input, schedule_to_close = {})`
возвращает `Future<R>`; необязательный последний аргумент ограничивает весь вызов (0 = значение по
умолчанию на сервере). Endpoint регистрируется через `Client::CreateNexusEndpoint` и обслуживается
воркером, который вызвал `Worker::RegisterNexusOperation`.

## Continue-as-new

Для долгоживущих или циклических воркфлоу **continue-as-new** атомарно завершает текущий запуск и начинает новый — удерживая историю событий в разумных пределах:

```cpp
int Countdown(temporal::workflow::Context& ctx, int n) {
  if (n <= 0) return 0;
  ctx.ContinueAsNew("Countdown", n - 1);   // терминальный: никогда не возвращает управление
}
```

`ContinueAsNew(workflow_type, args...)` не возвращает управление — он перезапускает воркфлоу с новыми входными данными. Метод `Result()` клиента прозрачно **следует по цепочке** запусков до финального:

```cpp
int result = handle.Result<int>();   // 0, after Countdown(3) -> 2 -> 1 -> 0
```

## Отмена

Клиент может запросить отмену выполняющегося воркфлоу:

```cpp
handle.Cancel();
```

Воркфлоу наблюдает отмену и сам решает, как на неё реагировать. Простейшая форма опрашивает `ctx.IsCancelled()`:

```cpp
std::string Cancellable(temporal::workflow::Context& ctx) {
  auto channel = ctx.GetSignalChannel<std::string>("go");
  while (true) {
    if (ctx.IsCancelled()) {
      return "cleaned up";   // завершиться корректно
    }
    channel.Receive();
  }
}
```

Чтобы *состязать* выполняющуюся работу с отменой — и сворачивать эту работу при отмене — добавьте
`ctx.AwaitCancellation()` (`Future<void>`, который завершается при отмене) как вариант селектора и
вызовите `Cancel()` на future операции:

```cpp
std::string RaceWork(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);
  auto act       = ctx.ExecuteActivity<std::string>(o, "SlowWork", "x");
  auto cancelled = ctx.AwaitCancellation();

  std::string out;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(act, [&](std::string r) { out = r; });
  sel.AddFuture(cancelled, [&] { act.Cancel(); out = "cancelled"; });   // свернуть активность
  sel.Select();
  return out;
}
```

`Future::Cancel()` запрашивает отмену нижележащей операции (сегодня — таймеров) и заставляет
последующий `Get()` немедленно разблокироваться.

:::note
Структурированные области отмены пока не предоставлены — распространение явное: наблюдайте через
`IsCancelled()` / `AwaitCancellation()` и отменяйте `Future` каждой операции самостоятельно.
Смотрите [матрицу паритета](/parity).
:::

## Написание через корутины (`co_await`)

В качестве альтернативы `Future::Get()` воркфлоу может возвращать `workflow::workflow_task<R>` и
использовать `co_await` на future вместе с `co_return`. Это сводится к тем же командам, поэтому
порядок команд и реплей идентичны:

```cpp
#include <temporal/workflow/coro.h>

temporal::workflow::workflow_task<std::string> CoAwaitWorkflow(
    temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(10);
  std::string a = co_await ctx.ExecuteActivity<std::string>(o, "Echo", s);
  co_return a;
}
```

Зарегистрируйте его точно так же, как обычную функцию воркфлоу (`worker.RegisterWorkflow("CoAwaitWorkflow", ...)`).
