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
  selector.Select();   // runs the first ready case's handler
  return out;
}
```

- `AddFuture<T>(future, handler)` регистрирует future со значением; `AddFuture(Future<void>, handler)` — таймер.
- `AddDefault(handler)` делает `Select()` неблокирующим (обработчик по умолчанию вызывается, если ни один future ещё не готов).
- `Select()` приостанавливает воркфлоу (через корутину) до тех пор, пока один из вариантов не станет готов.

## Дочерние воркфлоу

Запустите другой воркфлоу как дочерний и дождитесь его результата:

```cpp
std::string Parent(temporal::workflow::Context& ctx, std::string name) {
  temporal::ChildWorkflowOptions o;
  o.task_queue = ctx.GetInfo().task_queue;   // default: the parent's task queue
  return ctx.ExecuteChildWorkflow<std::string>(o, "GreetChild", name).Get();
}

std::string GreetChild(temporal::workflow::Context& ctx, std::string name) {
  return "Hello, " + name;
}
```

Дочерний воркфлоу выполняется как самостоятельное исполнение воркфлоу; future родительского воркфлоу разрешается, когда дочерний завершается (или выбрасывает `ActivityError` в случае ошибки). Идентификатор дочернего воркфлоу формируется детерминированно на основе идентификатора родительского (переопределяется через `ChildWorkflowOptions::id`).

## Continue-as-new

Для долгоживущих или циклических воркфлоу **continue-as-new** атомарно завершает текущий запуск и начинает новый — удерживая историю событий в разумных пределах:

```cpp
int Countdown(temporal::workflow::Context& ctx, int n) {
  if (n <= 0) return 0;
  ctx.ContinueAsNew("Countdown", n - 1);   // terminal: never returns
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

Воркфлоу наблюдает отмену и сам решает, как на неё реагировать:

```cpp
std::string Cancellable(temporal::workflow::Context& ctx) {
  auto channel = ctx.GetSignalChannel<std::string>("go");
  while (true) {
    if (ctx.IsCancelled()) {
      return "cleaned up";   // finish gracefully
    }
    channel.Receive();
  }
}
```

:::note
Отмена в настоящее время реализована только в режиме **наблюдения** через `IsCancelled()`. Автоматическое распространение отмены на выполняющиеся активности/таймеры и структурированные области отмены пока не реализованы — см. [матрицу паритета](/parity).
:::
