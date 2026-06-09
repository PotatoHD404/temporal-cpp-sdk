---
title: Сигналы
description: Асинхронные, долговечные сообщения, доставляемые в работающий воркфлоу.
---

# Сигналы

**Сигнал** — это асинхронное, долговечное сообщение, доставляемое в *работающий* воркфлоу. Отправка
устроена по принципу fire-and-forget: вызывающая сторона не ждёт реакции воркфлоу и не получает
возвращаемого значения. Доставка записывается в историю воркфлоу, поэтому она происходит
**как минимум один раз** и переживает перезапуски воркера — при реплее сигнал доставляется повторно
ровно в том месте, где он пришёл впервые.

Сигналы — один из трёх способов взаимодействовать с работающим воркфлоу:

| Механизм | Что делает |
|-----------|--------------|
| **Сигнал** | fire-and-forget сообщение *в* воркфлоу; может изменять состояние; ничего не возвращает. |
| **Запрос**  | запрос только для чтения *из* воркфлоу; возвращает значение синхронно. |
| **Обновление** | запрос/ответ, который *может изменять* состояние и возвращает значение синхронно. |

Запросы и обновления рассмотрены на странице [сигналы, запросы и обновления](/workflows/messaging); эта
страница — полный справочник по сигналам. Про базовую модель — историю, реплей, sticky-кэш —
см. [концепции](/concepts).

## Приём сигналов в воркфлоу

Воркфлоу читает сигналы из **канала**, ключом которого служит имя. Получите канал через
`GetSignalChannel<T>` и вызовите `Receive()` — он блокируется (паркует воркфлоу) до тех пор, пока
сигнал не окажется в буфере, затем декодирует payload в `T` через [конвертер данных](/data-conversion):

```cpp
std::string GreetBySignal(temporal::workflow::Context& ctx) {
  auto channel = ctx.GetSignalChannel<std::string>("setName");
  std::string name = channel.Receive();   // blocks (parks) until a signal arrives
  return "Hello, " + name;
}
```

Парковка бесплатна: запаркованный воркфлоу не занимает ни одного потока воркера и вытесняется из
sticky-кэша как обычно; когда сигнал приходит, воркфлоу детерминированно возобновляется из истории.

### Неблокирующий приём

`ReceiveAsync()` вычерпывает следующий буферизованный сигнал без блокировки, возвращая
`std::optional<T>`, равный `std::nullopt`, когда в очереди ничего нет:

```cpp
auto channel = ctx.GetSignalChannel<int>("add");
while (auto v = channel.ReceiveAsync()) {   // drain everything buffered right now
  total += *v;
}
```

Существует также форма с bool + out-параметром, `bool ReceiveAsync(T& out)`, сохранённая ради
совместимости исходного кода.

### Гарантии буферизации и упорядочивания

Сигналы **буферизуются и упорядочиваются**. Несколько могут накопиться, прежде чем воркфлоу доберётся
до их чтения, и N-й `Receive()` всегда возвращает N-й сигнал, отправленный на это имя. Поскольку буфер
детерминированно восстанавливается из истории при каждом реплее, порядок стабилен между перезапусками
воркера и повторными прогонами.

:::note Вычерпайте до завершения
Функция воркфлоу, которая возвращает управление, пока сигналы ещё буферизованы, отбрасывает их — они
*не* доставляются повторно будущему запуску. Если завершение конкурирует с сигналами «на лету»,
вычерпайте канал через `ReceiveAsync()` (или сторожевым сигналом, см. ниже) перед возвратом.
:::

## Отправка сигнала из клиента

Извне воркфлоу получите `WorkflowHandle` (из `StartWorkflow` или `Client::GetHandle`) и вызовите
`Signal`. Вариативная перегрузка кодирует аргумент(ы) за вас через конвертер данных:

```cpp
handle.Signal("setName", std::string("World"));   // encodes via the data converter
```

Это fire-and-forget: вызов возвращается, как только сервер записал сигнал, а не когда воркфлоу его
обработал. Сигнализация идентификатора воркфлоу, который сервер не знает, выбрасывает
`temporal::NotFoundError` (подкласс `RpcError`).

Если у вас уже есть закодированные `Payloads`, предкодированная перегрузка `Signal(name, payloads)`
отправляет их как есть:

```cpp
const auto dc = temporal::DataConverter::Default();
handle.Signal("add", dc->ToPayloads(5));   // pre-encoded payloads
```

## Типизированные дескрипторы сигналов

Повторять имя на проводе в виде строки плюс явный `<T>` в каждой точке вызова — чревато ошибками.
Объявите `temporal::SignalRef<T>` один раз на уровне неймспейса — он связывает имя и тип payload
вместе — и тогда и `T` принимающего канала, и отправляемое значение проверяются и выводятся из него:

```cpp
inline constexpr temporal::SignalRef<bool> kStop{"stop"};
```

Внутри воркфлоу дескриптор заменяет имя *и* `<T>` канала:

```cpp
ctx.GetSignalChannel(kStop).Receive();   // ReceiveChannel<bool>, deduced
```

А из клиента тип значения проверяется по дескриптору:

```cpp
handle.Signal(kStop, true);   // bool checked against SignalRef<bool>
```

Типизированные дескрипторы сводятся к тем же строковым именам, поэтому взаимодействуют со строковым API
выше и реплеятся идентично.

## Signal-with-start

`Client::SignalWithStartWorkflow` атомарно *запускает воркфлоу, если он ещё не выполняется*, а затем
доставляет ему сигнал — за один вызов. Если воркфлоу с таким id уже выполняется, он лишь получает
сигнал. Это канонический способ управлять воркфлоу-«сущностью», первое взаимодействие с которой может
быть и её созданием.

Аргумент сигнала передаётся предкодированным как `Payloads`; любые оставшиеся аргументы — это
стартовые аргументы воркфлоу:

```cpp
const auto dc = temporal::DataConverter::Default();
temporal::StartWorkflowOptions o;
o.task_queue = "greeter";
o.workflow_id = "greeter-42";   // stable id makes start-or-signal idempotent

auto handle = client.SignalWithStartWorkflow(
    o, "GreetBySignalWorkflow",        // workflow type
    "setName", dc->ToPayloads(std::string("Ada")));   // signal name + pre-encoded arg
// handle now refers to the running workflow, which already has the "setName" signal buffered.
```

## Сигнализация другого воркфлоу из воркфлоу

Работающий воркфлоу может послать сигнал *другому* работающему воркфлоу по id через
`SignalExternalWorkflow` — тоже fire-and-forget. Он кодирует свои аргументы через конвертер данных,
в точности как вариативный `Signal` клиента. Это работает для любого id воркфлоу, включая дочерний,
который запустил текущий воркфлоу:

```cpp
std::string SignalExternalWf(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));   // encodes args
  ctx.Sleep(std::chrono::seconds(3));   // stay alive so the request leaves before we close
  return "done";
}
```

:::note Доставьте до завершения
`SignalExternalWorkflow` — это детерминированная команда, доставляемая асинхронно. Воркфлоу, который
не делает ничего, кроме сигнализации другого, и сразу возвращается, может закрыться до того, как запрос
уйдёт, — продержите его живым недолго (короткий `Sleep`, как выше, или другая ожидающая работа). См.
также [отмену](/cancellation) про парный `CancelExternalWorkflow`.
:::

## Гонка сигнала с таймером или отменой

Чтобы ждать сигнал *но не вечно*, состязайте канал с таймером (или с `AwaitCancellation()`) через
[`Selector`](/cancellation). `AddReceive` добавляет случай-канал, который становится готов, когда
сигнал буферизован; `Select()` выполняет тот случай, который срабатывает первым:

```cpp
std::string WaitForGoOrTimeout(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("go");
  auto timeout = ctx.NewTimer(std::chrono::seconds(30));
  std::string out;

  temporal::workflow::Selector sel(ctx);
  sel.AddReceive<std::string>(signals, [&](std::string s) { out = "signal:" + s; });
  sel.AddFuture(timeout, [&]() { out = "timeout"; });
  sel.Select();   // blocks until the signal arrives OR the timer fires

  return out;
}
```

Добавьте `sel.AddDefault([&]{ ... })`, чтобы сделать select неблокирующим (выполнить случай по
умолчанию, когда пока ничего не готово).

## Полный пример

Воркфлоу, который в цикле принимает сигналы `"input"` и накапливает их, пока не придёт сторожевой
сигнал `"done"`, плюс клиент, который отправляет три сигнала и читает результат:

```cpp
#include <temporal/temporal.h>
#include <iostream>
#include <string>

// Counts "input" signals until a "done" signal arrives, then completes with the count.
int CountSignalsWorkflow(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("input");
  int count = 0;
  while (signals.Receive() != "done") {   // blocks per iteration; "done" is the sentinel
    ++count;
  }
  return count;
}

int main() {
  auto client = temporal::client::Client::Connect({.target = "localhost:7233"});

  temporal::worker::Worker worker(client, "signals");
  worker.RegisterWorkflow("CountSignalsWorkflow", CountSignalsWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions opts;
  opts.task_queue = "signals";
  auto handle = client.StartWorkflow(opts, "CountSignalsWorkflow");

  handle.Signal("input", std::string("a"));      // variadic: encodes for us
  handle.Signal("input", std::string("b"));
  handle.Signal("input", std::string("done"));   // sentinel completes the loop

  std::cout << handle.Result<int>() << "\n";      // -> 2
  worker.Stop();
}
```

Целочисленный канал использует ту же форму с числовым сторожевым значением, например отрицательным
значением для завершения:

```cpp
int Sum(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<int>("add");
  int sum = 0;
  while (true) {
    const int v = signals.Receive();
    if (v < 0) return sum;   // negative sentinel completes the workflow
    sum += v;
  }
}
```

## Распространённые паттерны и подводные камни

- **Используйте сторожевое значение, чтобы завершить цикл.** Долгоживущему воркфлоу, который крутится
  на `Receive()`, нужен явный способ остановиться — выделенный сигнал `"done"`/`"stop"` или внеполосное
  значение (отрицательное число, пустая строка). Без него он паркуется навсегда.
- **Вычерпайте буферизованные сигналы перед завершением.** Возврат, когда сигналы ещё в очереди,
  молча отбрасывает их; они не доставляются повторно более позднему запуску.
  `while (auto v = ch.ReceiveAsync()) { ... }` вычерпывает всё, что буферизовано прямо сейчас.
- **Вызывающая сторона должна оставаться живой достаточно долго, чтобы доставить.** Воркфлоу, чья
  единственная задача — `SignalExternalWorkflow` для другого и возврат, может закрыться до того, как
  команда отправлена. Продержите его живым недолго (короткий `Sleep` или другая ожидающая работа).
  На стороне клиента процесс, который сигнализирует и сразу выходит, — это нормально: сигнал
  долговечен, как только вызов `Signal` вернул управление.
- **Сигналы доставляются как минимум один раз и не упорядочены *между именами*.** Упорядочивание
  гарантировано только *в пределах* одного имени сигнала. Не полагайтесь на то, что сигнал на канале
  `"a"` будет наблюдён раньше, чем на канале `"b"`.
- **Сигналы не могут вернуть значение.** Если отправителю нужен ответ, используйте
  [обновление](/workflows/messaging) — это синхронный запрос/ответ, который тоже может изменять
  состояние.
- **Запрос сразу после сигнализации согласован лишь в конечном счёте.** Сигнал изменяет состояние
  асинхронно, поэтому [запрос](/workflows/messaging), выполненный сразу после, может ещё не отразить
  его — сделайте опрос, если нужно наблюдать конкретную запись.
