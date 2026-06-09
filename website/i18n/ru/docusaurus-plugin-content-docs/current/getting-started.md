---
title: Начало работы
description: Установка зависимостей, сборка SDK и запуск воркфлоу от начала до конца.
---

# Начало работы

## Требования

- Компилятор C++20 (Apple Clang 21, актуальный Clang или GCC)
- CMake ≥ 3.21
- gRPC + Protobuf (C++) и nlohmann-json
- Temporal CLI (для локального сервера разработки)

На macOS:

```bash
brew install cmake grpc protobuf nlohmann-json temporal
```

GoogleTest загружается автоматически средствами CMake (`FetchContent`). Protobuf-определения `temporalio/api`
поставляются в виде git-субмодуля и компилируются во время сборки.

## Получить код

```bash
git clone --recurse-submodules <repo-url> temporal-cpp-sdk
cd temporal-cpp-sdk
# или, в уже существующем клоне:
git submodule update --init --recursive
```

## Сборка и тесты

```bash
cmake --preset default          # конфигурация (генерация C++ из protobuf, загрузка GoogleTest)
cmake --build build -j          # сборка библиотеки, примеров и тестов
ctest --test-dir build -LE integration   # юнит-тесты (сервер не требуется)
```

## Первый воркфлоу

Воркфлоу занимается оркестрацией; активность — непосредственно работой. Вот канонический пример «hello world»:

```cpp
#include <temporal/temporal.h>
#include <chrono>
#include <iostream>
#include <string>

// Активность: выполняется в реальном времени, может делать I/O.
std::string ComposeGreeting(temporal::activity::Context& ctx, std::string name) {
  return "Hello, " + name + "!";
}

// Воркфлоу: детерминированная оркестрация. `Get()` блокирует (паркует воркфлоу)
// до тех пор, пока активность не завершится.
std::string GreetWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);
  return ctx.ExecuteActivity<std::string>(opts, "ComposeGreeting", name).Get();
}

int main() {
  auto client = temporal::client::Client::Connect({.target = "localhost:7233"});

  temporal::worker::Worker worker(client, "hello-world");
  worker.RegisterWorkflow("GreetWorkflow", GreetWorkflow);
  worker.RegisterActivity("ComposeGreeting", ComposeGreeting);
  worker.Start();

  temporal::StartWorkflowOptions opts;
  opts.task_queue = "hello-world";
  auto handle = client.StartWorkflow(opts, "GreetWorkflow", std::string("Temporal"));
  std::cout << handle.Result<std::string>() << "\n";  // -> Hello, Temporal!

  worker.Stop();
}
```

## Запуск

```bash
temporal server start-dev                 # локальный сервер разработки на :7233
./build/examples/hello_world/hello_world  # -> workflow result: Hello, Temporal!
```

Проверить выполнение можно с помощью `temporal workflow list` или через веб-интерфейс по адресу http://localhost:8233.

## Интеграционные тесты (против реального сервера)

Сквозные тесты защищены флагом, поэтому по умолчанию запускаются без сервера:

```bash
temporal server start-dev &
TEMPORAL_INTEGRATION=1 ctest --test-dir build -L integration
```
