---
title: Установка и упаковка
description: Сборка temporal-cpp-sdk на macOS, Linux или любой платформе через Conan, установка и подключение из другого CMake-проекта через find_package.
---

# Установка и упаковка

[Быстрый старт](getting-started.md) описывает короткий путь для macOS. Эта страница — справочник по
кроссплатформенной сборке, установке и упаковке: как получить зависимости на каждой ОС, какие есть
опции сборки и как подключить установленный SDK из другого проекта.

## Требования

- Компилятор C++20 — свежий Clang/AppleClang, GCC ≥ 11 или MSVC 19.3x
- CMake ≥ 3.21
- Зависимости **gRPC**, **Protobuf** (C++) и **nlohmann-json**
- Protobuf-определения `temporalio/api`, вендоренные как git-сабмодуль и компилируемые во время сборки:

  ```bash
  git submodule update --init third_party/api
  ```

GoogleTest подтягивается автоматически (`FetchContent`), если рядом нет config-пакета `GTest`
(например, от Conan или системной установки).

## Получение зависимостей

Зависимости C++-тулчейна можно установить тремя способами. Выберите подходящий для вашей платформы.

### macOS — Homebrew

```bash
brew install cmake grpc protobuf nlohmann-json
```

CMake находит копии Homebrew автоматически (сборка добавляет префикс Homebrew в начало
`CMAKE_PREFIX_PATH` — но только когда не задан toolchain-файл, поэтому toolchain от Conan никогда не
перекрывается).

### Linux — системные пакеты

Сборка получает gRPC/Protobuf через их **CMake config-пакеты** (`find_package(... CONFIG)`).
Хватит ли системных пакетов, зависит от того, поставляет ли их дистрибутив:

- **Fedora** их поставляет, поэтому системные пакеты работают напрямую:

  ```bash
  sudo dnf install cmake gcc-c++ grpc-devel grpc-plugins protobuf-devel protobuf-compiler json-devel
  ```

- **Debian/Ubuntu** поставляют pkg-config-файлы, но *не* CMake config-пакеты для gRPC/Protobuf,
  поэтому `find_package(gRPC CONFIG)` / `find_package(protobuf CONFIG)` их не найдёт. Используйте
  путь через **Conan** ниже — именно его использует CI на Linux.

### Любая платформа — Conan

[Conan](https://conan.io) разрешает зависимости из исходников/бинарников на macOS, Linux и Windows,
поэтому вы не зависите от системного пакетного менеджера:

```bash
pip install conan
conan profile detect --force
git submodule update --init third_party/api

conan install . --build=missing      # разрешает gRPC + Protobuf + nlohmann_json
cmake --preset conan-release         # пресет, сгенерированный Conan
cmake --build --preset conan-release
```

:::note
В `conanfile.py` зафиксирована совместимая пара версий gRPC/Protobuf. Поднимайте их вместе, если
нужен более новый тулчейн — gRPC собирается под конкретную версию Protobuf.
:::

## Сборка

Когда зависимости на месте, сборка одинакова везде:

```bash
cmake --preset default          # конфигурация (генерирует C++ из protobuf, тянет GoogleTest)
cmake --build build -j          # сборка библиотеки, примера и тестов
ctest --test-dir build -LE integration   # юнит-тесты (сервер не нужен)
```

### Опции сборки

| Опция | По умолчанию | Действие |
| --- | --- | --- |
| `TEMPORAL_BUILD_TESTS` | `ON` | Собирать драйвер юнит- и интеграционных тестов. |
| `TEMPORAL_BUILD_EXAMPLES` | `ON` | Собирать встроенные примеры. |
| `TEMPORAL_INSTALL` | `ON` | Генерировать правила `install()` и config-пакет `find_package(temporal-cpp-sdk)`. |
| `TEMPORAL_ENABLE_CLANG_TIDY` | `OFF` | Запускать clang-tidy по исходникам проекта во время сборки. |

Пример — компактная сборка только библиотеки для упаковки:

```bash
cmake --preset default -DTEMPORAL_BUILD_TESTS=OFF -DTEMPORAL_BUILD_EXAMPLES=OFF
```

## Установка

```bash
cmake --install build --prefix /ваш/префикс
```

Устанавливаются статические архивы (`temporal_sdk`, `temporal_proto`), публичные заголовки в
`include/temporal/…` (плюс сгенерированные заголовки protobuf) и CMake config-пакет в
`lib/cmake/temporal-cpp-sdk/`.

## Подключение из другого проекта

### Через CMake `find_package`

```cmake
cmake_minimum_required(VERSION 3.20)
project(my_app CXX)

find_package(temporal-cpp-sdk CONFIG REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE temporal::sdk)
```

Укажите CMake на дерево установки (или сборки) и транзитивные зависимости:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH="/ваш/префикс"
```

Config-пакет сам заново находит gRPC, Protobuf, nlohmann_json и Threads, поэтому потребителю нужно
только слинковаться с `temporal::sdk`. (Каталог `tests/packaging/` в этом репозитории — ровно такой
потребитель: отдельный проект, который можно собрать против дерева установки для смоук-теста упаковки.)

### Через Conan

Если опубликовать рецепт (`conan create . --build=missing`), потребитель может зависеть от него и
получить тот же таргет `temporal::sdk`:

```python
# conanfile.txt
[requires]
temporal-cpp-sdk/0.1.0

[generators]
CMakeDeps
CMakeToolchain
```

## Поддержка платформ

| Платформа | Как | Статус |
| --- | --- | --- |
| macOS (arm64) | Homebrew или Conan | Собирается и полностью тестируется в CI |
| Linux (x86-64) | Conan (или системные пакеты Fedora) | Собирается в CI через Conan |
| Windows (MSVC) | Conan | Флаги компилятора безопасны для MSVC; в CI пока не проверяется |

CMake-конфигурация избегает платформенно-зависимых допущений (флаги предупреждений ограничены
GNU/Clang, инструменты `protoc`/`grpc_cpp_plugin` берутся из импортированных таргетов, то есть от
того, что предоставило библиотеки). Ожидается, что Windows работает через Conan, но пока не покрыт CI.
