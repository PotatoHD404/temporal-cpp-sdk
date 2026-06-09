---
title: Getting started
description: Install dependencies, build the SDK, and run a workflow end-to-end.
---

# Getting started

## Requirements

- A C++20 compiler (Apple Clang 21 / recent Clang or GCC)
- CMake ≥ 3.21
- gRPC + Protobuf (C++) and nlohmann-json
- The Temporal CLI (for a local dev server)

On macOS:

```bash
brew install cmake grpc protobuf nlohmann-json temporal
```

GoogleTest is fetched automatically by CMake (`FetchContent`). The `temporalio/api` protobuf
definitions are vendored as a git submodule and compiled at build time.

## Get the code

```bash
git clone --recurse-submodules <repo-url> temporal-cpp-sdk
cd temporal-cpp-sdk
# or, in an existing clone:
git submodule update --init --recursive
```

## Build & test

```bash
cmake --preset default          # configure (generates protobuf C++, fetches GoogleTest)
cmake --build build -j          # build the library, example, and tests
ctest --test-dir build -LE integration   # unit tests (no server needed)
```

## Your first workflow

A workflow orchestrates; an activity does the work. Here's the canonical "hello world":

```cpp
#include <temporal/temporal.h>
#include <chrono>
#include <iostream>
#include <string>

// An activity: runs in real time, may do I/O.
std::string ComposeGreeting(temporal::activity::Context& ctx, std::string name) {
  return "Hello, " + name + "!";
}

// A workflow: deterministic orchestration. `Get()` blocks (parking the workflow)
// until the activity resolves.
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

## Run it

```bash
temporal server start-dev                 # a local dev server on :7233
./build/examples/hello_world/hello_world  # -> workflow result: Hello, Temporal!
```

Inspect the run with `temporal workflow list` or the Web UI at http://localhost:8233.

## Integration tests (against a real server)

The end-to-end tests are gated so the default run needs no server:

```bash
temporal server start-dev &
TEMPORAL_INTEGRATION=1 ctest --test-dir build -L integration
```
