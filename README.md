# temporal-cpp

An **experimental, native C++ SDK for [Temporal](https://temporal.io)** — a from-scratch
port modeled on the official [Go SDK](https://github.com/temporalio/sdk-go), talking directly
to the Temporal frontend over gRPC with its own workflow replay engine. No Rust `sdk-core`
dependency.

> ⚠️ **Status: experimental / proof-of-concept.** This is not an official Temporal SDK and is
> not affiliated with Temporal Technologies. It currently implements a working *vertical slice*
> — enough to run real workflows that orchestrate activities and timers end-to-end — plus the
> full project scaffolding for growing toward Go-SDK parity. See [docs/ROADMAP.md](docs/ROADMAP.md)
> for what is and isn't supported yet.

## Why native (and how it relates to the other SDKs)

Temporal's SDKs come in two flavors:

- **Native** (Go, Java): implement the gRPC client *and* the determinism-critical workflow
  state-machine / history-replay engine directly in the host language.
- **Core-based** (Python, TypeScript, .NET, Ruby): delegate that engine to the Rust
  [`sdk-core`](https://github.com/temporalio/sdk-core) and only implement a thin lang-side
  runtime + data converter + public API.

This project takes the **native** route, mirroring the Go SDK's structure and developer
experience in idiomatic C++. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design
and the determinism model.

## Public API at a glance

The API deliberately reads like the Go SDK:

```cpp
#include <temporal/temporal.h>
#include <chrono>
#include <string>

// An activity: runs in real time, may do I/O.
std::string ComposeGreeting(temporal::activity::Context& ctx, std::string name) {
  return "Hello, " + name + "!";
}

// A workflow: deterministic orchestration. `Get()` blocks (parks the workflow)
// until the activity resolves on a later workflow task.
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

## Requirements

- C++20 compiler (Apple Clang 21 / recent Clang or GCC)
- CMake ≥ 3.21
- gRPC + Protobuf C++, nlohmann-json — on macOS: `brew install grpc protobuf nlohmann-json`
- For the end-to-end example: the Temporal CLI dev server — `brew install temporal`
- GoogleTest is fetched automatically (CMake `FetchContent`)

The `temporalio/api` protobuf definitions are vendored as a git submodule under
`third_party/api` and compiled at build time. Clone with submodules:

```bash
git clone --recurse-submodules <repo-url>
# or, in an existing clone:
git submodule update --init --recursive
```

## Build & test

```bash
cmake --preset default          # configure (generates protobuf C++, fetches GoogleTest)
cmake --build build -j          # build the library, example, and tests
ctest --test-dir build -LE integration   # unit tests (no server needed)
```

### Integration tests

End-to-end tests (timers, activity-failure propagation, parallel activities, retry policy,
terminate, signals, cancellation, queries, and selectors) run against a real server and are gated
so the default run needs none:

```bash
temporal server start-dev &                                   # dev server on :7233
TEMPORAL_INTEGRATION=1 ctest --test-dir build -L integration  # run them
```

Without `TEMPORAL_INTEGRATION=1` they self-skip. CI (`.github/workflows/ci.yml`) runs both
suites on macOS, standing up a dev server for the integration pass.

## Run the example end-to-end

```bash
temporal server start-dev                 # terminal 1: a local dev server on :7233
./build/examples/hello_world/hello_world  # terminal 2
# started workflow id=... run_id=...
# workflow result: Hello, Temporal!
```

You can inspect the run with `temporal workflow list` or the Web UI at http://localhost:8233.

## Project layout

```
include/temporal/   Public headers (client/ worker/ workflow/ activity/ converter/ common/ log/)
src/                Implementation; src/internal/ is the native engine (not installed)
cmake/              Protobuf/gRPC code generation
examples/           Runnable examples
tests/              GoogleTest unit tests
third_party/        Vendored protos + reference SDKs (submodules)
docs/               Architecture and roadmap
```

## References

- [Temporal Go SDK](https://github.com/temporalio/sdk-go) — the native SDK this mirrors
- [Temporal Python SDK](https://github.com/temporalio/sdk-python) — a core-based SDK, used to
  understand the lang/engine boundary
- [temporalio/api](https://github.com/temporalio/api) — the protobuf API definitions

## License

MIT — see [LICENSE](LICENSE). Not affiliated with or endorsed by Temporal Technologies, Inc.
