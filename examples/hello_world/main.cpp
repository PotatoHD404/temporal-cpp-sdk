// Minimal end-to-end Temporal workflow in C++: a worker runs `GreetWorkflow`,
// which calls the `ComposeGreeting` activity and returns its result; the client
// starts the workflow and prints the result.
//
// Run a dev server first:  temporal server start-dev
#include <chrono>
#include <exception>
#include <iostream>
#include <string>

#include <temporal/temporal.h>

namespace {

// An activity runs in real time and may perform I/O. Here it just builds a string.
std::string ComposeGreeting(temporal::activity::Context& ctx, std::string name) {
  (void)ctx;
  return "Hello, " + name + "!";
}

// A workflow is deterministic orchestration. It calls the activity and returns
// the result. `ExecuteActivity<R>(...).Get()` blocks (parking the workflow) until
// the activity completes on a later workflow task.
std::string GreetWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);
  std::string greeting =
      ctx.ExecuteActivity<std::string>(opts, "ComposeGreeting", name).Get();
  ctx.GetLogger().Info("composed greeting", {temporal::log::F("greeting", greeting)});
  return greeting;
}

}  // namespace

int main() {
  const std::string task_queue = "hello-world";

  try {
    temporal::ClientOptions client_opts;
    client_opts.target = "localhost:7233";
    auto client = temporal::client::Client::Connect(client_opts);

    temporal::worker::Worker worker(client, task_queue);
    worker.RegisterWorkflow("GreetWorkflow", GreetWorkflow);
    worker.RegisterActivity("ComposeGreeting", ComposeGreeting);
    worker.Start();

    temporal::StartWorkflowOptions wf_opts;
    wf_opts.task_queue = task_queue;
    auto handle =
        client.StartWorkflow(wf_opts, "GreetWorkflow", std::string("Temporal"));
    std::cout << "started workflow id=" << handle.id() << " run_id=" << handle.run_id()
              << '\n';

    const std::string result = handle.Result<std::string>();
    std::cout << "workflow result: " << result << '\n';

    worker.Stop();
    return result == "Hello, Temporal!" ? 0 : 1;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    std::cerr << "(is a Temporal dev server running on localhost:7233? "
                 "start one with: temporal server start-dev)\n";
    return 2;
  }
}
