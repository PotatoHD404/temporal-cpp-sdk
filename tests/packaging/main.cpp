// Downstream consumer smoke test: include a public header and reference a
// library-defined symbol so the linker must resolve temporal::sdk. The call is
// guarded by an impossible condition so the program never dials a server when
// run — building + linking is the whole point.
#include <temporal/client/client.h>

int main(int argc, char** /*argv*/) {
  if (argc > 9999) {
    (void)temporal::client::Client::Connect();
  }
  return 0;
}
