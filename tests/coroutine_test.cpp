#include <stdexcept>

#include <gtest/gtest.h>

#include "internal/coroutine.h"

namespace {

using temporal::internal::Coroutine;

TEST(Coroutine, RunsToCompletion) {
  int x = 0;
  Coroutine c([&] { x = 1; });
  c.Resume();
  EXPECT_TRUE(c.Done());
  EXPECT_EQ(x, 1);
}

TEST(Coroutine, YieldsAndResumesPreservingState) {
  Coroutine* self = nullptr;
  int step = 0;
  Coroutine c([&] {
    step = 1;
    self->Yield();
    step = 2;
    self->Yield();
    step = 3;
  });
  self = &c;

  EXPECT_EQ(step, 0);  // body does not start until the first Resume
  c.Resume();
  EXPECT_EQ(step, 1);
  EXPECT_FALSE(c.Done());
  c.Resume();
  EXPECT_EQ(step, 2);
  EXPECT_FALSE(c.Done());
  c.Resume();
  EXPECT_EQ(step, 3);
  EXPECT_TRUE(c.Done());
  c.Resume();  // no-op once done
  EXPECT_TRUE(c.Done());
}

TEST(Coroutine, PropagatesEscapedException) {
  Coroutine c([] { throw std::runtime_error("boom"); });
  c.Resume();
  EXPECT_TRUE(c.Done());
  EXPECT_THROW(c.RethrowIfError(), std::runtime_error);
}

TEST(Coroutine, TeardownUnwindsSuspendedStack) {
  bool cleaned = false;
  {
    Coroutine* self = nullptr;
    Coroutine c([&] {
      struct Guard {
        bool* flag;
        ~Guard() { *flag = true; }
      } guard{&cleaned};
      self->Yield();  // suspend here; destruction must unwind past `guard`
      self->Yield();
    });
    self = &c;
    c.Resume();
    EXPECT_FALSE(c.Done());
    EXPECT_FALSE(cleaned);
    // `c` is destroyed here, aborting the suspended coroutine.
  }
  EXPECT_TRUE(cleaned);
}

}  // namespace
