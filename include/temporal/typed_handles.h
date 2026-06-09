#pragma once

#include <string_view>

#include <temporal/internal/callable_traits.h>

namespace temporal {

// A compile-time handle to an activity: it binds the Temporal activity *type name*
// to the C++ function that implements it, so registration and invocation share one
// source of truth. A misspelled name or a wrong argument type becomes a compile
// error instead of a silent runtime drop, and the result type is deduced (no
// explicit `<R>` at the call site). Declare one with TEMPORAL_ACTIVITY(fn) and
// pass it to Worker::Register and Context::ExecuteActivity.
//
// It lowers to the same string name the string-based API uses, so the two are
// fully interchangeable and replay identically.
template <auto Fn>
struct ActivityRef {
  std::string_view name;
  using result_type = typename internal::fn_sig<decltype(Fn)>::ret;
  static constexpr auto function = Fn;
};

}  // namespace temporal

// Declares `<fn>_activity` — an ActivityRef whose Temporal type name is "<fn>"
// (i.e. the function's own name). Put it at namespace scope next to the function.
#define TEMPORAL_ACTIVITY(fn) inline constexpr ::temporal::ActivityRef<&fn> fn##_activity{#fn}
