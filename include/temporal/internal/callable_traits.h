#pragma once

#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>

// Shared compile-time helpers for adapting plain user callables (workflow,
// activity, and query handlers) to/from payloads.
namespace temporal::internal {

// Extract the return type and (decayed) parameter types of a callable.
template <class T>
struct fn_sig : fn_sig<decltype(&std::decay_t<T>::operator())> {};
template <class R, class... A>
struct fn_sig<R (*)(A...)> {
  using ret = R;
  using args = std::tuple<std::decay_t<A>...>;
};
template <class C, class R, class... A>
struct fn_sig<R (C::*)(A...) const> {
  using ret = R;
  using args = std::tuple<std::decay_t<A>...>;
};
template <class C, class R, class... A>
struct fn_sig<R (C::*)(A...)> {
  using ret = R;
  using args = std::tuple<std::decay_t<A>...>;
};

// Drop the first tuple element (e.g. the Context& parameter of workflows/activities).
template <class Tuple>
struct tuple_tail;
template <class Head, class... Tail>
struct tuple_tail<std::tuple<Head, Tail...>> {
  using type = std::tuple<Tail...>;
};

// Decode payloads[0..N-1] into a tuple of the given argument types.
template <class Tuple, std::size_t... I>
Tuple DecodeArgs(const DataConverter& dc, const Payloads& in, std::index_sequence<I...>) {
  return Tuple{dc.FromPayload<std::tuple_element_t<I, Tuple>>(in.at(I))...};
}

}  // namespace temporal::internal
