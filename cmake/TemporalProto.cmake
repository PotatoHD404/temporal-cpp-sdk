# TemporalProto.cmake
#
# Generates C++ message + gRPC stubs from the vendored `temporalio/api` protos
# (third_party/api submodule) and exposes them as the `temporal_proto` target.
#
# A single `-I third_party/api` import root resolves every import the temporal
# protos use: `temporal/...`, the vendored `google/...`, and `nexusannotations/...`.
# Well-known types (`google/protobuf/*`) are NOT regenerated — we link the system
# libprotobuf and pick up its headers — to avoid duplicate-symbol clashes.

set(TEMPORAL_API_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/api")
set(TEMPORAL_PROTO_GEN_DIR "${CMAKE_BINARY_DIR}/generated")

if(NOT EXISTS "${TEMPORAL_API_ROOT}/temporal/api/workflowservice/v1/service.proto")
  message(FATAL_ERROR
    "temporalio/api submodule not found at ${TEMPORAL_API_ROOT}.\n"
    "Run:  git submodule update --init --recursive")
endif()

find_program(PROTOC_EXECUTABLE protoc
  HINTS "${HOMEBREW_PREFIX}/bin" /opt/homebrew/bin /usr/local/bin REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin
  HINTS "${HOMEBREW_PREFIX}/bin" /opt/homebrew/bin /usr/local/bin REQUIRED)

file(MAKE_DIRECTORY "${TEMPORAL_PROTO_GEN_DIR}")

# All temporal protos + the handful of vendored option/annotation protos they
# reference (these are NOT part of libprotobuf, so they must be generated).
file(GLOB_RECURSE _temporal_protos RELATIVE "${TEMPORAL_API_ROOT}"
     "${TEMPORAL_API_ROOT}/temporal/*.proto")
set(_extra_protos
  google/api/annotations.proto
  google/api/http.proto
  nexusannotations/v1/options.proto)
set(_all_protos ${_temporal_protos} ${_extra_protos})

# Generate once per build tree. `cmake --fresh` (or deleting build/generated)
# forces regeneration if the pinned submodule moves.
if(NOT EXISTS "${TEMPORAL_PROTO_GEN_DIR}/temporal/api/workflowservice/v1/service.pb.h")
  list(LENGTH _all_protos _n)
  message(STATUS "temporal-cpp: generating C++ from ${_n} protos (this runs once)...")
  execute_process(
    COMMAND "${PROTOC_EXECUTABLE}" -I "${TEMPORAL_API_ROOT}"
            --cpp_out "${TEMPORAL_PROTO_GEN_DIR}" ${_all_protos}
    WORKING_DIRECTORY "${TEMPORAL_API_ROOT}"
    RESULT_VARIABLE _cpp_rc ERROR_VARIABLE _cpp_err)
  if(NOT _cpp_rc EQUAL 0)
    message(FATAL_ERROR "protoc --cpp_out failed (rc=${_cpp_rc}):\n${_cpp_err}")
  endif()
  execute_process(
    COMMAND "${PROTOC_EXECUTABLE}" -I "${TEMPORAL_API_ROOT}"
            --grpc_out "${TEMPORAL_PROTO_GEN_DIR}"
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
            temporal/api/workflowservice/v1/service.proto
    WORKING_DIRECTORY "${TEMPORAL_API_ROOT}"
    RESULT_VARIABLE _grpc_rc ERROR_VARIABLE _grpc_err)
  if(NOT _grpc_rc EQUAL 0)
    message(FATAL_ERROR "protoc --grpc_out failed (rc=${_grpc_rc}):\n${_grpc_err}")
  endif()
endif()

file(GLOB_RECURSE _gen_srcs CONFIGURE_DEPENDS "${TEMPORAL_PROTO_GEN_DIR}/*.pb.cc")

add_library(temporal_proto STATIC ${_gen_srcs})
target_include_directories(temporal_proto SYSTEM PUBLIC
  "${TEMPORAL_PROTO_GEN_DIR}")
target_link_libraries(temporal_proto PUBLIC
  protobuf::libprotobuf gRPC::grpc++)
# Generated code is third-party-ish: never lint it, silence its warnings.
target_compile_options(temporal_proto PRIVATE -w)
set_target_properties(temporal_proto PROPERTIES CXX_CLANG_TIDY "")
