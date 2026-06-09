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
# The TestService protos (time-skipping test server) are not part of the pinned
# temporalio/api submodule, so they are vendored under a sibling import root.
set(TEMPORAL_TESTSVC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/testservice_api")
set(TEMPORAL_PROTO_GEN_DIR "${CMAKE_BINARY_DIR}/generated")

if(NOT EXISTS "${TEMPORAL_API_ROOT}/temporal/api/workflowservice/v1/service.proto")
  message(FATAL_ERROR
    "temporalio/api submodule not found at ${TEMPORAL_API_ROOT}.\n"
    "Run:  git submodule update --init --recursive")
endif()

# Resolve `protoc` and `grpc_cpp_plugin` cross-platform.
#
# Both Homebrew's and Conan's CMake config packages export imported executable
# targets (protobuf::protoc, gRPC::grpc_cpp_plugin); prefer those so the tools
# come from whatever provided the libraries (no hardcoded paths; works on
# Linux/Windows/macOS). Fall back to find_program for plain system installs that
# ship only the binaries, keeping the Homebrew hints for the macOS quick-path.
#
# Protos are generated at configure time via execute_process(), where generator
# expressions like $<TARGET_FILE:...> are NOT evaluated. So read the imported
# target's on-disk location (IMPORTED_LOCATION[_<CONFIG>]) directly instead.
function(_temporal_imported_exe _target _out_var)
  get_target_property(_loc "${_target}" IMPORTED_LOCATION)
  if(NOT _loc)
    get_target_property(_cfgs "${_target}" IMPORTED_CONFIGURATIONS)
    foreach(_cfg IN LISTS _cfgs)
      get_target_property(_loc "${_target}" "IMPORTED_LOCATION_${_cfg}")
      if(_loc)
        break()
      endif()
    endforeach()
  endif()
  set(${_out_var} "${_loc}" PARENT_SCOPE)
endfunction()

if(TARGET protobuf::protoc)
  _temporal_imported_exe(protobuf::protoc PROTOC_EXECUTABLE)
endif()
if(NOT PROTOC_EXECUTABLE)
  find_program(PROTOC_EXECUTABLE protoc
    HINTS "${HOMEBREW_PREFIX}/bin" /opt/homebrew/bin /usr/local/bin REQUIRED)
endif()

if(TARGET gRPC::grpc_cpp_plugin)
  _temporal_imported_exe(gRPC::grpc_cpp_plugin GRPC_CPP_PLUGIN_EXECUTABLE)
endif()
if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
  find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin
    HINTS "${HOMEBREW_PREFIX}/bin" /opt/homebrew/bin /usr/local/bin REQUIRED)
endif()

file(MAKE_DIRECTORY "${TEMPORAL_PROTO_GEN_DIR}")

# All temporal protos + the handful of vendored option/annotation protos they
# reference (these are NOT part of libprotobuf, so they must be generated).
file(GLOB_RECURSE _temporal_protos RELATIVE "${TEMPORAL_API_ROOT}"
     "${TEMPORAL_API_ROOT}/temporal/*.proto")
# Vendored TestService protos (import-relative paths, resolved via the extra root).
file(GLOB_RECURSE _testsvc_protos RELATIVE "${TEMPORAL_TESTSVC_ROOT}"
     "${TEMPORAL_TESTSVC_ROOT}/temporal/*.proto")
set(_extra_protos
  google/api/annotations.proto
  google/api/http.proto
  nexusannotations/v1/options.proto)
set(_all_protos ${_temporal_protos} ${_testsvc_protos} ${_extra_protos})

# Generate once per build tree. `cmake --fresh` (or deleting build/generated)
# forces regeneration if the pinned submodule moves.
if(NOT EXISTS "${TEMPORAL_PROTO_GEN_DIR}/temporal/api/workflowservice/v1/service.pb.h"
   OR NOT EXISTS "${TEMPORAL_PROTO_GEN_DIR}/temporal/api/testservice/v1/service.grpc.pb.h")
  list(LENGTH _all_protos _n)
  message(STATUS "temporal-cpp-sdk: generating C++ from ${_n} protos (this runs once)...")
  execute_process(
    COMMAND "${PROTOC_EXECUTABLE}" -I "${TEMPORAL_API_ROOT}" -I "${TEMPORAL_TESTSVC_ROOT}"
            --cpp_out "${TEMPORAL_PROTO_GEN_DIR}" ${_all_protos}
    WORKING_DIRECTORY "${TEMPORAL_API_ROOT}"
    RESULT_VARIABLE _cpp_rc ERROR_VARIABLE _cpp_err)
  if(NOT _cpp_rc EQUAL 0)
    message(FATAL_ERROR "protoc --cpp_out failed (rc=${_cpp_rc}):\n${_cpp_err}")
  endif()
  execute_process(
    COMMAND "${PROTOC_EXECUTABLE}" -I "${TEMPORAL_API_ROOT}" -I "${TEMPORAL_TESTSVC_ROOT}"
            --grpc_out "${TEMPORAL_PROTO_GEN_DIR}"
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
            temporal/api/workflowservice/v1/service.proto
            temporal/api/operatorservice/v1/service.proto
            temporal/api/testservice/v1/service.proto
    WORKING_DIRECTORY "${TEMPORAL_API_ROOT}"
    RESULT_VARIABLE _grpc_rc ERROR_VARIABLE _grpc_err)
  if(NOT _grpc_rc EQUAL 0)
    message(FATAL_ERROR "protoc --grpc_out failed (rc=${_grpc_rc}):\n${_grpc_err}")
  endif()
endif()

file(GLOB_RECURSE _gen_srcs CONFIGURE_DEPENDS "${TEMPORAL_PROTO_GEN_DIR}/*.pb.cc")

add_library(temporal_proto STATIC ${_gen_srcs})
target_include_directories(temporal_proto SYSTEM PUBLIC
  $<BUILD_INTERFACE:${TEMPORAL_PROTO_GEN_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(temporal_proto PUBLIC
  protobuf::libprotobuf gRPC::grpc++)
# Generated code is third-party-ish: never lint it, silence its warnings.
# MSVC spells "suppress everything" as /w, GNU/Clang as -w.
target_compile_options(temporal_proto PRIVATE
  $<IF:$<CXX_COMPILER_ID:MSVC>,/w,-w>)
set_target_properties(temporal_proto PROPERTIES CXX_CLANG_TIDY "")

# ---- Install (opt-out via -DTEMPORAL_INSTALL=OFF) --------------------------
# The SDK static archive links temporal_proto PUBLIC, so a downstream consumer
# of the installed temporal::sdk must also be able to find temporal_proto and
# the generated headers it exposes. Ship both as part of the export set.
if(TEMPORAL_INSTALL)
  # Export as temporal::proto (see temporal-cpp-sdk-config.cmake.in), not the raw
  # temporal::temporal_proto the namespace would otherwise produce.
  set_target_properties(temporal_proto PROPERTIES EXPORT_NAME proto)
  install(TARGETS temporal_proto
    EXPORT temporal-cpp-sdk-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

  # Generated *.pb.h / *.grpc.pb.h, preserving the temporal/… directory layout.
  install(DIRECTORY "${TEMPORAL_PROTO_GEN_DIR}/"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING
      PATTERN "*.pb.h"
      PATTERN "*.grpc.pb.h")
endif()
