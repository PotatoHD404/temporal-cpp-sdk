cmake_minimum_required(VERSION 3.21)
project(temporal_cpp VERSION 0.1.0 LANGUAGES CXX
        DESCRIPTION "Native C++ SDK for Temporal (experimental)")

# ---- C++ standard ----------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(TEMPORAL_BUILD_EXAMPLES "Build examples" ON)
option(TEMPORAL_BUILD_TESTS "Build unit tests" ON)
option(TEMPORAL_ENABLE_CLANG_TIDY "Run clang-tidy on first-party sources" OFF)

# ---- Dependencies ----------------------------------------------------------
# Homebrew keeps grpc/protobuf/nlohmann-json config packages under its prefix.
if(NOT HOMEBREW_PREFIX)
  find_program(BREW_EXECUTABLE brew)
  if(BREW_EXECUTABLE)
    execute_process(COMMAND ${BREW_EXECUTABLE} --prefix
                    OUTPUT_VARIABLE HOMEBREW_PREFIX
                    OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
  endif()
  if(NOT HOMEBREW_PREFIX AND EXISTS "/opt/homebrew")
    set(HOMEBREW_PREFIX "/opt/homebrew")
  endif()
endif()
if(HOMEBREW_PREFIX)
  list(PREPEND CMAKE_PREFIX_PATH "${HOMEBREW_PREFIX}")
endif()

find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(Threads REQUIRED)

# ---- Generated Temporal API protobufs/gRPC ---------------------------------
include(cmake/TemporalProto.cmake)   # -> target temporal_proto

# ---- The SDK library -------------------------------------------------------
add_subdirectory(src)                # -> target temporal_sdk / temporal::sdk

if(TEMPORAL_BUILD_EXAMPLES)
  add_subdirectory(examples/hello_world)
endif()

if(TEMPORAL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

message(STATUS "temporal-cpp: protobuf=${protobuf_VERSION} grpc=${gRPC_VERSION} "
               "build=${CMAKE_BUILD_TYPE}")
