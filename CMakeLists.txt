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
option(TEMPORAL_INSTALL "Generate install + find_package(temporal-cpp) rules" ON)

include(GNUInstallDirs)

# ---- Dependencies ----------------------------------------------------------
# Homebrew keeps grpc/protobuf/nlohmann-json config packages under its prefix.
# This is the macOS quick-path only. When a toolchain file is supplied (e.g.
# Conan's CMakeToolchain, which provides the deps itself) we leave
# CMAKE_PREFIX_PATH alone so the Homebrew copies don't shadow the
# toolchain-provided packages.
if(APPLE AND NOT CMAKE_TOOLCHAIN_FILE)
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

# ---- Install + find_package(temporal-cpp CONFIG) ---------------------------
# Targets (temporal_sdk, temporal_proto) are added to the export set in their
# own CMakeLists. Here we install the public headers, the export set, and a
# generated package config so a downstream project can:
#
#   find_package(temporal-cpp CONFIG REQUIRED)
#   target_link_libraries(app PRIVATE temporal::sdk)
#
# Examples and tests are never installed.
if(TEMPORAL_INSTALL)
  include(CMakePackageConfigHelpers)

  set(TEMPORAL_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/temporal-cpp"
      CACHE STRING "Install location for temporal-cpp CMake package files")

  # Public headers: the whole include/temporal/ tree (the public API includes
  # temporal/internal/* helpers, so they ship too).
  install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/temporal"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp")

  install(EXPORT temporal-cpp-targets
    FILE temporal-cpp-targets.cmake
    NAMESPACE temporal::
    DESTINATION "${TEMPORAL_INSTALL_CMAKEDIR}")

  configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/temporal-cpp-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/temporal-cpp-config.cmake"
    INSTALL_DESTINATION "${TEMPORAL_INSTALL_CMAKEDIR}")

  write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/temporal-cpp-config-version.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMajorVersion)

  install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/temporal-cpp-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/temporal-cpp-config-version.cmake"
    DESTINATION "${TEMPORAL_INSTALL_CMAKEDIR}")

  # Let consumers in the same build tree use find_package(temporal-cpp) without
  # installing first.
  export(EXPORT temporal-cpp-targets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/temporal-cpp-targets.cmake"
    NAMESPACE temporal::)
endif()

message(STATUS "temporal-cpp: protobuf=${protobuf_VERSION} grpc=${gRPC_VERSION} "
               "build=${CMAKE_BUILD_TYPE}")
