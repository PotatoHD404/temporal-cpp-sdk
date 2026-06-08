# Prefer a GTest config package when one is provided (e.g. by Conan or a system
# install); otherwise fetch it. Both paths yield the GTest::gtest_main target.
find_package(GTest CONFIG QUIET)
if(NOT TARGET GTest::gtest_main)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  # Don't let GoogleTest contribute install rules to `cmake --install`.
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp")
add_executable(temporal_tests ${TEST_SOURCES})
target_link_libraries(temporal_tests PRIVATE temporal::sdk GTest::gtest_main)
# Unit tests may reach internal engine headers (e.g. the coroutine primitive).
target_include_directories(temporal_tests PRIVATE ${CMAKE_SOURCE_DIR}/src)

# Register the whole suite as one ctest entry. We deliberately avoid
# gtest_discover_tests' build-time enumeration: the binary statically links the
# large generated proto/gRPC archive, whose descriptor static-initialization can
# exceed the discovery timeout on a cold first run.
add_test(NAME temporal_tests COMMAND temporal_tests)

# Integration tests run end-to-end against a real Temporal server. They self-skip
# unless TEMPORAL_INTEGRATION=1 is set, and live in a separate binary + ctest
# label so `ctest -LE integration` stays server-free.
file(GLOB INTEGRATION_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/integration/*.cpp")
if(INTEGRATION_SOURCES)
  add_executable(temporal_integration_tests ${INTEGRATION_SOURCES})
  target_link_libraries(temporal_integration_tests PRIVATE temporal::sdk GTest::gtest_main)
  add_test(NAME temporal_integration_tests COMMAND temporal_integration_tests)
  # The suite runs end-to-end against a real server (each test is several seconds
  # of real workflow round-trips); it grows as features land, so keep a generous
  # ceiling well above the ~200s+ wall time on slower CI runners.
  set_tests_properties(temporal_integration_tests PROPERTIES TIMEOUT 600 LABELS integration)
endif()
