include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.15.2)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp")
add_executable(temporal_tests ${TEST_SOURCES})
target_link_libraries(temporal_tests PRIVATE temporal::sdk GTest::gtest_main)

# Register the whole suite as one ctest entry. We deliberately avoid
# gtest_discover_tests' build-time enumeration: the binary statically links the
# large generated proto/gRPC archive, whose descriptor static-initialization can
# exceed the discovery timeout on a cold first run.
add_test(NAME temporal_tests COMMAND temporal_tests)
