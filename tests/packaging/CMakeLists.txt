# Standalone downstream-consumer smoke test for the installed package.
#
# This is NOT part of the main build (the top-level tests/CMakeLists.txt globs
# only *_test.cpp). It is configured on its own against an install tree to prove
# `find_package(temporal-cpp)` + linking `temporal::sdk` works end to end:
#
#   cmake --install build --prefix /tmp/tcpp-install
#   cmake -S tests/packaging -B /tmp/consumer -DCMAKE_PREFIX_PATH=/tmp/tcpp-install
#   cmake --build /tmp/consumer
cmake_minimum_required(VERSION 3.20)
project(temporal_cpp_consumer CXX)

find_package(temporal-cpp CONFIG REQUIRED)

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE temporal::sdk)
