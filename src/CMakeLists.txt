file(GLOB_RECURSE TEMPORAL_SDK_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library(temporal_sdk STATIC ${TEMPORAL_SDK_SOURCES})
add_library(temporal::sdk ALIAS temporal_sdk)

target_include_directories(temporal_sdk
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

# temporal_proto is PUBLIC so the static archive's symbols/include dirs propagate
# to the final link. Public SDK headers never include protobuf — that boundary is
# kept by convention (engine code lives under src/internal/).
target_link_libraries(temporal_sdk
  PUBLIC
    temporal_proto
    nlohmann_json::nlohmann_json
    Threads::Threads)

target_compile_features(temporal_sdk PUBLIC cxx_std_20)
target_compile_options(temporal_sdk PRIVATE -Wall -Wextra)

if(TEMPORAL_ENABLE_CLANG_TIDY)
  find_program(CLANG_TIDY_EXECUTABLE clang-tidy)
  if(CLANG_TIDY_EXECUTABLE)
    set_target_properties(temporal_sdk PROPERTIES
      CXX_CLANG_TIDY "${CLANG_TIDY_EXECUTABLE}")
  endif()
endif()
