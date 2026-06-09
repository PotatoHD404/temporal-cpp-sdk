file(GLOB_RECURSE TEMPORAL_SDK_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library(temporal_sdk STATIC ${TEMPORAL_SDK_SOURCES})
add_library(temporal::sdk ALIAS temporal_sdk)

target_include_directories(temporal_sdk
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
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
# Warning flags are compiler-specific: -Wall/-Wextra are GNU/Clang spellings and
# are rejected by MSVC's cl.exe, so gate them on the compiler family.
target_compile_options(temporal_sdk PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra>)

if(TEMPORAL_ENABLE_CLANG_TIDY)
  find_program(CLANG_TIDY_EXECUTABLE clang-tidy)
  if(CLANG_TIDY_EXECUTABLE)
    set_target_properties(temporal_sdk PROPERTIES
      CXX_CLANG_TIDY "${CLANG_TIDY_EXECUTABLE}")
  endif()
endif()

# ---- Install (opt-out via -DTEMPORAL_INSTALL=OFF) --------------------------
if(TEMPORAL_INSTALL)
  # Export under the in-tree alias name (temporal::sdk); without this the export
  # set would namespace the raw target as temporal::temporal_sdk.
  set_target_properties(temporal_sdk PROPERTIES EXPORT_NAME sdk)
  install(TARGETS temporal_sdk
    EXPORT temporal-cpp-sdk-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
