# Conan 2.x recipe for temporal-cpp.
#
# Lets you build on any platform Conan supports, with the C++ toolchain deps
# (gRPC, protobuf, nlohmann_json) resolved by Conan instead of a system package
# manager:
#
#   pip install conan
#   conan profile detect --force
#   git submodule update --init third_party/api      # protos are generated at build time
#   conan install . --build=missing
#   cmake --preset conan-release
#   cmake --build --preset conan-release
#
# Or package the SDK itself:  conan create . --build=missing
#
# NOTE: the protobuf/gRPC versions below are a known-compatible ConanCenter pair;
# bump them together if you need a newer toolchain (gRPC pins the protobuf it was
# built against).
from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps, cmake_layout


class TemporalCppConan(ConanFile):
    name = "temporal-cpp"
    version = "0.1.0"
    license = "MIT"
    url = "https://github.com/PotatoHD404/temporal-cpp"
    description = "Native C++ SDK for Temporal (own gRPC + coroutine replay engine)"
    topics = ("temporal", "workflow", "grpc", "durable-execution")

    settings = "os", "compiler", "build_type", "arch"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}

    # For `conan create`. The third_party/api submodule must be checked out at
    # export time (it carries the .proto files the build generates code from).
    exports_sources = (
        "CMakeLists.txt",
        "cmake/*",
        "src/*",
        "include/*",
        "third_party/*",
    )

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        # PUBLIC deps of the installed temporal::sdk archive: their headers and
        # symbols are part of the SDK's interface, so consumers need them too.
        self.requires("grpc/1.54.3", transitive_headers=True, transitive_libs=True)
        self.requires("protobuf/3.21.12", transitive_headers=True, transitive_libs=True)
        self.requires("nlohmann_json/3.11.3", transitive_headers=True)

    def build_requirements(self):
        # protoc + grpc_cpp_plugin must run on the build machine to generate the
        # service stubs (cmake/TemporalProto.cmake reads them from the imported
        # protobuf::protoc / gRPC::grpc_cpp_plugin targets CMakeDeps creates).
        self.tool_requires("protobuf/<host_version>")
        self.tool_requires("grpc/<host_version>")

    def generate(self):
        CMakeDeps(self).generate()
        tc = CMakeToolchain(self)
        # The SDK ships its own tests/examples drivers; a package build wants
        # neither. Consumers building from source can flip these back on.
        tc.variables["TEMPORAL_BUILD_TESTS"] = False
        tc.variables["TEMPORAL_BUILD_EXAMPLES"] = False
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        CMake(self).install()

    def package_info(self):
        # Match the in-tree CMake names so `find_package(temporal-cpp)` +
        # temporal::sdk works identically whether consumed via Conan or a plain
        # install tree.
        self.cpp_info.set_property("cmake_file_name", "temporal-cpp")
        self.cpp_info.set_property("cmake_target_name", "temporal::sdk")
        # Link order matters for static archives: the SDK depends on the protos.
        self.cpp_info.libs = ["temporal_sdk", "temporal_proto"]
        self.cpp_info.requires = [
            "grpc::grpc++",
            "protobuf::libprotobuf",
            "nlohmann_json::nlohmann_json",
        ]
