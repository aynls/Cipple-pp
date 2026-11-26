cmake_minimum_required(VERSION 3.28)
project(Cipple__ LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Boost CONFIG REQUIRED COMPONENTS asio)
find_package(unofficial-sqlite3 CONFIG REQUIRED)

# The pinned vcpkg port disables TLS on Windows, but its NuRaftConfig.cmake
# still looks for OpenSSL. Import the port's exported static target directly.
include("${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/share/nuraft/NuRaftTargets.cmake")

enable_testing()

add_subdirectory(common)
add_subdirectory(server)
add_subdirectory(client)
add_subdirectory(tests)
add_subdirectory(bench)

if (MSVC)
    foreach (target IN ITEMS
            cipple_common
            cipple_server_lib
            cipple_client_lib
            cipple_server
            cipple_client
            cipple_tests
            cipple_bench
    )
        target_compile_options(${target} PRIVATE /utf-8)
    endforeach ()
endif ()
