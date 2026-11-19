cmake_minimum_required(VERSION 3.28)
project(Cipple__ LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(SQLite3 3.24 REQUIRED)
if (NOT TARGET SQLite3::SQLite3)
    add_library(SQLite3::SQLite3 ALIAS SQLite::SQLite3)
endif ()

enable_testing()

add_subdirectory(common)
add_subdirectory(server)
add_subdirectory(client)
add_subdirectory(tests)

if (MSVC)
    foreach (target IN ITEMS
            cipple_common
            cipple_server_lib
            cipple_client_lib
            cipple_server
            cipple_client
            cipple_tests
    )
        target_compile_options(${target} PRIVATE /utf-8)
    endforeach ()
endif ()
