add_library(cipple_server_lib STATIC
        src/server.cpp
)

target_link_libraries(cipple_server_lib PUBLIC cipple_common)

add_executable(cipple_server main.cpp)
target_link_libraries(cipple_server PRIVATE cipple_server_lib)
