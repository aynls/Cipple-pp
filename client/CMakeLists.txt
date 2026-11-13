add_library(cipple_client_lib STATIC
        src/client.cpp
)

target_link_libraries(cipple_client_lib PUBLIC cipple_common)

add_executable(cipple_client main.cpp)
target_link_libraries(cipple_client PRIVATE cipple_client_lib)
