add_library(cipple_client_lib STATIC
        src/client.cpp
        src/subscription/subscription_store.cpp
)

target_include_directories(cipple_client_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cipple_client_lib PUBLIC cipple_common PRIVATE unofficial::sqlite3::sqlite3)

add_executable(cipple_client main.cpp)
target_link_libraries(cipple_client PRIVATE cipple_client_lib)
