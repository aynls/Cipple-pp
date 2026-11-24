add_library(cipple_server_lib STATIC
        src/server.cpp
        src/clock/hybrid_clock.cpp
        src/replication/replication.cpp
        src/storage/sqlite_storage.cpp
        src/storage/storage.cpp
        src/storage/storage_writer.cpp
        src/subscription/subscriptions.cpp
)

target_include_directories(cipple_server_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cipple_server_lib PUBLIC cipple_common PRIVATE SQLite3::SQLite3)

add_executable(cipple_server main.cpp)
target_link_libraries(cipple_server PRIVATE cipple_server_lib)
