add_library(cipple_server_lib STATIC
        src/server.cpp
        src/raft/configuration_machine.cpp
        src/raft/raft_node.cpp
        src/raft/raft_persistence.cpp
        src/storage/sqlite_storage.cpp
        src/storage/storage.cpp
        src/subscription/subscriptions.cpp
)

target_include_directories(cipple_server_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cipple_server_lib
        PUBLIC cipple_common
        PRIVATE unofficial::sqlite3::sqlite3 NuRaft::static_lib bcrypt)

add_executable(cipple_server main.cpp)
target_link_libraries(cipple_server PRIVATE cipple_server_lib)
