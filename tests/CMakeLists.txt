find_package(GTest CONFIG REQUIRED)

add_executable(cipple_tests
        main.cpp
        common/buffer_reader_test.cpp
        common/buffer_writer_test.cpp
        common/message_protocol_test.cpp
        common/tcp_transport_test.cpp
        server/raft_persistence_test.cpp
        server/sqlite_storage_test.cpp
        client/subscription_store_test.cpp
        integration/single_server_test.cpp
)

target_link_libraries(cipple_tests PRIVATE
        cipple_common
        cipple_server_lib
        cipple_client_lib
        GTest::gtest_main
        NuRaft::static_lib
        unofficial::sqlite3::sqlite3
)

include(GoogleTest)
gtest_discover_tests(cipple_tests)
