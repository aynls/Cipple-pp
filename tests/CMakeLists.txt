include(FetchContent)

FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
        URL_HASH SHA256=f179ec217f9b3b3f3c6e8b02d3e7eda997b49e4ce26d6b235c9053bec9c0bf9f
)
FetchContent_MakeAvailable(googletest)

add_executable(cipple_tests
        main.cpp
        buffer_reader_test.cpp
        buffer_writer_test.cpp
        sqlite_storage_test.cpp
        tcp_transport_test.cpp
        single_server_test.cpp
)

target_link_libraries(cipple_tests PRIVATE
        cipple_common
        cipple_server_lib
        cipple_client_lib
        GTest::gtest_main
        SQLite3::SQLite3
)

include(GoogleTest)
gtest_discover_tests(cipple_tests)
