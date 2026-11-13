include(FetchContent)

FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
FetchContent_MakeAvailable(googletest)

add_executable(cipple_tests
        main.cpp
        buffer_writer_test.cpp
)

target_link_libraries(cipple_tests PRIVATE
        cipple_common
        cipple_server_lib
        cipple_client_lib
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(cipple_tests)
