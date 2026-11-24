add_executable(cipple_bench
        bench_main.cpp
        codec_bench.cpp
        fanout_bench.cpp
        commit_bench.cpp
        wire_driver.cpp
)

target_include_directories(cipple_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(cipple_bench PRIVATE cipple_common cipple_server_lib)
