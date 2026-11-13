add_library(cipple_common STATIC
        common.cpp
        tcp/buffer_writer.cpp
)

target_include_directories(cipple_common PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)