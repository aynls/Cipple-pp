add_library(cipple_common STATIC
        common.cpp
        uuid.cpp
        entity/atomic_operation.cpp
        entity/entity_message.cpp
        entity/operation_type.cpp
        message/message.cpp
        message/message_type.cpp
        tcp/buffer_reader.cpp
        tcp/buffer_writer.cpp
)

target_include_directories(cipple_common PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)