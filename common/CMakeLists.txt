add_library(cipple_common STATIC
        common.cpp
        uuid.cpp
        entity/atomic_operation.cpp
        entity/entity_message.cpp
        entity/operation_type.cpp
        message/ack_request.cpp
        message/ack_response.cpp
        message/delete_request.cpp
        message/delete_response.cpp
        message/dispatch_request.cpp
        message/dispatch_response.cpp
        message/get_client_list_request.cpp
        message/get_client_list_response.cpp
        message/get_request.cpp
        message/get_response.cpp
        message/heartbeat_request.cpp
        message/heartbeat_response.cpp
        message/incremental_update_request.cpp
        message/incremental_update_response.cpp
        message/message.cpp
        message/message_type.cpp
        message/put_request.cpp
        message/put_response.cpp
        message/subscribe_request.cpp
        message/subscribe_response.cpp
        message/sync_request.cpp
        message/sync_response.cpp
        message/system_info_request.cpp
        message/system_info_response.cpp
        message/unsubscribe_request.cpp
        message/unsubscribe_response.cpp
        storage/sqlite_storage.cpp
        storage/storage.cpp
        tcp/buffer_reader.cpp
        tcp/buffer_writer.cpp
        tcp/frame_codec.cpp
        tcp/message_decoder.cpp
)

target_include_directories(cipple_common PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(cipple_common PRIVATE SQLite3::SQLite3)
