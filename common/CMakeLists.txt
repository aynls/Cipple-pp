add_library(cipple_common STATIC
        common.cpp
        uuid.cpp
        entity/atomic_operation.cpp
        entity/entity_message.cpp
        entity/operation_type.cpp
        entity/resolve.cpp
        hlc/hybrid_clock.cpp
        message/delete_request.cpp
        message/delete_response.cpp
        message/dispatch_request.cpp
        message/dispatch_response.cpp
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
        message/unsubscribe_request.cpp
        message/unsubscribe_response.cpp
        storage/sqlite_storage.cpp
        storage/storage.cpp
        storage/storage_writer.cpp
        tcp/buffer_reader.cpp
        tcp/buffer_writer.cpp
        tcp/connector.cpp
        tcp/frame_codec.cpp
        tcp/message_decoder.cpp
        tcp/session.cpp
)

target_include_directories(cipple_common PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(cipple_common
        PUBLIC Boost::asio
        PRIVATE SQLite3::SQLite3
)

if (MINGW)
    target_link_libraries(cipple_common PUBLIC ws2_32 mswsock)
endif ()
