add_library(cipple_common STATIC
        uuid.cpp
        binary/buffer_reader.cpp
        binary/buffer_writer.cpp
        entity/atomic_operation.cpp
        entity/entity_message.cpp
        entity/operation_type.cpp
        entity/resolve.cpp
        protocol/cluster/history_lost_response.cpp
        protocol/cluster/retry_response.cpp
        protocol/cluster/wrong_shard_response.cpp
        protocol/configuration/delete_request.cpp
        protocol/configuration/delete_response.cpp
        protocol/configuration/get_request.cpp
        protocol/configuration/get_response.cpp
        protocol/configuration/incremental_update_request.cpp
        protocol/configuration/incremental_update_response.cpp
        protocol/configuration/put_request.cpp
        protocol/configuration/put_response.cpp
        protocol/frame_codec.cpp
        protocol/heartbeat/heartbeat_request.cpp
        protocol/heartbeat/heartbeat_response.cpp
        protocol/message.cpp
        protocol/message_decoder.cpp
        protocol/message_type.cpp
        protocol/replication/sync_request.cpp
        protocol/replication/sync_response.cpp
        protocol/subscription/dispatch_request.cpp
        protocol/subscription/dispatch_response.cpp
        protocol/subscription/subscribe_request.cpp
        protocol/subscription/subscribe_response.cpp
        protocol/subscription/unsubscribe_request.cpp
        protocol/subscription/unsubscribe_response.cpp
        sharding/shard_layout.cpp
        tcp/connector.cpp
        tcp/session.cpp
)

target_include_directories(cipple_common PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(cipple_common
        PUBLIC Boost::asio
)

if (MINGW)
    target_link_libraries(cipple_common PUBLIC ws2_32 mswsock)
endif ()
