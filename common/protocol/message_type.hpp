#pragma once
#include <cstdint>

namespace cipple::message
{
  // 消息类型与字段见 doc/behavior.md。
  enum class MessageType : std::uint8_t
  {
    kHeartbeatRequest = 0,
    kHeartbeatResponse = 1,
    kSyncRequest = 2,
    kSyncResponse = 3,
    kDeleteRequest = 4,
    kDeleteResponse = 5,
    kGetRequest = 6,
    kGetResponse = 7,
    kPutRequest = 8,
    kPutResponse = 9,
    kSubscribeRequest = 10,
    kSubscribeResponse = 11,
    kUnsubscribeRequest = 12,
    kUnsubscribeResponse = 13,
    kIncrementalUpdateRequest = 14,
    kIncrementalUpdateResponse = 15,
    kDispatchRequest = 16,
    kDispatchResponse = 17,
    kRetryResponse = 18,
    kHistoryLostResponse = 19,
    kWrongShardResponse = 20,
  };

  std::uint8_t to_byte(MessageType type) noexcept;
}
