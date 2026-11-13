#pragma once
#include <cstdint>
#include <optional>

namespace cipple::message
{
  // 线上协议消息类型，编号沿用原版协议（任务书 §5）
  enum class MessageType : std::uint8_t
  {
    kHeartbeatRequest = 0,
    kHeartbeatResponse = 1,
    kAckRequest = 2,
    kAckResponse = 3,
    kSyncRequest = 4,
    kSyncResponse = 5,
    kDeleteRequest = 6,
    kDeleteResponse = 7,
    kGetRequest = 8,
    kGetResponse = 9,
    kPutRequest = 10,
    kPutResponse = 11,
    kSubscribeRequest = 12,
    kSubscribeResponse = 13,
    kUnsubscribeRequest = 14,
    kUnsubscribeResponse = 15,
    kIncrementalUpdateRequest = 16,
    kIncrementalUpdateResponse = 17,
    kDispatchRequest = 18,
    kDispatchResponse = 19,
    kGetClientListRequest = 20,
    kGetClientListResponse = 21,
    kSystemInfoRequest = 22,
    kSystemInfoResponse = 23,
  };

  std::uint8_t to_byte(MessageType type) noexcept;

  // 线上字节 -> 枚举；未知值返回 nullopt（解码入口的协议校验）
  std::optional<MessageType> message_type_from_byte(std::uint8_t value) noexcept;
}
