#pragma once
#include <cstdint>
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(2) + uuid + messageUuid + int32 sourceId + int32 nodeId
  // server 间回执：nodeId 声明自己已持久化来自 sourceId 的实体 messageUuid
  class AckRequest final : public Message
  {
  public:
    AckRequest(Uuid uuid, Uuid message_uuid, std::int32_t source_id, std::int32_t node_id);

    const Uuid& message_uuid() const noexcept;
    std::int32_t source_id() const noexcept;
    std::int32_t node_id() const noexcept;

    static std::unique_ptr<AckRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    Uuid _message_uuid;
    std::int32_t _source_id;
    std::int32_t _node_id;
  };
}
