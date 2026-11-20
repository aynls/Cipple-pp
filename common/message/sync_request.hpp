#pragma once
#include <cstdint>
#include <memory>
#include <optional>

#include "entity/entity_message.hpp"
#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(4) + uuid + 实体公共段 + int32 fromId
  // server 间同步：携带一条实体消息；entity().uuid() 即线上 messageUuid
  class SyncRequest final : public Message
  {
  public:
    SyncRequest(Uuid uuid, entity::EntityMessage entity, std::int32_t from_id,
                std::optional<entity::EntityMessage> base = std::nullopt);

    const entity::EntityMessage& entity() const noexcept;
    std::int32_t from_id() const noexcept;
    const std::optional<entity::EntityMessage>& base() const noexcept;

    static std::unique_ptr<SyncRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    entity::EntityMessage _entity;
    std::int32_t _from_id;
    std::optional<entity::EntityMessage> _base;
  };
}
