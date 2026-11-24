#pragma once
#include <cstdint>
#include <memory>
#include <optional>

#include "entity/entity_message.hpp"
#include "protocol/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // 副本同步携带实体、发送节点 ID 和增量更新引用的完整基底。
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
