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
  // 实体、分发序号以及增量更新引用的完整基底。实时分发的序号为 0。
  class DispatchRequest final : public Message
  {
  public:
    DispatchRequest(Uuid uuid, entity::EntityMessage entity, std::int64_t sequence,
                    std::optional<entity::EntityMessage> base = std::nullopt);

    const entity::EntityMessage& entity() const noexcept;
    std::int64_t sequence() const noexcept;
    const std::optional<entity::EntityMessage>& base() const noexcept;

    static std::unique_ptr<DispatchRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    entity::EntityMessage _entity;
    std::int64_t _sequence;
    std::optional<entity::EntityMessage> _base;
  };
}
