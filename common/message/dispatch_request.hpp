#pragma once
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "entity/entity_message.hpp"
#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(18) + uuid + clientListSignature + 实体公共段
  // server -> client 分发：携带一条实体消息；entity().uuid() 即线上 messageUuid
  class DispatchRequest final : public Message
  {
  public:
    DispatchRequest(Uuid uuid, std::string client_list_signature, entity::EntityMessage entity,
                    std::int64_t sequence = 0, std::optional<entity::EntityMessage> base = std::nullopt);

    const std::string& client_list_signature() const noexcept;
    const entity::EntityMessage& entity() const noexcept;
    std::int64_t sequence() const noexcept;
    const std::optional<entity::EntityMessage>& base() const noexcept;

    static std::unique_ptr<DispatchRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _client_list_signature;
    entity::EntityMessage _entity;
    std::int64_t _sequence;
    std::optional<entity::EntityMessage> _base;
  };
}
