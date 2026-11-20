#include "sync_request.hpp"

#include <utility>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  SyncRequest::SyncRequest(const Uuid uuid, entity::EntityMessage entity, const std::int32_t from_id,
                           std::optional<entity::EntityMessage> base)
    : Message{MessageType::kSyncRequest, uuid}
      , _entity{std::move(entity)}
      , _from_id{from_id}
      , _base{std::move(base)}
  {
  }

  const entity::EntityMessage& SyncRequest::entity() const noexcept
  {
    return _entity;
  }

  std::int32_t SyncRequest::from_id() const noexcept
  {
    return _from_id;
  }

  const std::optional<entity::EntityMessage>& SyncRequest::base() const noexcept
  {
    return _base;
  }

  void SyncRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    _entity.encode(writer);
    writer.write_i32(_from_id);
    if (_base)
    {
      _base->encode(writer);
    }
  }

  std::unique_ptr<SyncRequest> SyncRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto entity = entity::EntityMessage::decode(reader);
    const auto from_id = reader.read_i32();
    std::optional<entity::EntityMessage> base;
    if (reader.remaining() != 0)
    {
      base = entity::EntityMessage::decode(reader);
    }
    return std::make_unique < SyncRequest > (uuid, std::move(entity), from_id, std::move(base));
  }
}
