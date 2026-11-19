#include "sync_request.hpp"

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  SyncRequest::SyncRequest(const Uuid uuid, entity::EntityMessage entity, const std::int32_t from_id)
    : Message{MessageType::kSyncRequest, uuid}
      , _entity{std::move(entity)}
      , _from_id{from_id}
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

  void SyncRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    _entity.encode(writer);
    writer.write_i32(_from_id);
  }

  std::unique_ptr<SyncRequest> SyncRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto entity = entity::EntityMessage::decode(reader);
    return std::make_unique < SyncRequest > (uuid, std::move(entity), reader.read_i32());
  }
}
