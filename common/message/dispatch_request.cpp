#include "dispatch_request.hpp"

#include <stdexcept>
#include <utility>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  DispatchRequest::DispatchRequest(const Uuid uuid, std::string client_list_signature,
                                   entity::EntityMessage entity, const std::int64_t sequence,
                                   std::optional<entity::EntityMessage> base)
    : Message{MessageType::kDispatchRequest, uuid}
      , _client_list_signature{std::move(client_list_signature)}
      , _entity{std::move(entity)}
      , _sequence{sequence}
      , _base{std::move(base)}
  {
    if (_sequence < 0)
    {
      throw std::invalid_argument{"negative dispatch sequence"};
    }
  }

  const std::string& DispatchRequest::client_list_signature() const noexcept
  {
    return _client_list_signature;
  }

  const entity::EntityMessage& DispatchRequest::entity() const noexcept
  {
    return _entity;
  }

  std::int64_t DispatchRequest::sequence() const noexcept
  {
    return _sequence;
  }

  const std::optional<entity::EntityMessage>& DispatchRequest::base() const noexcept
  {
    return _base;
  }

  void DispatchRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_client_list_signature);
    _entity.encode(writer);
    if (_sequence != 0 || _base)
    {
      writer.write_i64(_sequence);
      if (_base)
      {
        _base->encode(writer);
      }
    }
  }

  std::unique_ptr<DispatchRequest> DispatchRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto client_list_signature = reader.read_string();
    auto entity = entity::EntityMessage::decode(reader);
    std::int64_t sequence = 0;
    std::optional<entity::EntityMessage> base;
    if (reader.remaining() != 0)
    {
      sequence = reader.read_i64();
      if (reader.remaining() != 0)
      {
        base = entity::EntityMessage::decode(reader);
      }
    }
    return std::make_unique < DispatchRequest > (uuid, std::move(client_list_signature),
      std::move(entity), sequence, std::move(base));
  }
}
