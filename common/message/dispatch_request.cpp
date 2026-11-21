#include "dispatch_request.hpp"

#include <stdexcept>
#include <utility>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  DispatchRequest::DispatchRequest(const Uuid uuid, entity::EntityMessage entity, const std::int64_t sequence,
                                   std::optional<entity::EntityMessage> base)
    : Message{MessageType::kDispatchRequest, uuid}
      , _entity{std::move(entity)}
      , _sequence{sequence}
      , _base{std::move(base)}
  {
    if (_sequence < 0)
    {
      throw std::invalid_argument{"negative dispatch sequence"};
    }
    if (_base.has_value() != (_entity.operation_type() == entity::OperationType::kIncrementalUpdate))
    {
      throw std::invalid_argument{"dispatch must carry a base for incremental updates only"};
    }
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
    _entity.encode(writer);
    writer.write_i64(_sequence);
    if (_base)
    {
      _base->encode(writer);
    }
  }

  std::unique_ptr<DispatchRequest> DispatchRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto entity = entity::EntityMessage::decode(reader);
    const auto sequence = reader.read_i64();
    std::optional<entity::EntityMessage> base;
    if (entity.operation_type() == entity::OperationType::kIncrementalUpdate)
    {
      base = entity::EntityMessage::decode(reader);
    }
    return std::make_unique<DispatchRequest>(uuid, std::move(entity), sequence, std::move(base));
  }
}
