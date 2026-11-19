#include "entity_message.hpp"

#include <stdexcept>
#include <utility>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::entity
{
  EntityMessage::EntityMessage(const Uuid uuid, std::string application_name, std::string key,
                               const std::int64_t last_update, const std::int32_t last_update_server_id,
                               Payload payload)
    : _uuid{uuid}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
      , _last_update{last_update}
      , _last_update_server_id{last_update_server_id}
      , _payload{std::move(payload)}
  {
  }

  const Uuid& EntityMessage::uuid() const noexcept
  {
    return _uuid;
  }

  const std::string& EntityMessage::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& EntityMessage::key() const noexcept
  {
    return _key;
  }

  std::int64_t EntityMessage::last_update() const noexcept
  {
    return _last_update;
  }

  std::int32_t EntityMessage::last_update_server_id() const noexcept
  {
    return _last_update_server_id;
  }

  OperationType EntityMessage::operation_type() const noexcept
  {
    switch (_payload.index())
    {
    case 0:
      return OperationType::kUpdate;
    case 1:
      return OperationType::kIncrementalUpdate;
    default:
      return OperationType::kDelete;
    }
  }

  const EntityMessage::Payload& EntityMessage::payload() const noexcept
  {
    return _payload;
  }

  void EntityMessage::encode(tcp::BufferWriter& writer) const
  {
    writer.write_bytes(_uuid.bytes());
    writer.write_string(operation_type_wire_name(operation_type()));
    writer.write_string(_application_name);
    writer.write_string(_key);
    switch (operation_type())
    {
    case OperationType::kUpdate:
      writer.write_string(std::get<Update>(_payload).value);
      break;
    case OperationType::kIncrementalUpdate:
      {
        const auto& p = std::get<IncrementalUpdate>(_payload);
        writer.write_bytes(p.base_message_uuid.bytes());
        writer.write_string(atomic_operation_wire_name(p.atomic_operation));
        writer.write_string(p.value);
        break;
      }
    case OperationType::kDelete:
      break;
    }
    writer.write_i64(_last_update);
    writer.write_i32(_last_update_server_id);
  }

  EntityMessage EntityMessage::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    const auto operation = operation_type_parse(reader.read_string());
    if (!operation.has_value())
    {
      throw std::out_of_range{"EntityMessage::decode: unknown operation type"};
    }
    auto application_name = reader.read_string();
    auto key = reader.read_string();

    Payload payload;
    switch (*operation)
    {
    case OperationType::kUpdate:
      payload = Update{reader.read_string()};
      break;
    case OperationType::kIncrementalUpdate:
      {
        const auto base_message_uuid = Uuid::from_bytes(reader.read_fixed<16>());
        const auto atomic_operation = atomic_operation_parse(reader.read_string());
        if (!atomic_operation.has_value())
        {
          throw std::out_of_range{"EntityMessage::decode: unknown atomic operation"};
        }
        payload = IncrementalUpdate{base_message_uuid, *atomic_operation, reader.read_string()};
        break;
      }
    case OperationType::kDelete:
      payload = Delete{};
      break;
    }

    const auto last_update = reader.read_i64();
    const auto last_update_server_id = reader.read_i32();
    return EntityMessage{
      uuid, std::move(application_name), std::move(key),
      last_update, last_update_server_id, std::move(payload)
    };
  }
}
