#include "incremental_update_request.hpp"

#include <stdexcept>

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  IncrementalUpdateRequest::IncrementalUpdateRequest(const Uuid uuid, std::string application_name,
                                                     std::string key, const Uuid base_message_uuid,
                                                     const entity::AtomicOperation atomic_operation,
                                                     std::string value)
    : Message{MessageType::kIncrementalUpdateRequest, uuid}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
      , _base_message_uuid{base_message_uuid}
      , _atomic_operation{atomic_operation}
      , _value{std::move(value)}
  {
  }

  const std::string& IncrementalUpdateRequest::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& IncrementalUpdateRequest::key() const noexcept
  {
    return _key;
  }

  const Uuid& IncrementalUpdateRequest::base_message_uuid() const noexcept
  {
    return _base_message_uuid;
  }

  entity::AtomicOperation IncrementalUpdateRequest::atomic_operation() const noexcept
  {
    return _atomic_operation;
  }

  const std::string& IncrementalUpdateRequest::value() const noexcept
  {
    return _value;
  }

  void IncrementalUpdateRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_application_name);
    writer.write_string(_key);
    writer.write_bytes(_base_message_uuid.bytes());
    writer.write_string(entity::atomic_operation_wire_name(_atomic_operation));
    writer.write_string(_value);
  }

  std::unique_ptr<IncrementalUpdateRequest> IncrementalUpdateRequest::decode(
    tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto application_name = reader.read_string();
    auto key = reader.read_string();
    const auto base_message_uuid = Uuid::from_bytes(reader.read_fixed<16>());
    const auto atomic_operation = entity::atomic_operation_parse(reader.read_string());
    if (!atomic_operation.has_value())
    {
      throw std::out_of_range{"IncrementalUpdateRequest::decode: unknown atomic operation"};
    }
    return std::make_unique < IncrementalUpdateRequest > (uuid, std::move(application_name),
      std::move(key), base_message_uuid,
      *atomic_operation, reader.read_string());
  }
}
