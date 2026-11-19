#include "dispatch_request.hpp"

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  DispatchRequest::DispatchRequest(const Uuid uuid, std::string client_list_signature,
                                   entity::EntityMessage entity)
    : Message{MessageType::kDispatchRequest, uuid}
      , _client_list_signature{std::move(client_list_signature)}
      , _entity{std::move(entity)}
  {
  }

  const std::string& DispatchRequest::client_list_signature() const noexcept
  {
    return _client_list_signature;
  }

  const entity::EntityMessage& DispatchRequest::entity() const noexcept
  {
    return _entity;
  }

  void DispatchRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_client_list_signature);
    _entity.encode(writer);
  }

  std::unique_ptr<DispatchRequest> DispatchRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto client_list_signature = reader.read_string();
    auto entity = entity::EntityMessage::decode(reader);
    return std::make_unique < DispatchRequest > (uuid, std::move(client_list_signature),
      std::move(entity));
  }
}
