#include "ack_request.hpp"

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  AckRequest::AckRequest(const Uuid uuid, const Uuid message_uuid,
                         const std::int32_t source_id, const std::int32_t node_id)
    : Message{MessageType::kAckRequest, uuid}
      , _message_uuid{message_uuid}
      , _source_id{source_id}
      , _node_id{node_id}
  {
  }

  const Uuid& AckRequest::message_uuid() const noexcept
  {
    return _message_uuid;
  }

  std::int32_t AckRequest::source_id() const noexcept
  {
    return _source_id;
  }

  std::int32_t AckRequest::node_id() const noexcept
  {
    return _node_id;
  }

  void AckRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bytes(_message_uuid.bytes());
    writer.write_i32(_source_id);
    writer.write_i32(_node_id);
  }

  std::unique_ptr<AckRequest> AckRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    const auto message_uuid = Uuid::from_bytes(reader.read_fixed<16>());
    const auto source_id = reader.read_i32();
    const auto node_id = reader.read_i32();
    return std::make_unique<AckRequest>(uuid, message_uuid, source_id, node_id);
  }
}
