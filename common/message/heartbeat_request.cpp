#include "heartbeat_request.hpp"

#include "tcp/buffer_reader.hpp"

namespace cipple::message
{
  HeartbeatRequest::HeartbeatRequest(const Uuid uuid)
    : Message{MessageType::kHeartbeatRequest, uuid}
  {
  }

  void HeartbeatRequest::encode_fields(tcp::BufferWriter& /*writer*/) const
  {
  }

  std::unique_ptr<HeartbeatRequest> HeartbeatRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    return std::make_unique < HeartbeatRequest > (uuid);
  }
}
