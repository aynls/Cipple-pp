#include "heartbeat_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  HeartbeatResponse::HeartbeatResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kHeartbeatResponse, uuid}
      , _success{success}
  {
  }

  bool HeartbeatResponse::success() const noexcept
  {
    return _success;
  }

  void HeartbeatResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<HeartbeatResponse> HeartbeatResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique<HeartbeatResponse>(uuid, reader.read_bool());
  }
}
