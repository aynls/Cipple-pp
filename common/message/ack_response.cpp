#include "ack_response.hpp"

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  AckResponse::AckResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kAckResponse, uuid}
      , _success{success}
  {
  }

  bool AckResponse::success() const noexcept
  {
    return _success;
  }

  void AckResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<AckResponse> AckResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    return std::make_unique < AckResponse > (uuid, reader.read_bool());
  }
}
