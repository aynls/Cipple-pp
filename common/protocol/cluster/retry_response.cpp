#include "retry_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  RetryResponse::RetryResponse(const Uuid uuid)
    : Message{MessageType::kRetryResponse, uuid}
  {
  }

  void RetryResponse::encode_fields(tcp::BufferWriter&) const
  {
  }

  std::unique_ptr<RetryResponse> RetryResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < RetryResponse > (uuid);
  }
}
