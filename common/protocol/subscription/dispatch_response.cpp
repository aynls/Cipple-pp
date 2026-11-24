#include "dispatch_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  DispatchResponse::DispatchResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kDispatchResponse, uuid}
      , _success{success}
  {
  }

  bool DispatchResponse::success() const noexcept
  {
    return _success;
  }

  void DispatchResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<DispatchResponse> DispatchResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < DispatchResponse > (uuid, reader.read_bool());
  }
}
