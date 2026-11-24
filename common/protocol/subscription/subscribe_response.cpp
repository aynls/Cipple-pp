#include "subscribe_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  SubscribeResponse::SubscribeResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kSubscribeResponse, uuid}
      , _success{success}
  {
  }

  bool SubscribeResponse::success() const noexcept
  {
    return _success;
  }

  void SubscribeResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<SubscribeResponse> SubscribeResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < SubscribeResponse > (uuid, reader.read_bool());
  }
}
