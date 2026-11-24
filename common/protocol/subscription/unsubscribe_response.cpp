#include "unsubscribe_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  UnsubscribeResponse::UnsubscribeResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kUnsubscribeResponse, uuid}
      , _success{success}
  {
  }

  bool UnsubscribeResponse::success() const noexcept
  {
    return _success;
  }

  void UnsubscribeResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<UnsubscribeResponse> UnsubscribeResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < UnsubscribeResponse > (uuid, reader.read_bool());
  }
}
