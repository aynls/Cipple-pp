#include "sync_response.hpp"

#include <stdexcept>

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  SyncResponse::SyncResponse(const Uuid uuid, const Result result)
    : Message{MessageType::kSyncResponse, uuid}
      , _result{result}
  {
  }

  Result SyncResponse::result() const noexcept
  {
    return _result;
  }

  void SyncResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_u8(static_cast<std::uint8_t>(_result));
  }

  std::unique_ptr<SyncResponse> SyncResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    const auto result = reader.read_u8();
    if (result > static_cast<std::uint8_t>(Result::kRejected))
    {
      throw std::out_of_range{"SyncResponse::decode: unknown result"};
    }
    return std::make_unique < SyncResponse > (uuid, static_cast<Result>(result));
  }
}
