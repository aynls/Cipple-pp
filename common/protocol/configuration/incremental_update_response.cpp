#include "incremental_update_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  IncrementalUpdateResponse::IncrementalUpdateResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kIncrementalUpdateResponse, uuid}
      , _success{success}
  {
  }

  bool IncrementalUpdateResponse::success() const noexcept
  {
    return _success;
  }

  void IncrementalUpdateResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<IncrementalUpdateResponse> IncrementalUpdateResponse::decode(
    tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    return std::make_unique < IncrementalUpdateResponse > (uuid, reader.read_bool());
  }
}
