#include "delete_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  DeleteResponse::DeleteResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kDeleteResponse, uuid}
      , _success{success}
  {
  }

  bool DeleteResponse::success() const noexcept
  {
    return _success;
  }

  void DeleteResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<DeleteResponse> DeleteResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < DeleteResponse > (uuid, reader.read_bool());
  }
}
