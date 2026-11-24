#include "put_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  PutResponse::PutResponse(const Uuid uuid, const bool success)
    : Message{MessageType::kPutResponse, uuid}
      , _success{success}
  {
  }

  bool PutResponse::success() const noexcept
  {
    return _success;
  }

  void PutResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_success);
  }

  std::unique_ptr<PutResponse> PutResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < PutResponse > (uuid, reader.read_bool());
  }
}
