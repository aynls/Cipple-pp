#include "wrong_shard_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  WrongShardResponse::WrongShardResponse(const Uuid request_id, const std::uint32_t shard_id)
    : Message{MessageType::kWrongShardResponse, request_id}
      , _shard_id{shard_id}
  {
  }

  std::uint32_t WrongShardResponse::shard_id() const noexcept
  {
    return _shard_id;
  }

  void WrongShardResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_u32(_shard_id);
  }

  std::unique_ptr<WrongShardResponse> WrongShardResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < WrongShardResponse > (uuid, reader.read_u32());
  }
}
