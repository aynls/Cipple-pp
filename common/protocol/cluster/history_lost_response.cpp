#include "history_lost_response.hpp"

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  HistoryLostResponse::HistoryLostResponse(const Uuid uuid, const std::int64_t minimum_cursor)
    : Message{MessageType::kHistoryLostResponse, uuid}
      , _minimum_cursor{minimum_cursor}
  {
  }

  std::int64_t HistoryLostResponse::minimum_cursor() const noexcept
  {
    return _minimum_cursor;
  }

  void HistoryLostResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_i64(_minimum_cursor);
  }

  std::unique_ptr<HistoryLostResponse> HistoryLostResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    return std::make_unique < HistoryLostResponse > (uuid, reader.read_i64());
  }
}
