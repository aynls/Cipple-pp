#pragma once
#include <cstdint>
#include <memory>

#include "protocol/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // Replies to a subscription cursor older than the retained cluster history.
  class HistoryLostResponse final : public Message
  {
  public:
    HistoryLostResponse(Uuid uuid, std::int64_t minimum_cursor);

    std::int64_t minimum_cursor() const noexcept;

    static std::unique_ptr<HistoryLostResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::int64_t _minimum_cursor;
  };
}
