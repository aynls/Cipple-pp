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
  class WrongShardResponse final : public Message
  {
  public:
    WrongShardResponse(Uuid request_id, std::uint32_t shard_id);

    std::uint32_t shard_id() const noexcept;

    static std::unique_ptr<WrongShardResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::uint32_t _shard_id;
  };
}
