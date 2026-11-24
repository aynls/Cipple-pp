#pragma once
#include <memory>

#include "protocol/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // This message carries only the RPC UUID.
  class HeartbeatRequest final : public Message
  {
  public:
    explicit HeartbeatRequest(Uuid uuid);

    // 解码 type 字节之后的 payload：uuid + 自有字段
    static std::unique_ptr<HeartbeatRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;
  };
}
