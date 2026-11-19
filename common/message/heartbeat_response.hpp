#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(1) + uuid + bool success
  class HeartbeatResponse final : public Message
  {
  public:
    HeartbeatResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<HeartbeatResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
