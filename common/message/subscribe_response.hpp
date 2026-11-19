#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(13) + uuid + bool success
  class SubscribeResponse final : public Message
  {
  public:
    SubscribeResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<SubscribeResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
