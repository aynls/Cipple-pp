#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(15) + uuid + bool success
  class UnsubscribeResponse final : public Message
  {
  public:
    UnsubscribeResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<UnsubscribeResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
