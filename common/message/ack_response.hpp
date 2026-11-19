#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(3) + uuid + bool success
  class AckResponse final : public Message
  {
  public:
    AckResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<AckResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
