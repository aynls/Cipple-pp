#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(19) + uuid + bool success
  class DispatchResponse final : public Message
  {
  public:
    DispatchResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<DispatchResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
