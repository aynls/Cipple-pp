#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(11) + uuid + bool success
  class PutResponse final : public Message
  {
  public:
    PutResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<PutResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
