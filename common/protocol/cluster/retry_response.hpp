#pragma once
#include <memory>

#include "protocol/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // The endpoint cannot currently confirm the request. Retrying reuses the request UUID.
  class RetryResponse final : public Message
  {
  public:
    explicit RetryResponse(Uuid uuid);

    static std::unique_ptr<RetryResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;
  };
}
