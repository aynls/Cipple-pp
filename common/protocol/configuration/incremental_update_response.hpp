#pragma once
#include <memory>

#include "protocol/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // Fields after the RPC UUID are bool success.
  class IncrementalUpdateResponse final : public Message
  {
  public:
    IncrementalUpdateResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<IncrementalUpdateResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
