#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // Fields after the RPC UUID are bool success.
  class DeleteResponse final : public Message
  {
  public:
    DeleteResponse(Uuid uuid, bool success);

    bool success() const noexcept;

    static std::unique_ptr<DeleteResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    bool _success;
  };
}
