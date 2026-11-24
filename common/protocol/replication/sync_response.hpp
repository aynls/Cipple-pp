#pragma once
#include <memory>

#include "protocol/message.hpp"
#include "protocol/result.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // Fields after the RPC UUID are byte result.
  class SyncResponse final : public Message
  {
  public:
    SyncResponse(Uuid uuid, Result result);

    Result result() const noexcept;

    static std::unique_ptr<SyncResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    Result _result;
  };
}
