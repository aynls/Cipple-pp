#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(22) + uuid
  class SystemInfoRequest final : public Message
  {
  public:
    explicit SystemInfoRequest(Uuid uuid);

    static std::unique_ptr<SystemInfoRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;
  };
}
