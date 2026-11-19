#pragma once
#include <memory>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(23) + uuid + f64 cpuUsage
  class SystemInfoResponse final : public Message
  {
  public:
    SystemInfoResponse(Uuid uuid, double cpu_usage);

    double cpu_usage() const noexcept;

    static std::unique_ptr<SystemInfoResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    double _cpu_usage;
  };
}
