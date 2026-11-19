#include "system_info_response.hpp"

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  SystemInfoResponse::SystemInfoResponse(const Uuid uuid, const double cpu_usage)
    : Message{MessageType::kSystemInfoResponse, uuid}
      , _cpu_usage{cpu_usage}
  {
  }

  double SystemInfoResponse::cpu_usage() const noexcept
  {
    return _cpu_usage;
  }

  void SystemInfoResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_f64(_cpu_usage);
  }

  std::unique_ptr<SystemInfoResponse> SystemInfoResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    return std::make_unique < SystemInfoResponse > (uuid, reader.read_f64());
  }
}
