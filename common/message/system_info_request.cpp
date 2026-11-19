#include "system_info_request.hpp"

#include "tcp/buffer_reader.hpp"

namespace cipple::message
{
  SystemInfoRequest::SystemInfoRequest(const Uuid uuid)
    : Message{MessageType::kSystemInfoRequest, uuid}
  {
  }

  void SystemInfoRequest::encode_fields(tcp::BufferWriter& /*writer*/) const
  {
  }

  std::unique_ptr<SystemInfoRequest> SystemInfoRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    return std::make_unique < SystemInfoRequest > (uuid);
  }
}
