#include "get_client_list_request.hpp"

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  GetClientListRequest::GetClientListRequest(const Uuid uuid, std::string client_list_signature)
    : Message{MessageType::kGetClientListRequest, uuid}
      , _client_list_signature{std::move(client_list_signature)}
  {
  }

  const std::string& GetClientListRequest::client_list_signature() const noexcept
  {
    return _client_list_signature;
  }

  void GetClientListRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_client_list_signature);
  }

  std::unique_ptr<GetClientListRequest> GetClientListRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    return std::make_unique < GetClientListRequest > (uuid, reader.read_string());
  }
}
