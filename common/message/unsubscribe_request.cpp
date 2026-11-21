#include "unsubscribe_request.hpp"

#include <utility>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  UnsubscribeRequest::UnsubscribeRequest(const Uuid uuid, std::string application_name, std::string key)
    : Message{MessageType::kUnsubscribeRequest, uuid}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
  {
  }

  const std::string& UnsubscribeRequest::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& UnsubscribeRequest::key() const noexcept
  {
    return _key;
  }

  void UnsubscribeRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_application_name);
    writer.write_string(_key);
  }

  std::unique_ptr<UnsubscribeRequest> UnsubscribeRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto application_name = reader.read_string();
    auto key = reader.read_string();
    return std::make_unique<UnsubscribeRequest>(uuid, std::move(application_name), std::move(key));
  }
}
