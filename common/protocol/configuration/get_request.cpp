#include "get_request.hpp"

#include <utility>

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  GetRequest::GetRequest(const Uuid uuid, std::string application_name, std::string key)
    : Message{MessageType::kGetRequest, uuid}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
  {
  }

  const std::string& GetRequest::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& GetRequest::key() const noexcept
  {
    return _key;
  }

  void GetRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_application_name);
    writer.write_string(_key);
  }

  std::unique_ptr<GetRequest> GetRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    auto application_name{reader.read_string()};
    auto key{reader.read_string()};
    return std::make_unique < GetRequest > (uuid, std::move(application_name), std::move(key));
  }
}
