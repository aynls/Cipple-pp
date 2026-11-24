#include "put_request.hpp"

#include <utility>

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  PutRequest::PutRequest(const Uuid uuid, std::string application_name, std::string key,
                         std::string value)
    : Message{MessageType::kPutRequest, uuid}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
      , _value{std::move(value)}
  {
  }

  const std::string& PutRequest::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& PutRequest::key() const noexcept
  {
    return _key;
  }

  const std::string& PutRequest::value() const noexcept
  {
    return _value;
  }

  void PutRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_application_name);
    writer.write_string(_key);
    writer.write_string(_value);
  }

  std::unique_ptr<PutRequest> PutRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    auto application_name{reader.read_string()};
    auto key{reader.read_string()};
    auto value{reader.read_string()};
    return std::make_unique < PutRequest > (uuid, std::move(application_name), std::move(key),
      std::move(value));
  }
}
