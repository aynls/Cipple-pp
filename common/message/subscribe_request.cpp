#include "subscribe_request.hpp"

#include <stdexcept>
#include <utility>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  SubscribeRequest::SubscribeRequest(const Uuid uuid, std::string application_name, std::string key,
                                     std::string callback_address, const std::int32_t callback_port,
                                     const std::optional<std::int64_t> resume_after)
    : Message{MessageType::kSubscribeRequest, uuid}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
      , _callback_address{std::move(callback_address)}
      , _callback_port{callback_port}
      , _resume_after{resume_after}
  {
    if (_resume_after && *_resume_after < 0)
    {
      throw std::invalid_argument{"negative subscription cursor"};
    }
  }

  const std::string& SubscribeRequest::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& SubscribeRequest::key() const noexcept
  {
    return _key;
  }

  const std::string& SubscribeRequest::callback_address() const noexcept
  {
    return _callback_address;
  }

  std::int32_t SubscribeRequest::callback_port() const noexcept
  {
    return _callback_port;
  }

  std::optional<std::int64_t> SubscribeRequest::resume_after() const noexcept
  {
    return _resume_after;
  }

  void SubscribeRequest::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_application_name);
    writer.write_string(_key);
    writer.write_string(_callback_address);
    writer.write_i32(_callback_port);
    if (_resume_after)
    {
      writer.write_i64(*_resume_after);
    }
  }

  std::unique_ptr<SubscribeRequest> SubscribeRequest::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto application_name = reader.read_string();
    auto key = reader.read_string();
    auto callback_address = reader.read_string();
    const auto callback_port = reader.read_i32();
    std::optional<std::int64_t> resume_after;
    if (reader.remaining() != 0)
    {
      resume_after = reader.read_i64();
    }
    return std::make_unique < SubscribeRequest > (uuid, std::move(application_name), std::move(key),
      std::move(callback_address), callback_port, resume_after);
  }
}
