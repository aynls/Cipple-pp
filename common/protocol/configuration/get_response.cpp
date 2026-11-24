#include "get_response.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  GetResponse::GetResponse(const Uuid uuid, std::string application_name, std::string key,
                           std::string value, std::vector<entity::EntityMessage> items)
    : Message{MessageType::kGetResponse, uuid}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
      , _value{std::move(value)}
      , _items{std::move(items)}
  {
  }

  const std::string& GetResponse::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& GetResponse::key() const noexcept
  {
    return _key;
  }

  const std::string& GetResponse::value() const noexcept
  {
    return _value;
  }

  const std::vector<entity::EntityMessage>& GetResponse::items() const noexcept
  {
    return _items;
  }

  void GetResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    if (_items.size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()))
    {
      throw std::length_error{"GetResponse::encode_fields: too many items"};
    }
    writer.write_string(_application_name);
    writer.write_string(_key);
    writer.write_string(_value);
    writer.write_i32(static_cast<std::int32_t>(_items.size()));
    for (const auto& item : _items)
    {
      item.encode(writer);
    }
  }

  std::unique_ptr<GetResponse> GetResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid{Uuid::from_bytes(reader.read_fixed<16>())};
    auto application_name{reader.read_string()};
    auto key{reader.read_string()};
    auto value{reader.read_string()};
    const auto count{reader.read_i32()};
    if (count < 0)
    {
      throw std::out_of_range{"GetResponse::decode: negative items count"};
    }
    // The shortest entity is a deletion with empty application/key strings.
    constexpr std::size_t kMinItemSize{16 + (8 + 6) + 8 + 8 + 8 + 4};
    if (static_cast<std::size_t>(count) > reader.remaining() / kMinItemSize)
    {
      throw std::out_of_range{"GetResponse::decode: items count exceeds remaining data"};
    }
    std::vector<entity::EntityMessage> items;
    items.reserve(static_cast<std::size_t>(count));
    for (std::int32_t i{0}; i < count; ++i)
    {
      items.push_back(entity::EntityMessage::decode(reader));
    }
    return std::make_unique < GetResponse > (uuid, std::move(application_name), std::move(key),
      std::move(value), std::move(items));
  }
}
