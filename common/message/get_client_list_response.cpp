#include "get_client_list_response.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"

namespace cipple::message
{
  GetClientListResponse::GetClientListResponse(const Uuid uuid, std::string client_list_signature,
                                               std::vector<Item> items)
    : Message{MessageType::kGetClientListResponse, uuid}
      , _client_list_signature{std::move(client_list_signature)}
      , _items{std::move(items)}
  {
  }

  const std::string& GetClientListResponse::client_list_signature() const noexcept
  {
    return _client_list_signature;
  }

  const std::vector<GetClientListResponse::Item>& GetClientListResponse::items() const noexcept
  {
    return _items;
  }

  void GetClientListResponse::encode_fields(tcp::BufferWriter& writer) const
  {
    if (_items.size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()))
    {
      throw std::length_error{"GetClientListResponse::encode_fields: too many items"};
    }
    writer.write_string(_client_list_signature);
    writer.write_i32(static_cast<std::int32_t>(_items.size()));
    for (const auto& item : _items)
    {
      writer.write_string(item.address);
      writer.write_i32(item.port);
    }
  }

  std::unique_ptr<GetClientListResponse> GetClientListResponse::decode(tcp::BufferReader& reader)
  {
    const auto uuid = Uuid::from_bytes(reader.read_fixed<16>());
    auto client_list_signature = reader.read_string();
    const auto count = reader.read_i32();
    if (count < 0)
    {
      throw std::out_of_range{"GetClientListResponse::decode: negative items count"};
    }
    constexpr std::size_t kMinItemSize = 8 + 4; // Empty address string + int32 port.
    if (static_cast<std::size_t>(count) > reader.remaining() / kMinItemSize)
    {
      throw std::out_of_range{"GetClientListResponse::decode: items count exceeds remaining data"};
    }
    std::vector<Item> items;
    items.reserve(static_cast<std::size_t>(count));
    for (std::int32_t i = 0; i < count; ++i)
    {
      items.push_back(Item{reader.read_string(), reader.read_i32()});
    }
    return std::make_unique < GetClientListResponse > (uuid, std::move(client_list_signature),
      std::move(items));
  }
}
