#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(21) + uuid + clientListSignature + int32 itemsCount + items[address + int32 port]
  class GetClientListResponse final : public Message
  {
  public:
    struct Item
    {
      std::string address;
      std::int32_t port;
    };

    GetClientListResponse(Uuid uuid, std::string client_list_signature, std::vector<Item> items);

    const std::string& client_list_signature() const noexcept;
    const std::vector<Item>& items() const noexcept;

    static std::unique_ptr<GetClientListResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _client_list_signature;
    std::vector<Item> _items;
  };
}
