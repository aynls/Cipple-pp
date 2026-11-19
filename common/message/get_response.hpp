#pragma once
#include <memory>
#include <string>
#include <vector>

#include "entity/entity_message.hpp"
#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(9) + uuid + applicationName + key + value
  //       + int32 itemsCount + items[实体公共段]
  class GetResponse final : public Message
  {
  public:
    GetResponse(Uuid uuid, std::string application_name, std::string key, std::string value,
                std::vector<entity::EntityMessage> items);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    const std::string& value() const noexcept;
    const std::vector<entity::EntityMessage>& items() const noexcept;

    static std::unique_ptr<GetResponse> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _application_name;
    std::string _key;
    std::string _value;
    std::vector<entity::EntityMessage> _items;
  };
}
