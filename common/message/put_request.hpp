#pragma once
#include <memory>
#include <string>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(10) + uuid + applicationName + key + value
  class PutRequest final : public Message
  {
  public:
    PutRequest(Uuid uuid, std::string application_name, std::string key, std::string value);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    const std::string& value() const noexcept;

    static std::unique_ptr<PutRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _application_name;
    std::string _key;
    std::string _value;
  };
}
