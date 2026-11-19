#pragma once
#include <cstdint>
#include <memory>
#include <string>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(12) + uuid + applicationName + key + callbackAddress + int32 callbackPort
  class SubscribeRequest final : public Message
  {
  public:
    SubscribeRequest(Uuid uuid, std::string application_name, std::string key,
                     std::string callback_address, std::int32_t callback_port);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    const std::string& callback_address() const noexcept;
    std::int32_t callback_port() const noexcept;

    static std::unique_ptr<SubscribeRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _application_name;
    std::string _key;
    std::string _callback_address;
    std::int32_t _callback_port;
  };
}
