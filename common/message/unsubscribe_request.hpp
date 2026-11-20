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
  // type(14) + uuid + applicationName + key + callbackAddress + int32 callbackPort
  // callbackAddress 为空、callbackPort 为 0 时，取消当前 TCP 会话的订阅。
  class UnsubscribeRequest final : public Message
  {
  public:
    UnsubscribeRequest(Uuid uuid, std::string application_name, std::string key,
                       std::string callback_address, std::int32_t callback_port);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    const std::string& callback_address() const noexcept;
    std::int32_t callback_port() const noexcept;

    static std::unique_ptr<UnsubscribeRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _application_name;
    std::string _key;
    std::string _callback_address;
    std::int32_t _callback_port;
  };
}
