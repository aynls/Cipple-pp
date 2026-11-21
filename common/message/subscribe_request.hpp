#pragma once
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // 在当前 TCP 会话上订阅。游标 -1 表示实时订阅，非负值表示持久回放。
  class SubscribeRequest final : public Message
  {
  public:
    SubscribeRequest(Uuid uuid, std::string application_name, std::string key,
                     std::optional<std::int64_t> resume_after = std::nullopt);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    std::optional<std::int64_t> resume_after() const noexcept;

    static std::unique_ptr<SubscribeRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _application_name;
    std::string _key;
    std::optional<std::int64_t> _resume_after;
  };
}
