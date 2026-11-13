#include "message_type.hpp"

namespace cipple::message
{
  std::uint8_t to_byte(const MessageType type) noexcept
  {
    return static_cast<std::uint8_t>(type);
  }

  std::optional<MessageType> message_type_from_byte(const std::uint8_t value) noexcept
  {
    // 类型编号在协议中是连续的 0..23
    if (value <= to_byte(MessageType::kSystemInfoResponse))
    {
      return static_cast<MessageType>(value);
    }
    return std::nullopt;
  }
}
