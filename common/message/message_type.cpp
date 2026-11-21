#include "message_type.hpp"

namespace cipple::message
{
  std::uint8_t to_byte(const MessageType type) noexcept
  {
    return static_cast<std::uint8_t>(type);
  }
}
