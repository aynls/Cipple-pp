#pragma once
#include <cstdint>
#include <memory>
#include <span>

namespace cipple::message
{
  class Message;
}

namespace cipple::tcp
{
  // Decode exactly one frame body (type + uuid + fields), without its length prefix.
  // Unknown types or trailing bytes throw std::invalid_argument; truncated fields
  // and invalid field values propagate the message decoder's exceptions.
  std::unique_ptr<message::Message> decode_message(std::span<const std::uint8_t> body);
}
