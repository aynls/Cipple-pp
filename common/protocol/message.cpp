#include "message.hpp"

#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  Message::Message(const MessageType type, const Uuid uuid)
    : _type{type}
      , _uuid{uuid}
  {
  }

  MessageType Message::type() const noexcept
  {
    return _type;
  }

  const Uuid& Message::uuid() const noexcept
  {
    return _uuid;
  }

  void Message::encode(tcp::BufferWriter& writer) const
  {
    writer.write_u8(to_byte(_type));
    writer.write_bytes(_uuid.bytes());
    encode_fields(writer);
  }
}
