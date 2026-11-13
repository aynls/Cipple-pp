#pragma once
#include "message/message_type.hpp"
#include "uuid.hpp"

namespace cipple::tcp
{
  class BufferWriter;
}

namespace cipple::message
{
  // 线上消息基类：type + uuid（传输层身份，兼作请求/响应关联 id）。
  // 字段仅经构造写入，无 setter，构造后不可变；一律以 std::unique_ptr 持有。
  class Message
  {
  public:
    virtual ~Message() = default;

    MessageType type() const noexcept;
    const Uuid& uuid() const noexcept;

    // 序列化 type 字节 + payload 到 writer；帧长前缀由帧层负责
    void encode(tcp::BufferWriter& writer) const;

  protected:
    Message(MessageType type, Uuid uuid);

    // 各派生类按协议顺序写自己的 payload 字段
    virtual void encode_fields(tcp::BufferWriter& writer) const = 0;

  private:
    MessageType _type;
    Uuid _uuid;
  };
}
