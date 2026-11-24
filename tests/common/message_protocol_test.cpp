#include <gtest/gtest.h>

#include <stdexcept>

#include "entity/entity_message.hpp"
#include "protocol/message.hpp"
#include "protocol/message_type.hpp"
#include "binary/buffer_writer.hpp"
#include "protocol/message_decoder.hpp"

namespace
{
  using cipple::Uuid;
  using cipple::entity::AtomicOperation;
  using cipple::entity::EntityMessage;
  using cipple::message::MessageType;
  using cipple::message::to_byte;
  using cipple::tcp::BufferWriter;
  using cipple::tcp::decode_message;

  BufferWriter message_body(const MessageType type)
  {
    BufferWriter body;
    body.write_u8(to_byte(type));
    body.write_bytes(Uuid::generate().bytes());
    return body;
  }

  TEST(MessageProtocolTest, RejectsTrailingSubscriptionFields)
  {
    for (const auto type : {MessageType::kSubscribeRequest, MessageType::kUnsubscribeRequest})
    {
      SCOPED_TRACE(to_byte(type));
      auto body = message_body(type);
      body.write_string("app");
      body.write_string("key");
      if (type == MessageType::kSubscribeRequest)
      {
        body.write_i64(0);
      }
      body.write_u8(0);
      EXPECT_THROW(decode_message(body.bytes()), std::invalid_argument);
    }
  }

  TEST(MessageProtocolTest, RejectsDispatchWithoutSequence)
  {
    const EntityMessage update{Uuid::generate(), "app", "key", 1, 1, EntityMessage::Update{"value"}};
    auto body = message_body(MessageType::kDispatchRequest);
    update.encode(body);
    EXPECT_THROW(decode_message(body.bytes()), std::out_of_range);
  }

  TEST(MessageProtocolTest, RejectsIncrementalSyncWithoutFullBase)
  {
    auto body = message_body(MessageType::kSyncRequest);
    const EntityMessage delta{
      Uuid::generate(), "app", "key", 2, 1,
      EntityMessage::IncrementalUpdate{Uuid::generate(), AtomicOperation::kAddEntry, "x"}
    };
    delta.encode(body);
    body.write_i32(1);
    EXPECT_THROW(decode_message(body.bytes()), std::out_of_range);
  }

  TEST(MessageProtocolTest, RejectsUnknownMessageType)
  {
    BufferWriter body;
    body.write_u8(255);
    EXPECT_THROW(decode_message(body.bytes()), std::invalid_argument);
  }
}
