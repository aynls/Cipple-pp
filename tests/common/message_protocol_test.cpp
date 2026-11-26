#include <gtest/gtest.h>

#include <stdexcept>

#include "entity/entity_message.hpp"
#include "protocol/message.hpp"
#include "protocol/message_type.hpp"
#include "binary/buffer_writer.hpp"
#include "protocol/message_decoder.hpp"
#include "protocol/subscription/relay_messages.hpp"

namespace
{
  using cipple::Uuid;
  using cipple::entity::AtomicOperation;
  using cipple::entity::EntityMessage;
  using cipple::message::DispatchRequest;
  using cipple::message::MessageType;
  using cipple::message::RelayDispatch;
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
      auto body{message_body(type)};
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
    auto body{message_body(MessageType::kDispatchRequest)};
    update.encode(body);
    EXPECT_THROW(decode_message(body.bytes()), std::out_of_range);
  }

  TEST(MessageProtocolTest, RejectsIncrementalSyncWithoutFullBase)
  {
    auto body{message_body(MessageType::kSyncRequest)};
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

  TEST(MessageProtocolTest, RelayDispatchPreservesAttemptEventAndIncrementalBase)
  {
    const auto attempt{Uuid::generate()};
    const EntityMessage base{Uuid::generate(), "app", "key", 10, 0, EntityMessage::Update{"initial"}};
    const EntityMessage delta{
      Uuid::generate(), "app", "key", 12, 0,
      EntityMessage::IncrementalUpdate{base.uuid(), AtomicOperation::kAddEntry, "x"}
    };
    const std::vector targets{Uuid::generate(), Uuid::generate()};
    const RelayDispatch relay{DispatchRequest{attempt, delta, 12, base}, true, targets};
    BufferWriter body;
    relay.encode(body);

    const auto message{decode_message(body.bytes())};
    const auto* decoded{dynamic_cast<const RelayDispatch*>(message.get())};
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->uuid(), attempt);
    EXPECT_EQ(decoded->dispatch().uuid(), attempt);
    EXPECT_EQ(decoded->dispatch().entity(), delta);
    EXPECT_EQ(decoded->dispatch().sequence(), 12);
    EXPECT_EQ(decoded->dispatch().base(), base);
    EXPECT_EQ(decoded->targets(), targets);
    EXPECT_TRUE(decoded->deliver_local());
  }

  TEST(MessageProtocolTest, RejectsInvalidRelaySlotsAndDuplicateTargets)
  {
    auto advertisement{message_body(MessageType::kRelayAdvertise)};
    advertisement.write_u32(9000);
    advertisement.write_u32(0);
    EXPECT_THROW(decode_message(advertisement.bytes()), std::invalid_argument);

    auto relay{message_body(MessageType::kRelayDispatch)};
    const auto target{Uuid::generate()};
    const EntityMessage update{Uuid::generate(), "app", "key", 10, 0, EntityMessage::Update{"value"}};
    update.encode(relay);
    relay.write_i64(10);
    relay.write_bool(false);
    relay.write_u32(2);
    relay.write_bytes(target.bytes());
    relay.write_bytes(target.bytes());
    EXPECT_THROW(decode_message(relay.bytes()), std::invalid_argument);
  }
}
