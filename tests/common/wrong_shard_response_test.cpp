#include <gtest/gtest.h>

#include <cstdint>

#include "binary/buffer_writer.hpp"
#include "protocol/cluster/wrong_shard_response.hpp"
#include "protocol/message_decoder.hpp"
#include "protocol/message_type.hpp"
#include "uuid.hpp"

namespace
{
  using cipple::Uuid;
  using cipple::message::MessageType;
  using cipple::message::WrongShardResponse;
  using cipple::tcp::BufferWriter;
  using cipple::tcp::decode_message;

  TEST(WrongShardResponseTest, RoundTripsThroughMessageDecoder)
  {
    const auto parsed_request_id{Uuid::parse("00112233-4455-6677-8899-aabbccddeeff")};
    ASSERT_TRUE(parsed_request_id.has_value());
    const WrongShardResponse response{*parsed_request_id, 0x1234ABCDU};

    BufferWriter writer;
    response.encode(writer);

    ASSERT_EQ(writer.bytes().size(), 21);
    EXPECT_EQ(writer.bytes()[0], 20);
    EXPECT_EQ(writer.bytes()[17], 0x12);
    EXPECT_EQ(writer.bytes()[18], 0x34);
    EXPECT_EQ(writer.bytes()[19], 0xAB);
    EXPECT_EQ(writer.bytes()[20], 0xCD);

    const auto decoded{decode_message(writer.bytes())};
    ASSERT_EQ(decoded->type(), MessageType::kWrongShardResponse);
    const auto* wrong_shard{dynamic_cast<const WrongShardResponse*>(decoded.get())};
    ASSERT_NE(wrong_shard, nullptr);
    EXPECT_EQ(wrong_shard->uuid(), *parsed_request_id);
    EXPECT_EQ(wrong_shard->shard_id(), 0x1234ABCDU);
  }
}
