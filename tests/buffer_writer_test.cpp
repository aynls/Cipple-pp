#include <gtest/gtest.h>

#include <array>
#include <stdexcept>
#include <string>
#include <vector>

#include "tcp/buffer_writer.hpp"

using cipple::tcp::BufferWriter;

TEST(BufferWriterTest, WritesU8)
{
  BufferWriter writer;

  writer.write_u8(0xAB);

  EXPECT_EQ(writer.bytes(), (std::vector<std::uint8_t>{0xAB}));
}

TEST(BufferWriterTest, WritesU32InBigEndian)
{
  BufferWriter writer;

  writer.write_u32(0x01020304);

  EXPECT_EQ(writer.bytes(), (std::vector<std::uint8_t>{0x01, 0x02, 0x03, 0x04}));
}

TEST(BufferWriterTest, WritesU64InBigEndian)
{
  BufferWriter writer;

  writer.write_u64(0x0102030405060708);

  EXPECT_EQ(writer.bytes(), (std::vector<std::uint8_t>{
              0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}));
}

TEST(BufferWriterTest, WritesSignedIntegersBitExact)
{
  BufferWriter writer;

  writer.write_i32(-1);
  writer.write_i64(-2);

  EXPECT_EQ(writer.bytes(), (std::vector<std::uint8_t>{
              0xFF, 0xFF, 0xFF, 0xFF,
              0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}));
}

TEST(BufferWriterTest, WritesStringWithU64LengthPrefix)
{
  BufferWriter writer;

  writer.write_string("abc");

  EXPECT_EQ(writer.bytes(), (std::vector<std::uint8_t>{
              0, 0, 0, 0, 0, 0, 0, 3,
              0x61, 0x62, 0x63}));
}

TEST(BufferWriterTest, WritesRawBytesWithoutLengthPrefix)
{
  BufferWriter writer;
  const std::array<std::uint8_t, 16> uuid{
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
    0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F
  };

  writer.write_bytes(uuid);

  EXPECT_EQ(writer.bytes(), (std::vector<std::uint8_t>{uuid.begin(), uuid.end()}));
}

TEST(BufferWriterTest, GrowsBeyondOneKilobyte)
{
  BufferWriter writer;
  const std::string payload(4096, 'x');

  writer.write_string(payload);

  EXPECT_EQ(writer.size(), 8 + 4096);
}

TEST(BufferWriterTest, ClearResetsBuffer)
{
  BufferWriter writer;

  writer.write_u32(1);
  writer.clear();

  EXPECT_TRUE(writer.bytes().empty());
}

TEST(BufferWriterTest, PatchU32BackpatchesFrameLength)
{
  BufferWriter writer;

  writer.write_u32(0); // length 占位
  writer.write_u8(0x0A); // type
  writer.write_string("k"); // payload
  writer.patch_u32(0, static_cast<std::uint32_t>(writer.size() - 4));

  const std::vector<std::uint8_t> expected{
    0x00, 0x00, 0x00, 0x0A, // length = type(1) + payload(9) = 10
    0x0A,
    0, 0, 0, 0, 0, 0, 0, 1, 0x6B,
  };
  EXPECT_EQ(writer.bytes(), expected);
}

TEST(BufferWriterTest, PatchU32OutOfRangeThrows)
{
  BufferWriter writer;
  writer.write_u8(0);

  EXPECT_THROW(writer.patch_u32(0, 0), std::out_of_range);
}
