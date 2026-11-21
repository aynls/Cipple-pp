#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <stdexcept>
#include <string>
#include <vector>

#include "tcp/buffer_reader.hpp"
#include "tcp/buffer_writer.hpp"
#include "uuid.hpp"

using cipple::Uuid;
using cipple::tcp::BufferReader;
using cipple::tcp::BufferWriter;

TEST(BufferReaderTest, ReadsU8)
{
  const std::vector<std::uint8_t> raw{0xAB};
  BufferReader reader{raw};

  EXPECT_EQ(reader.read_u8(), 0xAB);
  EXPECT_EQ(reader.remaining(), 0);
}

TEST(BufferReaderTest, ReadsU32InBigEndian)
{
  const std::vector<std::uint8_t> raw{0x01, 0x02, 0x03, 0x04};
  BufferReader reader{raw};

  EXPECT_EQ(reader.read_u32(), 0x01020304);
}

TEST(BufferReaderTest, ReadsU64InBigEndian)
{
  const std::vector<std::uint8_t> raw{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  BufferReader reader{raw};

  EXPECT_EQ(reader.read_u64(), 0x0102030405060708);
}

TEST(BufferReaderTest, ReadsSignedIntegersBitExact)
{
  const std::vector<std::uint8_t> raw{
    0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE
  };
  BufferReader reader{raw};

  EXPECT_EQ(reader.read_i32(), -1);
  EXPECT_EQ(reader.read_i64(), -2);
}

TEST(BufferReaderTest, ReadsStringWithU64LengthPrefix)
{
  const std::vector<std::uint8_t> raw{
    0, 0, 0, 0, 0, 0, 0, 3,
    0x61, 0x62, 0x63
  };
  BufferReader reader{raw};

  EXPECT_EQ(reader.read_string(), "abc");
  EXPECT_EQ(reader.remaining(), 0);
}

TEST(BufferReaderTest, ReadBytesIsZeroCopy)
{
  const std::vector<std::uint8_t> raw{1, 2, 3, 4, 5, 6};
  BufferReader reader{raw};

  const auto view = reader.read_bytes(3);

  ASSERT_EQ(view.size(), 3);
  EXPECT_EQ(view.data(), raw.data()); // 视图指向原缓冲，无拷贝
  EXPECT_EQ(reader.read_u8(), 4);
}

TEST(BufferReaderTest, TracksPositionAndRemaining)
{
  BufferWriter writer;
  writer.write_u32(1);
  writer.write_string("xy");
  BufferReader reader{writer.bytes()};

  EXPECT_EQ(reader.position(), 0);
  EXPECT_EQ(reader.remaining(), 14);
  EXPECT_EQ(reader.read_u32(), 1);
  EXPECT_EQ(reader.position(), 4);
  EXPECT_EQ(reader.read_string(), "xy");
  EXPECT_EQ(reader.remaining(), 0);
}

TEST(BufferReaderTest, RoundTripsWithBufferWriter)
{
  BufferWriter writer;
  writer.write_u8(0x7F);
  writer.write_u32(0x01020304);
  writer.write_u64(0x0102030405060708);
  writer.write_i32(-123);
  writer.write_i64(-456);
  writer.write_string("hello");
  const std::array<std::uint8_t, 4> tail{0xDE, 0xAD, 0xBE, 0xEF};
  writer.write_bytes(tail);

  BufferReader reader{writer.bytes()};
  EXPECT_EQ(reader.read_u8(), 0x7F);
  EXPECT_EQ(reader.read_u32(), 0x01020304);
  EXPECT_EQ(reader.read_u64(), 0x0102030405060708);
  EXPECT_EQ(reader.read_i32(), -123);
  EXPECT_EQ(reader.read_i64(), -456);
  EXPECT_EQ(reader.read_string(), "hello");
  EXPECT_TRUE(std::ranges::equal(reader.read_bytes(4), tail));
  EXPECT_EQ(reader.remaining(), 0);
}

TEST(BufferReaderTest, UuidRoundTripsThroughWireFormat)
{
  const Uuid original = Uuid::generate();
  BufferWriter writer;
  writer.write_bytes(original.bytes());

  BufferReader reader{writer.bytes()};
  const Uuid decoded = Uuid::from_bytes(reader.read_fixed<16>());

  EXPECT_EQ(decoded, original);
  EXPECT_EQ(reader.remaining(), 0);
}

TEST(BufferReaderTest, ThrowsOnEmptyInput)
{
  BufferReader reader{std::span<const std::uint8_t>{}};

  EXPECT_THROW(reader.read_u8(), std::out_of_range);
  EXPECT_THROW(reader.read_u32(), std::out_of_range);
  EXPECT_THROW(reader.read_u64(), std::out_of_range);
  EXPECT_THROW(reader.read_i32(), std::out_of_range);
  EXPECT_THROW(reader.read_i64(), std::out_of_range);
  EXPECT_THROW(reader.read_string(), std::out_of_range);
  EXPECT_THROW(reader.read_bytes(1), std::out_of_range);
  EXPECT_THROW(reader.read_fixed<16>(), std::out_of_range);
}

TEST(BufferReaderTest, ExactFitSucceedsThenThrows)
{
  const std::vector<std::uint8_t> raw{0x01, 0x02, 0x03, 0x04};
  BufferReader reader{raw};

  EXPECT_EQ(reader.read_u32(), 0x01020304);
  EXPECT_THROW(reader.read_u8(), std::out_of_range);
}

TEST(BufferReaderTest, ThrowsOnTruncatedInput)
{
  const std::vector<std::uint8_t> raw{0x01, 0x02, 0x03};
  BufferReader reader{raw};

  EXPECT_THROW(reader.read_u32(), std::out_of_range);
}

TEST(BufferReaderTest, ThrowsOnDeclaredStringLengthBeyondData)
{
  BufferWriter writer;
  writer.write_u64(100);
  writer.write_u8('x');
  BufferReader reader{writer.bytes()};

  EXPECT_THROW(reader.read_string(), std::out_of_range);
}
