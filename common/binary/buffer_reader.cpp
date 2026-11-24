#include "buffer_reader.hpp"

#include <bit>
#include <stdexcept>

namespace cipple::tcp
{
  BufferReader::BufferReader(const std::span<const std::uint8_t> data) noexcept
    : _data{data}
  {
  }

  std::uint8_t BufferReader::read_u8()
  {
    require(1);
    return _data[_pos++];
  }

  std::uint32_t BufferReader::read_u32()
  {
    require(4);
    const auto value{
      (static_cast<std::uint32_t>(_data[_pos]) << 24)
      | (static_cast<std::uint32_t>(_data[_pos + 1]) << 16)
      | (static_cast<std::uint32_t>(_data[_pos + 2]) << 8)
      | static_cast<std::uint32_t>(_data[_pos + 3])
    };
    _pos += 4;
    return value;
  }

  std::uint64_t BufferReader::read_u64()
  {
    require(8);
    const auto hi{read_u32()};
    const auto lo{read_u32()};
    return (static_cast<std::uint64_t>(hi) << 32) | lo;
  }

  std::int32_t BufferReader::read_i32()
  {
    return std::bit_cast<std::int32_t>(read_u32());
  }

  std::int64_t BufferReader::read_i64()
  {
    return std::bit_cast<std::int64_t>(read_u64());
  }

  bool BufferReader::read_bool()
  {
    return read_u8() == 1;
  }

  std::string BufferReader::read_string()
  {
    const auto length{read_u64()};
    if (length > remaining())
    {
      throw std::out_of_range{"BufferReader: string length exceeds remaining data"};
    }
    const auto* p{reinterpret_cast<const char*>(_data.data() + _pos)};
    std::string value{p, static_cast<std::size_t>(length)};
    _pos += static_cast<std::size_t>(length);
    return value;
  }

  std::span<const std::uint8_t> BufferReader::read_bytes(const std::size_t n)
  {
    require(n);
    const auto value{_data.subspan(_pos, n)};
    _pos += n;
    return value;
  }

  std::size_t BufferReader::remaining() const noexcept
  {
    return _data.size() - _pos;
  }

  std::size_t BufferReader::position() const noexcept
  {
    return _pos;
  }

  void BufferReader::require(const std::size_t n) const
  {
    if (remaining() < n)
    {
      throw std::out_of_range{"BufferReader: insufficient data"};
    }
  }
}
