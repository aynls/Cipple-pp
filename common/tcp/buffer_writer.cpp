#include "buffer_writer.hpp"

#include <bit>
#include <stdexcept>

namespace cipple::tcp
{
  void BufferWriter::write_u8(const std::uint8_t value)
  {
    _buffer.push_back(value);
  }

  void BufferWriter::write_u32(const std::uint32_t value)
  {
    _buffer.push_back(static_cast<std::uint8_t>(value >> 24));
    _buffer.push_back(static_cast<std::uint8_t>(value >> 16));
    _buffer.push_back(static_cast<std::uint8_t>(value >> 8));
    _buffer.push_back(static_cast<std::uint8_t>(value));
  }

  void BufferWriter::write_u64(const std::uint64_t value)
  {
    write_u32(static_cast<std::uint32_t>(value >> 32));
    write_u32(static_cast<std::uint32_t>(value));
  }

  void BufferWriter::write_i32(const std::int32_t value)
  {
    write_u32(std::bit_cast<std::uint32_t>(value));
  }

  void BufferWriter::write_i64(const std::int64_t value)
  {
    write_u64(std::bit_cast<std::uint64_t>(value));
  }

  void BufferWriter::write_string(const std::string_view value)
  {
    write_u64(static_cast<std::uint64_t>(value.size()));
    write_bytes({reinterpret_cast<const std::uint8_t*>(value.data()), value.size()});
  }

  void BufferWriter::write_bytes(const std::span<const std::uint8_t> value)
  {
    _buffer.insert(_buffer.end(), value.begin(), value.end());
  }

  void BufferWriter::patch_u32(const std::size_t pos, const std::uint32_t value)
  {
    if (pos + 4 > _buffer.size())
    {
      throw std::out_of_range{"BufferWriter::patch_u32: position out of range"};
    }
    _buffer[pos] = static_cast<std::uint8_t>(value >> 24);
    _buffer[pos + 1] = static_cast<std::uint8_t>(value >> 16);
    _buffer[pos + 2] = static_cast<std::uint8_t>(value >> 8);
    _buffer[pos + 3] = static_cast<std::uint8_t>(value);
  }

  std::size_t BufferWriter::size() const noexcept
  {
    return _buffer.size();
  }

  void BufferWriter::clear() noexcept
  {
    _buffer.clear();
  }

  void BufferWriter::reserve(const std::size_t n)
  {
    _buffer.reserve(n);
  }

  const std::vector<std::uint8_t>& BufferWriter::bytes() const noexcept
  {
    return _buffer;
  }
}
