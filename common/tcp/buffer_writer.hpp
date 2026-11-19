#pragma once
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace cipple::tcp
{
  // 大端序，向内部_buffer追加字节，无容量上限。
  class BufferWriter
  {
  public:
    void write_u8(std::uint8_t value);
    void write_u32(std::uint32_t value);
    void write_u64(std::uint64_t value);
    void write_i32(std::int32_t value);
    void write_i64(std::int64_t value);
    void write_bool(bool value); // 单字节 0/1
    void write_f64(double value); // IEEE-754 8 字节 BE
    void write_string(std::string_view value); // u64 长度前缀 + UTF-8 字节
    void write_bytes(std::span<const std::uint8_t> value); // 裸字节追加，供 uuid 等定长字段用

    // 回填 4 字节大端整数，供帧编码补写 int32 length 用；越界抛 std::out_of_range
    void patch_u32(std::size_t pos, std::uint32_t value);

    std::size_t size() const noexcept;
    void clear() noexcept;
    void reserve(std::size_t n);
    const std::vector<std::uint8_t>& bytes() const noexcept;

  private:
    std::vector<std::uint8_t> _buffer;
  };
}
