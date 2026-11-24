#pragma once
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace cipple::tcp
{
  // BufferWriter 的解码对应物：大端序，从只读字节缓冲顺序读取。
  // 数据不足时所有 read_* 抛 std::out_of_range。
  class BufferReader
  {
  public:
    explicit BufferReader(std::span<const std::uint8_t> data) noexcept;

    std::uint8_t read_u8();
    std::uint32_t read_u32();
    std::uint64_t read_u64();
    std::int32_t read_i32();
    std::int64_t read_i64();
    bool read_bool(); // 单字节，1 为 true
    std::string read_string(); // u64 长度前缀 + UTF-8 字节
    std::span<const std::uint8_t> read_bytes(std::size_t n); // 裸字节，零拷贝

    // 定长裸字节，如 uuid 的 16 字节；返回定长 extent 供 from_bytes 直接用
    template <std::size_t N>
    std::span<const std::uint8_t, N> read_fixed()
    {
      return read_bytes(N).first<N>();
    }

    std::size_t remaining() const noexcept;
    std::size_t position() const noexcept;

  private:
    void require(std::size_t n) const;

    std::span<const std::uint8_t> _data;
    std::size_t _pos{0};
  };
}
