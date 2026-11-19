#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace cipple
{
  // 16 字节二进制 UUID（v4 随机）。值类型，构造后不可变；
  // 线上协议按定长（16字节）裸字节读写，不加长度前缀。
  class Uuid
  {
  public:
    static Uuid generate(); // v4 随机，线程安全
    static Uuid from_bytes(std::span<const std::uint8_t, 16>) noexcept; // 解码用
    static std::optional<Uuid> parse(std::string_view) noexcept; // "8-4-4-4-12" 格式，日志/测试用

    const std::array<std::uint8_t, 16>& bytes() const noexcept;
    std::string to_string() const;

    friend bool operator==(const Uuid&, const Uuid&) noexcept = default;
    friend auto operator<=>(const Uuid&, const Uuid&) noexcept = default;

  private:
    std::array<std::uint8_t, 16> _bytes{};
  };
}

template <>
struct std::hash<cipple::Uuid>
{
  std::size_t operator()(const cipple::Uuid& id) const noexcept
  {
    const auto& b = id.bytes();
    std::uint64_t hi = 0, lo = 0;
    for (int i = 0; i < 8; ++i)
    {
      hi = (hi << 8) | b[i];
      lo = (lo << 8) | b[i + 8];
    }
    return static_cast<std::size_t>(hi ^ (lo + 0x9E3779B97F4A7C15ULL + (hi << 6) + (hi >> 2)));
  }
};
