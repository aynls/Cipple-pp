#include "uuid.hpp"

#include <algorithm>
#include <random>

namespace cipple
{
  Uuid Uuid::generate()
  {
    thread_local std::mt19937_64 rng{
      []
      {
        std::random_device rd;
        std::seed_seq seed{rd(), rd(), rd(), rd()};
        return std::mt19937_64{seed};
      }()
    };

    Uuid id;
    const auto fill{
      [&id](const int offset)
      {
        auto bits{rng()};
        for (int i{0}; i < 8; ++i)
        {
          id._bytes[offset + i] = static_cast<std::uint8_t>(bits >> 56);
          bits <<= 8;
        }
      }
    };
    fill(0);
    fill(8);

    // RFC 4122：版本 4 + variant 10xx
    id._bytes[6] = (id._bytes[6] & 0x0F) | 0x40;
    id._bytes[8] = (id._bytes[8] & 0x3F) | 0x80;
    return id;
  }

  Uuid Uuid::from_bytes(const std::span<const std::uint8_t, 16> bytes) noexcept
  {
    Uuid id;
    std::ranges::copy(bytes, id._bytes.begin());
    return id;
  }

  std::optional<Uuid> Uuid::parse(const std::string_view text) noexcept
  {
    if (text.size() != 36)
    {
      return std::nullopt;
    }

    const auto hex{
      [](const char ch) -> int
      {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
        return -1;
      }
    };

    Uuid id;
    int nibble{0};
    for (std::size_t i{0}; i < 36; ++i)
    {
      const char c{text[i]};
      if (i == 8 || i == 13 || i == 18 || i == 23)
      {
        if (c != '-') return std::nullopt;
        continue;
      }
      const int v{hex(c)};
      if (v < 0) return std::nullopt;
      if (nibble % 2 == 0)
      {
        id._bytes[nibble / 2] = static_cast<std::uint8_t>(v << 4);
      }
      else
      {
        id._bytes[nibble / 2] |= static_cast<std::uint8_t>(v);
      }
      ++nibble;
    }
    return id;
  }

  const std::array<std::uint8_t, 16>& Uuid::bytes() const noexcept
  {
    return _bytes;
  }

  std::string Uuid::to_string() const
  {
    static constexpr char kDigits[]{"0123456789abcdef"};
    std::string out(36, '-');
    std::size_t j{0};
    for (std::size_t i{0}; i < _bytes.size(); ++i)
    {
      if (i == 4 || i == 6 || i == 8 || i == 10)
      {
        ++j; // 跳过连字符位
      }
      out[j] = kDigits[_bytes[i] >> 4];
      out[j + 1] = kDigits[_bytes[i] & 0x0F];
      j += 2;
    }
    return out;
  }
}
