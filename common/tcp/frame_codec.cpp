#include "frame_codec.hpp"

#include <limits>
#include <stdexcept>

#include "buffer_reader.hpp"
#include "buffer_writer.hpp"
#include "message/message.hpp"

namespace cipple::tcp
{
  void validate_frame_limit(const std::size_t max_frame_length)
  {
    if (max_frame_length == 0
      || max_frame_length > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()))
    {
      throw std::invalid_argument{"frame codec: max frame length must be in [1, INT32_MAX]"};
    }
  }

  std::vector<std::uint8_t> encode_frame(
    const message::Message& message, const std::size_t max_frame_length)
  {
    validate_frame_limit(max_frame_length);
    BufferWriter writer;
    writer.write_i32(0);
    message.encode(writer);
    const auto length = writer.size() - kFrameLengthPrefixSize;
    if (length > max_frame_length)
    {
      throw std::length_error{"encode_frame: frame length exceeds limit"};
    }
    writer.patch_u32(0, static_cast<std::uint32_t>(length));
    return writer.bytes();
  }

  EncodedFrame encode_frame_shared(
    const message::Message& message, const std::size_t max_frame_length)
  {
    return std::make_shared<const std::vector<std::uint8_t>>(encode_frame(message, max_frame_length));
  }

  std::size_t decode_frame_length(const std::span<const std::uint8_t, kFrameLengthPrefixSize> prefix,
                                  const std::size_t max_frame_length)
  {
    validate_frame_limit(max_frame_length);
    BufferReader reader{prefix};
    const auto signed_length = reader.read_i32();
    if (signed_length <= 0)
    {
      throw std::invalid_argument{"frame codec: frame length must be positive"};
    }
    const auto length = static_cast<std::size_t>(signed_length);
    if (length > max_frame_length)
    {
      throw std::length_error{"frame codec: frame length exceeds limit"};
    }
    return length;
  }

  std::optional<FrameView> try_decode_frame(
    const std::span<const std::uint8_t> data, const std::size_t max_frame_length)
  {
    if (data.size() < kFrameLengthPrefixSize)
    {
      validate_frame_limit(max_frame_length);
      return std::nullopt;
    }
    const auto length = decode_frame_length(data.first<kFrameLengthPrefixSize>(), max_frame_length);
    if (data.size() - kFrameLengthPrefixSize < length)
    {
      return std::nullopt;
    }
    return FrameView{.body = data.subspan(kFrameLengthPrefixSize, length),
                     .bytes_consumed = kFrameLengthPrefixSize + length};
  }
}
