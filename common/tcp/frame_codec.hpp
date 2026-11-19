#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace cipple::message
{
  class Message;
}

namespace cipple::tcp
{
  inline constexpr std::size_t kDefaultMaxFrameLength = 16 * 1024 * 1024;

  struct FrameView
  {
    // Borrows the input buffer; keep it alive and unchanged while using this view.
    std::span<const std::uint8_t> body;
    std::size_t bytes_consumed;
  };

  // Encode int32 length (BE) + type + uuid + fields. Length excludes its own 4 bytes.
  // The configurable limit covers the body only and must be in [1, INT32_MAX].
  // Invalid limits throw std::invalid_argument; oversized frames throw std::length_error.
  std::vector<std::uint8_t> encode_frame(
    const message::Message& message, std::size_t max_frame_length = kDefaultMaxFrameLength);

  // Inspect the first frame without consuming or copying input. A partial header/body
  // returns nullopt. Invalid lengths throw std::invalid_argument, oversized frames
  // throw std::length_error. Extra input is left for the next frame.
  std::optional<FrameView> try_decode_frame(
    std::span<const std::uint8_t> data, std::size_t max_frame_length = kDefaultMaxFrameLength);
}
