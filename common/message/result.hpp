#pragma once
#include <cstdint>

namespace cipple::message
{
  // SyncResponse 使用一个字节携带结果码。
  enum class Result : std::uint8_t
  {
    kSuccess = 0,
    kDuplicated = 1,
    kRejected = 2,
  };
}
