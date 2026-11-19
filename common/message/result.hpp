#pragma once
#include <cstdint>

namespace cipple::message
{
  // SyncResponse 结果码，线上单字节，取值沿用原版
  enum class Result : std::uint8_t
  {
    kSuccess = 0,
    kDuplicated = 1,
  };
}
