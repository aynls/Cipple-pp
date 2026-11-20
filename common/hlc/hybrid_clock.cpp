#include "hybrid_clock.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <stdexcept>

namespace cipple::hlc
{
  namespace
  {
    constexpr std::int64_t kPackedTag = std::int64_t{1} << 62;
    constexpr std::int64_t kLogicalRange = std::int64_t{1} << 16;
    constexpr std::int64_t kLargestVersion = std::numeric_limits<std::int64_t>::max();
    constexpr std::int64_t kLargestMillis = (kLargestVersion - kPackedTag) / kLogicalRange;
    constexpr std::int64_t kMaxClockOffsetMs = 86'400'000;

    std::int64_t pack(const std::int64_t milliseconds)
    {
      if (milliseconds < 0 || milliseconds > kLargestMillis)
      {
        throw std::out_of_range{"physical clock outside supported epoch milliseconds"};
      }
      return kPackedTag + milliseconds * kLogicalRange;
    }
  }

  HybridClock::HybridClock(const std::int64_t offset_ms) : _offset_ms{offset_ms}
  {
    if (_offset_ms < -kMaxClockOffsetMs || _offset_ms > kMaxClockOffsetMs)
    {
      throw std::invalid_argument{"clock offset must be within one day"};
    }
  }

  void HybridClock::observe(const std::int64_t version)
  {
    // Pre-HLC databases ordered by signed epoch milliseconds. Retain that order on disk.
    const auto observed = version >= kPackedTag ? version : pack(std::max<std::int64_t>(0, version));
    _last = std::max(_last, observed);
  }

  std::int64_t HybridClock::tick()
  {
    using namespace std::chrono;
    const auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    const auto physical = pack(now + _offset_ms);
    if (_last == kLargestVersion)
    {
      throw std::overflow_error{"configuration version exhausted"};
    }
    _last = std::max(physical, _last + 1);
    return _last;
  }
}
