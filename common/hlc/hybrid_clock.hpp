#pragma once
#include <cstdint>

namespace cipple::hlc
{
  // Packs physical milliseconds and 16 logical bits into the existing version field.
  class HybridClock final
  {
  public:
    explicit HybridClock(std::int64_t offset_ms = 0);

    void observe(std::int64_t version);
    std::int64_t tick();

  private:
    std::int64_t _offset_ms;
    std::int64_t _last = 0;
  };
}
