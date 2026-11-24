#include "benches.hpp"

#include <chrono>
#include <cstdio>

#include "entity/entity_message.hpp"
#include "protocol/subscription/dispatch_request.hpp"
#include "protocol/frame_codec.hpp"
#include "uuid.hpp"

namespace
{
  using Clock = std::chrono::steady_clock;

  double us_per(Clock::duration total, std::size_t ops)
  {
    return std::chrono::duration<double, std::micro>(total).count() / static_cast<double>(ops);
  }
}

namespace cipple::bench
{
  // 对比扇出编码的两种成本形状：每个订阅者各自 encode_frame（旧路径）
  // 与一次 encode_frame_shared 全体复用（新路径）。
  int codec(std::span<const char* const> args)
  {
    const auto subs = arg_count(args, "--subs", 500);
    const auto reps = arg_count(args, "--reps", 100);
    const auto value_bytes = arg_count(args, "--value-bytes", 256);

    const entity::EntityMessage entity{
      Uuid::generate(), "bench-app", "bench-key",
      1'700'000'000'000, 1,
      entity::EntityMessage::Update{std::string(value_bytes, 'x')}
    };
    const message::DispatchRequest dispatch{Uuid::generate(), entity, 7};

    volatile std::size_t sink = 0;
    const auto a0 = Clock::now();
    for (std::size_t r = 0; r < reps; ++r)
    {
      for (std::size_t i = 0; i < subs; ++i)
      {
        sink += tcp::encode_frame(dispatch).size();
      }
    }
    const auto per_session = Clock::now() - a0;

    const auto b0 = Clock::now();
    for (std::size_t r = 0; r < reps; ++r)
    {
      sink += tcp::encode_frame_shared(dispatch)->size();
    }
    const auto shared = Clock::now() - b0;
    (void)sink;

    const auto pairs = reps * subs;
    std::printf("codec bench: subs=%zu reps=%zu value_bytes=%zu\n", subs, reps, value_bytes);
    std::printf("  frame bytes:            %zu\n", tcp::encode_frame(dispatch).size());
    std::printf("  per-session encode:     %8.2f us/pair  (%6.1f us/op, total %.1f ms)\n",
                us_per(per_session, pairs), us_per(per_session, reps),
                std::chrono::duration<double, std::milli>(per_session).count());
    std::printf("  shared encode:          %8.4f us/pair  (%6.1f us/op, total %.1f ms)\n",
                us_per(shared, pairs), us_per(shared, reps),
                std::chrono::duration<double, std::milli>(shared).count());
    return 0;
  }
}
