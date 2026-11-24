#include "benches.hpp"

#include <cstdio>
#include <cstring>
#include <span>
#include <string_view>

int main(int argc, char** argv)
{
  const std::span args{const_cast<const char**>(argv + 1), static_cast<std::size_t>(argc - 1)};
  if (args.empty())
  {
    std::fprintf(stderr,
                 "usage: cipple_bench <codec|fanout|commit|drive> [args]\n"
                 "  codec  [--subs N] [--reps R] [--value-bytes B]\n"
                 "  fanout [--subs N] [--ops M] [--warmup W] [--value-bytes B]\n"
                 "  commit [--ops M]\n"
                 "  drive  --server host:port [--subs N] [--ops M] [--warmup W] [--gap-ms G]\n"
                 "         [--value-bytes B] [--resume-after SEQ] [--deadline-ms D]\n");
    return 2;
  }
  const std::string_view command{args[0]};
  const auto rest{args.subspan(1)};
  if (command == "codec")
  {
    return cipple::bench::codec(rest);
  }
  if (command == "fanout")
  {
    return cipple::bench::fanout(rest);
  }
  if (command == "commit")
  {
    return cipple::bench::commit(rest);
  }
  if (command == "drive")
  {
    return cipple::bench::drive(rest);
  }
  std::fprintf(stderr, "unknown command: %.*s\n", static_cast<int>(command.size()), command.data());
  return 2;
}
