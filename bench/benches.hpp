#pragma once
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>

namespace cipple::bench
{
  int codec(std::span<const char* const> args);
  int fanout(std::span<const char* const> args);
  int commit(std::span<const char* const> args);
  int drive(std::span<const char* const> args);

  inline std::size_t arg_count(std::span<const char* const> args, std::string_view name,
                               std::size_t fallback)
  {
    for (std::size_t i = 0; i + 1 < args.size(); ++i)
    {
      if (name == args[i])
      {
        return std::strtoull(args[i + 1], nullptr, 10);
      }
    }
    return fallback;
  }

  inline std::string arg_string(std::span<const char* const> args, std::string_view name,
                                std::string fallback = {})
  {
    for (std::size_t i = 0; i + 1 < args.size(); ++i)
    {
      if (name == args[i])
      {
        return args[i + 1];
      }
    }
    return fallback;
  }

  inline bool arg_flag(std::span<const char* const> args, std::string_view name)
  {
    for (const auto* arg : args)
    {
      if (name == arg)
      {
        return true;
      }
    }
    return false;
  }
}
