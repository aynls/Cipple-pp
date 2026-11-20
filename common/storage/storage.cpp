#include "storage.hpp"

#include <utility>

namespace cipple::storage
{
  Storage::~Storage() noexcept = default;

  std::optional<entity::EntityMessage> Storage::latest(const std::string_view application_name,
                                                       const std::string_view key) const
  {
    auto versions = history(application_name, key);
    if (versions.empty())
    {
      return std::nullopt;
    }
    return std::move(versions.back());
  }
}
