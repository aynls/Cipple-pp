#include "resolve.hpp"

namespace cipple::entity
{
  std::string apply_increment(const std::string_view base, const EntityMessage::IncrementalUpdate& update)
  {
    std::string result{base};
    if (update.atomic_operation == AtomicOperation::kAddEntry)
    {
      result.append(update.value);
      return result;
    }
    if (update.value.empty())
    {
      return result;
    }

    result.clear();
    std::size_t pos{0};
    for (auto match{base.find(update.value)}; match != std::string_view::npos; match = base.find(update.value, pos))
    {
      result.append(base.substr(pos, match - pos));
      pos = match + update.value.size();
    }
    result.append(base.substr(pos));
    return result;
  }
}
