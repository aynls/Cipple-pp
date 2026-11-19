#include "atomic_operation.hpp"

namespace cipple::entity
{
  std::string_view atomic_operation_wire_name(const AtomicOperation operation) noexcept
  {
    switch (operation)
    {
    case AtomicOperation::kAddEntry:
      return "add_entry";
    case AtomicOperation::kRemoveEntry:
      return "remove_entry";
    }
    return {};
  }

  std::optional<AtomicOperation> atomic_operation_parse(const std::string_view name) noexcept
  {
    if (name == "add_entry")
    {
      return AtomicOperation::kAddEntry;
    }
    if (name == "remove_entry")
    {
      return AtomicOperation::kRemoveEntry;
    }
    return std::nullopt;
  }
}
