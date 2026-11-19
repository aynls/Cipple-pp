#include "operation_type.hpp"

namespace cipple::entity
{
  std::string_view operation_type_wire_name(const OperationType type) noexcept
  {
    switch (type)
    {
    case OperationType::kUpdate:
      return "update";
    case OperationType::kDelete:
      return "delete";
    case OperationType::kIncrementalUpdate:
      return "incremental_update";
    }
    return {};
  }

  std::optional<OperationType> operation_type_parse(const std::string_view name) noexcept
  {
    if (name == "update")
    {
      return OperationType::kUpdate;
    }
    if (name == "delete")
    {
      return OperationType::kDelete;
    }
    if (name == "incremental_update")
    {
      return OperationType::kIncrementalUpdate;
    }
    return std::nullopt;
  }
}
