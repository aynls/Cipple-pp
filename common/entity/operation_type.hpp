#pragma once
#include <cstdint>
#include <optional>
#include <string_view>

namespace cipple::entity
{
  // 实体消息操作类型；线上仍按原版编码为 string
  enum class OperationType : std::uint8_t
  {
    kUpdate,
    kDelete,
    kIncrementalUpdate,
  };

  // "update" / "delete" / "incremental_update"
  std::string_view operation_type_wire_name(OperationType type) noexcept;
  std::optional<OperationType> operation_type_parse(std::string_view name) noexcept;
}
