#pragma once
#include <cstdint>
#include <optional>
#include <string_view>

namespace cipple::entity
{
  // 增量更新的原子操作；线上按原版编码为 string
  enum class AtomicOperation : std::uint8_t
  {
    kAddEntry,
    kRemoveEntry,
  };

  // "add_entry" / "remove_entry"
  std::string_view atomic_operation_wire_name(AtomicOperation operation) noexcept;
  std::optional<AtomicOperation> atomic_operation_parse(std::string_view name) noexcept;
}
