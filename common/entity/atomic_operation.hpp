#pragma once
#include <cstdint>
#include <optional>
#include <string_view>

namespace cipple::entity
{
  // 增量更新的原子操作在线上编码为字符串。
  enum class AtomicOperation : std::uint8_t
  {
    kAddEntry,
    kRemoveEntry,
  };

  // "add_entry" / "remove_entry"
  std::string_view atomic_operation_wire_name(AtomicOperation operation) noexcept;
  std::optional<AtomicOperation> atomic_operation_parse(std::string_view name) noexcept;
}
