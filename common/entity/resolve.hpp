#pragma once
#include <string>
#include <string_view>

#include "entity_message.hpp"

namespace cipple::entity
{
  // Increments use a full configuration as their base and operate on literal strings.
  std::string apply_increment(std::string_view base, const EntityMessage::IncrementalUpdate& update);
}
