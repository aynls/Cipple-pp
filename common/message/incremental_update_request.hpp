#pragma once
#include <memory>
#include <string>

#include "entity/atomic_operation.hpp"
#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(16) + uuid + applicationName + key + baseMessageUuid + atomicOperation + value
  class IncrementalUpdateRequest final : public Message
  {
  public:
    IncrementalUpdateRequest(Uuid uuid, std::string application_name, std::string key,
                             Uuid base_message_uuid, entity::AtomicOperation atomic_operation,
                             std::string value);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    const Uuid& base_message_uuid() const noexcept;
    entity::AtomicOperation atomic_operation() const noexcept;
    const std::string& value() const noexcept;

    static std::unique_ptr<IncrementalUpdateRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _application_name;
    std::string _key;
    Uuid _base_message_uuid;
    entity::AtomicOperation _atomic_operation;
    std::string _value;
  };
}
