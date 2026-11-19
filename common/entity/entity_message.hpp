#pragma once
#include <cstdint>
#include <string>
#include <variant>

#include "entity/atomic_operation.hpp"
#include "entity/operation_type.hpp"
#include "uuid.hpp"

namespace cipple::tcp
{
  class BufferReader;
  class BufferWriter;
}

namespace cipple::entity
{
  // AbstractMessage 对应物：一条配置更新的实体消息，构造后不可变。
  // 负载为判别联合，operation_type() 由实际持有项推出；线上按 string 编码。
  class EntityMessage
  {
  public:
    struct Update
    {
      std::string value;
    };

    struct IncrementalUpdate
    {
      Uuid base_message_uuid;
      AtomicOperation atomic_operation;
      std::string value;
    };

    struct Delete
    {
    };

    using Payload = std::variant<Update, IncrementalUpdate, Delete>;

    EntityMessage(Uuid uuid, std::string application_name, std::string key,
                  std::int64_t last_update, std::int32_t last_update_server_id,
                  Payload payload);

    const Uuid& uuid() const noexcept; // 即线上 messageUuid 字段
    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    std::int64_t last_update() const noexcept;
    std::int32_t last_update_server_id() const noexcept;
    OperationType operation_type() const noexcept;
    const Payload& payload() const noexcept;

    // 线上公共段：uuid + operationType + applicationName + key + 条件段
    //            + lastUpdate + lastUpdateServerId
    void encode(tcp::BufferWriter& writer) const;
    static EntityMessage decode(tcp::BufferReader& reader);

  private:
    Uuid _uuid;
    std::string _application_name;
    std::string _key;
    std::int64_t _last_update;
    std::int32_t _last_update_server_id;
    Payload _payload;
  };
}
