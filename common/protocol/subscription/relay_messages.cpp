#include "relay_messages.hpp"

#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::message
{
  namespace
  {
    Uuid read_uuid(tcp::BufferReader& reader)
    {
      return Uuid::from_bytes(reader.read_fixed<16>());
    }

    std::uint16_t read_port(tcp::BufferReader& reader)
    {
      const auto port{reader.read_u32()};
      if (port > std::numeric_limits<std::uint16_t>::max())
      {
        throw std::invalid_argument{"relay port out of range"};
      }
      return static_cast<std::uint16_t>(port);
    }
  }

  RelayAdvertise::RelayAdvertise(const Uuid uuid, const std::uint16_t port, const std::uint16_t slots)
    : Message{MessageType::kRelayAdvertise, uuid}, _port{port}, _slots{slots}
  {
    if ((_port == 0) != (_slots == 0) || _slots > kMaxRelaySlots)
    {
      throw std::invalid_argument{"invalid relay advertisement"};
    }
  }

  std::uint16_t RelayAdvertise::port() const noexcept
  {
    return _port;
  }

  std::uint16_t RelayAdvertise::slots() const noexcept
  {
    return _slots;
  }

  std::unique_ptr<RelayAdvertise> RelayAdvertise::decode(tcp::BufferReader& reader)
  {
    const auto uuid{read_uuid(reader)};
    const auto port{read_port(reader)};
    const auto slots{reader.read_u32()};
    if (slots > kMaxRelaySlots)
    {
      throw std::invalid_argument{"invalid relay slot count"};
    }
    return std::make_unique<RelayAdvertise>(uuid, port, static_cast<std::uint16_t>(slots));
  }

  void RelayAdvertise::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_u32(_port);
    writer.write_u32(_slots);
  }

  RelayRoute::RelayRoute(const Uuid token, std::string application_name, std::string key,
                         std::string address, const std::uint16_t port)
    : Message{MessageType::kRelayRoute, token}
      , _application_name{std::move(application_name)}
      , _key{std::move(key)}
      , _address{std::move(address)}
      , _port{port}
  {
    if (_address.empty() != (_port == 0))
    {
      throw std::invalid_argument{"incomplete relay endpoint"};
    }
  }

  const std::string& RelayRoute::application_name() const noexcept
  {
    return _application_name;
  }

  const std::string& RelayRoute::key() const noexcept
  {
    return _key;
  }

  const std::string& RelayRoute::address() const noexcept
  {
    return _address;
  }

  std::uint16_t RelayRoute::port() const noexcept
  {
    return _port;
  }

  std::unique_ptr<RelayRoute> RelayRoute::decode(tcp::BufferReader& reader)
  {
    const auto token{read_uuid(reader)};
    auto application_name{reader.read_string()};
    auto key{reader.read_string()};
    auto address{reader.read_string()};
    const auto port{read_port(reader)};
    return std::make_unique<RelayRoute>(token, std::move(application_name), std::move(key),
                                        std::move(address), port);
  }

  void RelayRoute::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_string(_application_name);
    writer.write_string(_key);
    writer.write_string(_address);
    writer.write_u32(_port);
  }

  RelayStatus::RelayStatus(const Uuid token, const bool ready)
    : Message{MessageType::kRelayStatus, token}, _ready{ready}
  {
  }

  bool RelayStatus::ready() const noexcept
  {
    return _ready;
  }

  std::unique_ptr<RelayStatus> RelayStatus::decode(tcp::BufferReader& reader)
  {
    const auto token{read_uuid(reader)};
    return std::make_unique<RelayStatus>(token, reader.read_bool());
  }

  void RelayStatus::encode_fields(tcp::BufferWriter& writer) const
  {
    writer.write_bool(_ready);
  }

  RelayHello::RelayHello(const Uuid token) : Message{MessageType::kRelayHello, token}
  {
  }

  std::unique_ptr<RelayHello> RelayHello::decode(tcp::BufferReader& reader)
  {
    return std::make_unique<RelayHello>(read_uuid(reader));
  }

  void RelayHello::encode_fields(tcp::BufferWriter&) const noexcept
  {
  }

  RelayDispatch::RelayDispatch(DispatchRequest dispatch, const bool deliver_local, std::vector<Uuid> targets)
    : Message{MessageType::kRelayDispatch, dispatch.uuid()}
      , _dispatch{std::move(dispatch)}
      , _deliver_local{deliver_local}
      , _targets{std::move(targets)}
  {
    if (_dispatch.sequence() <= 0 || _targets.empty() || _targets.size() > kMaxRelaySlots
      || std::set<Uuid>{_targets.begin(), _targets.end()}.size() != _targets.size())
    {
      throw std::invalid_argument{"invalid relay dispatch"};
    }
  }

  const DispatchRequest& RelayDispatch::dispatch() const noexcept
  {
    return _dispatch;
  }

  bool RelayDispatch::deliver_local() const noexcept
  {
    return _deliver_local;
  }

  const std::vector<Uuid>& RelayDispatch::targets() const noexcept
  {
    return _targets;
  }

  std::unique_ptr<RelayDispatch> RelayDispatch::decode(tcp::BufferReader& reader)
  {
    auto dispatch{DispatchRequest::decode(reader)};
    const auto deliver_local{reader.read_bool()};
    const auto count{reader.read_u32()};
    if (count == 0 || count > kMaxRelaySlots)
    {
      throw std::invalid_argument{"invalid relay target count"};
    }
    std::vector<Uuid> targets;
    targets.reserve(count);
    for (std::uint32_t index{0}; index < count; ++index)
    {
      targets.push_back(read_uuid(reader));
    }
    return std::make_unique<RelayDispatch>(std::move(*dispatch), deliver_local, std::move(targets));
  }

  void RelayDispatch::encode_fields(tcp::BufferWriter& writer) const
  {
    _dispatch.encode_fields(writer);
    writer.write_bool(_deliver_local);
    writer.write_u32(static_cast<std::uint32_t>(_targets.size()));
    for (const auto& token : _targets)
    {
      writer.write_bytes(token.bytes());
    }
  }
}
