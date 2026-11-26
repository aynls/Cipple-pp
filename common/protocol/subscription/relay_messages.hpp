#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "protocol/subscription/dispatch_request.hpp"

namespace cipple::message
{
  inline constexpr std::uint16_t kMaxRelaySlots{64};

  class RelayAdvertise final : public Message
  {
  public:
    RelayAdvertise(Uuid uuid, std::uint16_t port, std::uint16_t slots);

    std::uint16_t port() const noexcept;
    std::uint16_t slots() const noexcept;
    static std::unique_ptr<RelayAdvertise> decode(tcp::BufferReader& reader);

  private:
    void encode_fields(tcp::BufferWriter& writer) const override;

    std::uint16_t _port;
    std::uint16_t _slots;
  };

  // The UUID identifies one link. An empty address authorizes its captain;
  // a concrete endpoint tells its member where to connect.
  class RelayRoute final : public Message
  {
  public:
    RelayRoute(Uuid token, std::string application_name, std::string key,
               std::string address, std::uint16_t port);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;
    const std::string& address() const noexcept;
    std::uint16_t port() const noexcept;
    static std::unique_ptr<RelayRoute> decode(tcp::BufferReader& reader);

  private:
    void encode_fields(tcp::BufferWriter& writer) const override;

    std::string _application_name;
    std::string _key;
    std::string _address;
    std::uint16_t _port;
  };

  class RelayStatus final : public Message
  {
  public:
    RelayStatus(Uuid token, bool ready);

    bool ready() const noexcept;
    static std::unique_ptr<RelayStatus> decode(tcp::BufferReader& reader);

  private:
    void encode_fields(tcp::BufferWriter& writer) const override;

    bool _ready;
  };

  class RelayHello final : public Message
  {
  public:
    explicit RelayHello(Uuid token);

    static std::unique_ptr<RelayHello> decode(tcp::BufferReader& reader);

  private:
    void encode_fields(tcp::BufferWriter& writer) const noexcept override;
  };

  class RelayDispatch final : public Message
  {
  public:
    RelayDispatch(DispatchRequest dispatch, bool deliver_local, std::vector<Uuid> targets);

    const DispatchRequest& dispatch() const noexcept;
    bool deliver_local() const noexcept;
    const std::vector<Uuid>& targets() const noexcept;
    static std::unique_ptr<RelayDispatch> decode(tcp::BufferReader& reader);

  private:
    void encode_fields(tcp::BufferWriter& writer) const override;

    DispatchRequest _dispatch;
    bool _deliver_local;
    std::vector<Uuid> _targets;
  };
}
