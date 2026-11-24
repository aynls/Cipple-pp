#pragma once
#include <memory>
#include <string>

#include "protocol/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // Fields after the RPC UUID are applicationName + key.
  class GetRequest final : public Message
  {
  public:
    GetRequest(Uuid uuid, std::string application_name, std::string key);

    const std::string& application_name() const noexcept;
    const std::string& key() const noexcept;

    static std::unique_ptr<GetRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _application_name;
    std::string _key;
  };
}
