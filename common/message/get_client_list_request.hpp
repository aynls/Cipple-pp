#pragma once
#include <memory>
#include <string>

#include "message/message.hpp"

namespace cipple::tcp
{
  class BufferReader;
}

namespace cipple::message
{
  // type(20) + uuid + clientListSignature
  class GetClientListRequest final : public Message
  {
  public:
    GetClientListRequest(Uuid uuid, std::string client_list_signature);

    const std::string& client_list_signature() const noexcept;

    static std::unique_ptr<GetClientListRequest> decode(tcp::BufferReader& reader);

  protected:
    void encode_fields(tcp::BufferWriter& writer) const override;

  private:
    std::string _client_list_signature;
  };
}
