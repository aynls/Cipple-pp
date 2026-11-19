#include "message_decoder.hpp"

#include <array>
#include <stdexcept>

#include "buffer_reader.hpp"
#include "message/ack_request.hpp"
#include "message/ack_response.hpp"
#include "message/delete_request.hpp"
#include "message/delete_response.hpp"
#include "message/dispatch_request.hpp"
#include "message/dispatch_response.hpp"
#include "message/get_client_list_request.hpp"
#include "message/get_client_list_response.hpp"
#include "message/get_request.hpp"
#include "message/get_response.hpp"
#include "message/heartbeat_request.hpp"
#include "message/heartbeat_response.hpp"
#include "message/incremental_update_request.hpp"
#include "message/incremental_update_response.hpp"
#include "message/message.hpp"
#include "message/message_type.hpp"
#include "message/put_request.hpp"
#include "message/put_response.hpp"
#include "message/subscribe_request.hpp"
#include "message/subscribe_response.hpp"
#include "message/sync_request.hpp"
#include "message/sync_response.hpp"
#include "message/system_info_request.hpp"
#include "message/system_info_response.hpp"
#include "message/unsubscribe_request.hpp"
#include "message/unsubscribe_response.hpp"

namespace cipple::tcp
{
  namespace
  {
    template <typename T>
    std::unique_ptr<message::Message> decode_as(BufferReader& reader)
    {
      return T::decode(reader);
    }

    // Immutable dispatch table, indexed by the v1 wire type (0..23).
    constexpr std::array kDecoders{
      &decode_as<message::HeartbeatRequest>,
      &decode_as<message::HeartbeatResponse>,
      &decode_as<message::AckRequest>,
      &decode_as<message::AckResponse>,
      &decode_as<message::SyncRequest>,
      &decode_as<message::SyncResponse>,
      &decode_as<message::DeleteRequest>,
      &decode_as<message::DeleteResponse>,
      &decode_as<message::GetRequest>,
      &decode_as<message::GetResponse>,
      &decode_as<message::PutRequest>,
      &decode_as<message::PutResponse>,
      &decode_as<message::SubscribeRequest>,
      &decode_as<message::SubscribeResponse>,
      &decode_as<message::UnsubscribeRequest>,
      &decode_as<message::UnsubscribeResponse>,
      &decode_as<message::IncrementalUpdateRequest>,
      &decode_as<message::IncrementalUpdateResponse>,
      &decode_as<message::DispatchRequest>,
      &decode_as<message::DispatchResponse>,
      &decode_as<message::GetClientListRequest>,
      &decode_as<message::GetClientListResponse>,
      &decode_as<message::SystemInfoRequest>,
      &decode_as<message::SystemInfoResponse>,
    };

    static_assert(kDecoders.size() == static_cast<std::uint8_t>(message::MessageType::kSystemInfoResponse) + 1);
  }

  std::unique_ptr<message::Message> decode_message(const std::span<const std::uint8_t> body)
  {
    BufferReader reader{body};
    const auto type = reader.read_u8();
    if (!message::message_type_from_byte(type).has_value() || type >= kDecoders.size())
    {
      throw std::invalid_argument{"decode_message: unknown message type"};
    }
    auto result = kDecoders[type](reader);
    if (reader.remaining() != 0)
    {
      throw std::invalid_argument{"decode_message: trailing bytes in frame body"};
    }
    return result;
  }
}
