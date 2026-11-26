#include "message_decoder.hpp"

#include <array>
#include <stdexcept>

#include "binary/buffer_reader.hpp"
#include "protocol/cluster/history_lost_response.hpp"
#include "protocol/cluster/retry_response.hpp"
#include "protocol/cluster/wrong_shard_response.hpp"
#include "protocol/configuration/delete_request.hpp"
#include "protocol/configuration/delete_response.hpp"
#include "protocol/subscription/dispatch_request.hpp"
#include "protocol/subscription/dispatch_response.hpp"
#include "protocol/configuration/get_request.hpp"
#include "protocol/configuration/get_response.hpp"
#include "protocol/heartbeat/heartbeat_request.hpp"
#include "protocol/heartbeat/heartbeat_response.hpp"
#include "protocol/configuration/incremental_update_request.hpp"
#include "protocol/configuration/incremental_update_response.hpp"
#include "protocol/message.hpp"
#include "protocol/message_type.hpp"
#include "protocol/configuration/put_request.hpp"
#include "protocol/configuration/put_response.hpp"
#include "protocol/subscription/subscribe_request.hpp"
#include "protocol/subscription/subscribe_response.hpp"
#include "protocol/replication/sync_request.hpp"
#include "protocol/replication/sync_response.hpp"
#include "protocol/subscription/unsubscribe_request.hpp"
#include "protocol/subscription/unsubscribe_response.hpp"

namespace cipple::tcp
{
  namespace
  {
    template <typename T>
    std::unique_ptr<message::Message> decode_as(BufferReader& reader)
    {
      return T::decode(reader);
    }

    constexpr std::array kDecoders{
      &decode_as<message::HeartbeatRequest>,
      &decode_as<message::HeartbeatResponse>,
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
      &decode_as<message::RetryResponse>,
      &decode_as<message::HistoryLostResponse>,
      &decode_as<message::WrongShardResponse>,
    };

    static_assert(kDecoders.size() == static_cast<std::uint8_t>(message::MessageType::kWrongShardResponse) + 1);
  }

  std::unique_ptr<message::Message> decode_message(const std::span<const std::uint8_t> body)
  {
    BufferReader reader{body};
    const auto type{reader.read_u8()};
    if (type >= kDecoders.size())
    {
      throw std::invalid_argument{"decode_message: unknown message type"};
    }
    auto result{kDecoders[type](reader)};
    if (reader.remaining() != 0)
    {
      throw std::invalid_argument{"decode_message: trailing bytes in frame body"};
    }
    return result;
  }
}
