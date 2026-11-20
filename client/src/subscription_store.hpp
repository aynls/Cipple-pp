#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

struct sqlite3;

namespace cipple::message
{
  class DispatchRequest;
}

namespace cipple::client
{
  struct AcceptedDelivery
  {
    bool first_seen;
    bool current;
    std::optional<std::string> value;
  };

  std::optional<std::string> materialize_delivery(const message::DispatchRequest& dispatch,
                                                   std::optional<std::string_view> saved_base = std::nullopt);

  // A database represents one receiving client. Replay on a different endpoint
  // uses that endpoint's cursor and shares the same set of accepted versions.
  class SubscriptionStore final
  {
  public:
    explicit SubscriptionStore(const std::string& path);
    ~SubscriptionStore() noexcept;

    SubscriptionStore(const SubscriptionStore&) = delete;
    SubscriptionStore& operator=(const SubscriptionStore&) = delete;

    std::int64_t cursor(std::string_view endpoint, std::string_view application, std::string_view key) const;
    AcceptedDelivery accept(std::string_view endpoint, const message::DispatchRequest& dispatch);

  private:
    sqlite3* _database = nullptr;
  };
}
