#include <gtest/gtest.h>
#include <sqlite3.h>

#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "entity/atomic_operation.hpp"
#include "entity/entity_message.hpp"
#include "message/dispatch_request.hpp"
#include "subscription_store.hpp"
#include "uuid.hpp"

namespace
{
  using cipple::Uuid;
  using cipple::client::SubscriptionStore;
  using cipple::entity::AtomicOperation;
  using cipple::entity::EntityMessage;
  using cipple::message::DispatchRequest;

  EntityMessage update(const std::int64_t version, std::string value)
  {
    return {Uuid::generate(), "app", "key", version, 1, EntityMessage::Update{std::move(value)}};
  }

  DispatchRequest delivery(const EntityMessage& entity, const std::int64_t sequence,
                           std::optional<EntityMessage> base = std::nullopt)
  {
    return {Uuid::generate(), "", entity, sequence, std::move(base)};
  }

  class SubscriptionStoreTest : public testing::Test
  {
  protected:
    void TearDown() override
    {
      std::error_code error;
      std::filesystem::remove(_db, error);
      EXPECT_FALSE(error) << error.message();
      std::filesystem::remove(_db.string() + "-wal", error);
      std::filesystem::remove(_db.string() + "-shm", error);
    }

    const std::filesystem::path _db =
        std::filesystem::temp_directory_path() / ("cipple-subscription-" + Uuid::generate().to_string() + ".db");
  };

  TEST_F(SubscriptionStoreTest, FirstDeliveryIsNewAndCurrent)
  {
    SubscriptionStore store{_db.string()};
    const auto accepted = store.accept("ep", delivery(update(1, "value"), 7));
    EXPECT_TRUE(accepted.first_seen);
    EXPECT_TRUE(accepted.current);
    ASSERT_TRUE(accepted.value);
    EXPECT_EQ(*accepted.value, "value");
    EXPECT_EQ(store.cursor("ep", "app", "key"), 7);
  }

  TEST_F(SubscriptionStoreTest, RedeliveryOnlyAdvancesTheCursor)
  {
    SubscriptionStore store{_db.string()};
    const auto entity = update(1, "value");
    ASSERT_TRUE(store.accept("ep", delivery(entity, 1)).first_seen);
    const auto replayed = store.accept("ep", delivery(entity, 2));
    EXPECT_FALSE(replayed.first_seen);
    EXPECT_EQ(store.cursor("ep", "app", "key"), 2);
  }

  TEST_F(SubscriptionStoreTest, ConflictingUuidFailsWithoutWritingAnything)
  {
    SubscriptionStore store{_db.string()};
    const auto entity = update(1, "first");
    ASSERT_TRUE(store.accept("ep", delivery(entity, 1)).first_seen);
    const EntityMessage changed{entity.uuid(), "app", "key", 2, 1, EntityMessage::Update{"different"}};
    EXPECT_THROW(store.accept("ep", delivery(changed, 2)), std::runtime_error);
    EXPECT_EQ(store.cursor("ep", "app", "key"), 1);
    const auto again = store.accept("ep", delivery(entity, 3));
    EXPECT_FALSE(again.first_seen);
    EXPECT_EQ(store.cursor("ep", "app", "key"), 3);
  }

  TEST_F(SubscriptionStoreTest, IncrementResolvesFromStoredBase)
  {
    SubscriptionStore store{_db.string()};
    const auto base = update(1, "a-x-x-b");
    ASSERT_TRUE(store.accept("ep", delivery(base, 1)).first_seen);
    const EntityMessage delta{Uuid::generate(), "app", "key", 2, 1,
        EntityMessage::IncrementalUpdate{base.uuid(), AtomicOperation::kRemoveEntry, "-x"}};
    const auto accepted = store.accept("ep", delivery(delta, 2));
    EXPECT_TRUE(accepted.first_seen);
    ASSERT_TRUE(accepted.value);
    EXPECT_EQ(*accepted.value, "a-b");
  }

  TEST_F(SubscriptionStoreTest, IncrementCanUseTheCarriedFullBase)
  {
    SubscriptionStore store{_db.string()};
    const auto base = update(1, "a-x-x-b");
    const EntityMessage delta{Uuid::generate(), "app", "key", 2, 1,
        EntityMessage::IncrementalUpdate{base.uuid(), AtomicOperation::kRemoveEntry, "-x"}};
    const auto accepted = store.accept("ep", delivery(delta, 1, base));
    EXPECT_TRUE(accepted.first_seen);
    ASSERT_TRUE(accepted.value);
    EXPECT_EQ(*accepted.value, "a-b");
  }

  TEST_F(SubscriptionStoreTest, IncrementWithoutAnyBaseFails)
  {
    SubscriptionStore store{_db.string()};
    const EntityMessage delta{Uuid::generate(), "app", "key", 2, 1,
        EntityMessage::IncrementalUpdate{Uuid::generate(), AtomicOperation::kAddEntry, "x"}};
    EXPECT_THROW(store.accept("ep", delivery(delta, 1)), std::runtime_error);
    EXPECT_EQ(store.cursor("ep", "app", "key"), 0);
  }

  TEST_F(SubscriptionStoreTest, DeleteIsCurrentAndValueless)
  {
    SubscriptionStore store{_db.string()};
    ASSERT_TRUE(store.accept("ep", delivery(update(1, "v"), 1)).first_seen);
    const EntityMessage deletion{Uuid::generate(), "app", "key", 2, 1, EntityMessage::Delete{}};
    const auto accepted = store.accept("ep", delivery(deletion, 2));
    EXPECT_TRUE(accepted.first_seen);
    EXPECT_TRUE(accepted.current);
    EXPECT_FALSE(accepted.value);
  }

  TEST_F(SubscriptionStoreTest, OlderVersionIsStoredButNotCurrent)
  {
    SubscriptionStore store{_db.string()};
    ASSERT_TRUE(store.accept("ep", delivery(update(2, "newer"), 1)).first_seen);
    const auto older = store.accept("ep", delivery(update(1, "older"), 2));
    EXPECT_TRUE(older.first_seen);
    EXPECT_FALSE(older.current);
  }

  TEST_F(SubscriptionStoreTest, CursorIsScopedToEndpointApplicationAndKey)
  {
    SubscriptionStore store{_db.string()};
    const auto entity = update(1, "v");
    ASSERT_TRUE(store.accept("ep1", delivery(entity, 5)).first_seen);
    EXPECT_EQ(store.cursor("ep1", "app", "key"), 5);
    EXPECT_EQ(store.cursor("ep2", "app", "key"), 0);
    EXPECT_EQ(store.cursor("ep1", "other", "key"), 0);
    const auto elsewhere = store.accept("ep2", delivery(entity, 9));
    EXPECT_FALSE(elsewhere.first_seen);
    EXPECT_EQ(store.cursor("ep2", "app", "key"), 9);
  }

  TEST_F(SubscriptionStoreTest, DedupAndCursorSurviveReopen)
  {
    const auto entity = update(1, "v");
    {
      SubscriptionStore store{_db.string()};
      ASSERT_TRUE(store.accept("ep", delivery(entity, 4)).first_seen);
    }
    SubscriptionStore reopened{_db.string()};
    EXPECT_EQ(reopened.cursor("ep", "app", "key"), 4);
    EXPECT_FALSE(reopened.accept("ep", delivery(entity, 5)).first_seen);
    EXPECT_EQ(reopened.cursor("ep", "app", "key"), 5);
  }

  TEST_F(SubscriptionStoreTest, NonPositiveSequenceIsRejected)
  {
    SubscriptionStore store{_db.string()};
    EXPECT_THROW(store.accept("ep", delivery(update(1, "v"), 0)), std::runtime_error);
  }

  TEST(SubscriptionStoreCtorTest, RejectsInMemoryAndEmptyPaths)
  {
    EXPECT_THROW(SubscriptionStore{":memory:"}, std::invalid_argument);
    EXPECT_THROW(SubscriptionStore{""}, std::invalid_argument);
  }
}
