#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "entity/entity_message.hpp"
#include "storage/storage.hpp"
#include "storage/storage_writer.hpp"
#include "uuid.hpp"

namespace
{
  namespace asio = boost::asio;
  using cipple::Uuid;
  using cipple::entity::EntityMessage;
  using cipple::storage::AppendResult;
  using cipple::storage::ItemKey;
  using cipple::storage::JournalEntry;
  using cipple::storage::MessageUnavailable;
  using cipple::storage::Storage;
  using cipple::storage::StorageWriter;

  constexpr auto kWait = std::chrono::seconds{10};
  constexpr std::size_t kRoomyBytes = 256u << 20;

  class FakeStorage final : public Storage
  {
  public:
    AppendResult append(const EntityMessage&, std::span<const std::int32_t>,
                        const EntityMessage*) override
    {
      return AppendResult::kInserted;
    }
    cipple::storage::AppendOutcome append_with_outcome(const EntityMessage&, std::span<const std::int32_t>,
                                                     const EntityMessage*) override
    {
      return {AppendResult::kInserted, true};
    }
    bool mark_acked(const Uuid&, std::int32_t) override
    {
      return true;
    }
    std::vector<ItemKey> list_keys() const override
    {
      return {};
    }
    std::vector<EntityMessage> history(std::string_view, std::string_view) const override
    {
      return {};
    }
    std::expected<EntityMessage, MessageUnavailable> find_message(const Uuid&) const override
    {
      return std::unexpected{MessageUnavailable::kMissing};
    }
    std::vector<EntityMessage> pending_for_node(std::int32_t, std::int64_t) const override
    {
      return {};
    }
    std::vector<Uuid> pending_ids_for_node(std::int32_t, std::int64_t) const override
    {
      return {};
    }
    std::vector<JournalEntry> journal_after(std::string_view, std::string_view, std::int64_t,
                                          std::int64_t) const override
    {
      return {};
    }
    std::int64_t journal_sequence(const Uuid&) const override
    {
      return 0;
    }
    std::int64_t version_watermark() const override
    {
      return 0;
    }
    std::size_t recycle(std::string_view, std::string_view, std::size_t) override
    {
      return 0;
    }
  };

  std::unique_ptr<Storage> fake_storage()
  {
    return std::make_unique<FakeStorage>();
  }

  const std::function<void(Storage&)> kNoop = [](Storage&) {};

  // Parks the writer thread until open(): entered proves the task is running.
  struct Gate
  {
    std::function<void(Storage&)> task()
    {
      return [this](Storage&) {
        entered.set_value();
        released.wait();
      };
    }

    bool wait_entered()
    {
      return entered.get_future().wait_for(kWait) == std::future_status::ready;
    }

    void open()
    {
      released_promise.set_value();
    }

    std::promise<void> entered;
    std::promise<void> released_promise;
    std::shared_future<void> released{released_promise.get_future().share()};
  };

  // Fires when its task ran on the writer thread (task()) or its done ran on
  // the io_context (done()).
  struct Signal
  {
    std::function<void(Storage&)> task()
    {
      return [promise = promise](Storage&) { promise->set_value(); };
    }

    std::function<void()> done()
    {
      return [promise = promise] { promise->set_value(); };
    }

    bool wait() const
    {
      return future.wait_for(kWait) == std::future_status::ready;
    }

    std::shared_ptr<std::promise<void>> promise = std::make_shared<std::promise<void>>();
    std::future<void> future = promise->get_future();
  };
}

namespace cipple::storage
{
  class StorageWriterTest : public testing::Test
  {
  protected:
    asio::io_context _io;
    const StorageWriter::Limits _roomy{};
  };

  TEST_F(StorageWriterTest, RejectsRequestsOnceTheLaneIsFull)
  {
    StorageWriter writer{fake_storage(), fake_storage(), {2, kRoomyBytes, 8}};
    Gate gate;
    ASSERT_TRUE(writer.submit(_io, gate.task()));
    ASSERT_TRUE(gate.wait_entered());        // the running task still occupies its slot
    ASSERT_TRUE(writer.submit(_io, kNoop));  // second and last request slot
    EXPECT_FALSE(writer.submit(_io, kNoop));
    gate.open();
  }

  TEST_F(StorageWriterTest, ByteChargesBoundTheRequestLaneSeparately)
  {
    StorageWriter writer{fake_storage(), fake_storage(), {8, 1u << 20, 4}};
    Gate gate;
    ASSERT_TRUE(writer.submit(_io, gate.task()));
    ASSERT_TRUE(gate.wait_entered());
    ASSERT_TRUE(writer.submit(_io, kNoop, {}, 700'000));
    EXPECT_FALSE(writer.submit(_io, kNoop, {}, 400'000));
    ASSERT_TRUE(writer.submit(_io, kNoop, {}, 348'576));   // exactly the 1 MiB budget
    EXPECT_FALSE(writer.submit(_io, kNoop, {}, 1));
    gate.open();
  }

  TEST_F(StorageWriterTest, AckLaneKeepsItsReserveUnderRequestPressure)
  {
    StorageWriter writer{fake_storage(), fake_storage(), {1, kRoomyBytes, 2}};
    Gate gate;
    ASSERT_TRUE(writer.submit(_io, gate.task()));
    ASSERT_TRUE(gate.wait_entered());
    EXPECT_FALSE(writer.submit(_io, kNoop));
    Signal a1, a2;
    EXPECT_TRUE(writer.submit_ack(_io, a1.task()));
    EXPECT_TRUE(writer.submit_ack(_io, a2.task()));
    EXPECT_FALSE(writer.submit_ack(_io, kNoop));
    gate.open();
    EXPECT_TRUE(a1.wait());
    EXPECT_TRUE(a2.wait());
  }

  TEST_F(StorageWriterTest, PostedDoneStillChargesTheRequestLane)
  {
    StorageWriter writer{fake_storage(), fake_storage(), {2, kRoomyBytes, 8}};
    Gate gate, probe;
    Signal second, posted;
    ASSERT_TRUE(writer.submit(_io, gate.task()));
    ASSERT_TRUE(gate.wait_entered());
    ASSERT_TRUE(writer.submit(_io, second.task(), posted.done()));
    EXPECT_FALSE(writer.submit(_io, kNoop));
    gate.open();
    // second only runs after the first slot was released, and probe only runs
    // after second's done was posted to the idle io_context. The still-held
    // charge below can therefore only be the unconsumed done's.
    ASSERT_TRUE(second.wait());
    ASSERT_TRUE(writer.submit(_io, probe.task()));
    ASSERT_TRUE(probe.wait_entered());
    EXPECT_FALSE(writer.submit(_io, kNoop));
    EXPECT_EQ(_io.poll(), 1u);
    EXPECT_TRUE(posted.wait());
    EXPECT_TRUE(writer.submit(_io, kNoop));
    probe.open();
  }

  TEST_F(StorageWriterTest, StopDrainsAcceptedTasksAndPostsDones)
  {
    StorageWriter writer{fake_storage(), fake_storage(), _roomy};
    Gate gate;
    Signal t1, t2, d1, d2;
    ASSERT_TRUE(writer.submit(_io, gate.task()));
    ASSERT_TRUE(gate.wait_entered());
    ASSERT_TRUE(writer.submit(_io, t1.task(), d1.done()));
    ASSERT_TRUE(writer.submit(_io, t2.task(), d2.done()));
    gate.open();
    writer.stop();
    EXPECT_TRUE(t1.wait());
    EXPECT_TRUE(t2.wait());
    EXPECT_EQ(_io.poll(), 2u);               // both dones were already posted
    EXPECT_TRUE(d1.wait());
    EXPECT_TRUE(d2.wait());
    EXPECT_FALSE(writer.submit(_io, kNoop));
    EXPECT_FALSE(writer.submit_ack(_io, kNoop));
  }

  TEST_F(StorageWriterTest, FailedTaskStillDeliversDoneAndDoesNotWedgeQueue)
  {
    StorageWriter writer{fake_storage(), fake_storage(), _roomy};
    Signal after, d1, d2;
    ASSERT_TRUE(writer.submit(_io, [](Storage&) { throw std::runtime_error{"boom"}; }, d1.done()));
    ASSERT_TRUE(writer.submit(_io, after.task(), d2.done()));
    ASSERT_TRUE(after.wait());
    EXPECT_EQ(_io.poll(), 2u);
    EXPECT_TRUE(d1.wait());
    EXPECT_TRUE(d2.wait());
  }

  TEST_F(StorageWriterTest, RejectedTaskAndDoneNeverRun)
  {
    StorageWriter writer{fake_storage(), fake_storage(), {1, kRoomyBytes, 0}};
    Gate gate;
    ASSERT_TRUE(writer.submit(_io, gate.task()));
    ASSERT_TRUE(gate.wait_entered());
    auto count = std::make_shared<int>(0);
    EXPECT_FALSE(writer.submit(_io, [count](Storage&) { ++*count; }, [count] { ++*count; }));
    EXPECT_FALSE(writer.submit_ack(_io, [count](Storage&) { ++*count; }));
    gate.open();
    writer.stop();
    EXPECT_EQ(*count, 0);
    EXPECT_EQ(_io.poll(), 0u);
  }

  TEST_F(StorageWriterTest, RunsTasksInSubmissionOrder)
  {
    StorageWriter writer{fake_storage(), fake_storage(), _roomy};
    std::vector<int> order;
    for (int i = 0; i < 64; ++i)
    {
      ASSERT_TRUE(writer.submit(_io, [&order, i](Storage&) { order.push_back(i); }));
    }
    writer.stop();
    ASSERT_EQ(order.size(), 64u);
    for (int i = 0; i < 64; ++i)
    {
      EXPECT_EQ(order[static_cast<std::size_t>(i)], i);
    }
  }

  TEST_F(StorageWriterTest, DefaultLimitsAdmitAMaximumSizedPayload)
  {
    StorageWriter writer{fake_storage(), fake_storage()};
    // A maximum frame can carry ~16 MiB of entity plus a same-sized base;
    // the default byte budget must accept at least one such charge.
    EXPECT_TRUE(writer.submit(_io, kNoop, {}, 32u << 20));
  }

  TEST(StorageWriterCtorTest, RequiresTwoConnections)
  {
    EXPECT_THROW((StorageWriter{nullptr, fake_storage()}), std::invalid_argument);
    EXPECT_THROW((StorageWriter{fake_storage(), nullptr}), std::invalid_argument);
  }
}
