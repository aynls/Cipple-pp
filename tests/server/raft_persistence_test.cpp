#include <gtest/gtest.h>

#include <libnuraft/buffer.hxx>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/srv_state.hxx>

#include <filesystem>
#include <system_error>

#include "raft/raft_persistence.hpp"
#include "uuid.hpp"

namespace
{
  using cipple::raft::RaftOptions;
  using cipple::raft::RaftStateManager;

  class RaftPersistenceTest : public testing::Test
  {
  protected:
    void SetUp() override
    {
      ASSERT_TRUE(std::filesystem::create_directory(_directory));
    }

    void TearDown() override
    {
      std::error_code error;
      std::filesystem::remove_all(_directory, error);
      EXPECT_FALSE(error) << error.message();
    }

    static void append(nuraft::log_store& store, const nuraft::ulong term)
    {
      auto entry{nuraft::cs_new<nuraft::log_entry>(term, nuraft::buffer::alloc(1))};
      store.append(entry);
    }

    static void vote_for_self(RaftStateManager& state)
    {
      nuraft::srv_state vote;
      vote.set_term(6);
      vote.set_voted_for(state.server_id());
      state.save_state(vote);
    }

    const std::filesystem::path _directory{
      std::filesystem::temp_directory_path() / ("cipple-raft-storage-" + cipple::Uuid::generate().to_string())
    };
    const RaftOptions _options{
      1, {{1, "127.0.0.1:7101"}, {2, "127.0.0.1:7102"}, {3, "127.0.0.1:7103"}}, _directory / "log.db"
    };
  };

  TEST_F(RaftPersistenceTest, InstalledSnapshotDiscardsConflictingSuffixAndKeepsTermAfterReopen)
  {
    {
      RaftStateManager state{_options};
      vote_for_self(state);
      const auto store{std::static_pointer_cast<cipple::raft::RaftLogStore>(state.load_log_store())};
      for (const auto term : {2, 2, 3, 4})
      {
        append(*store, term);
      }

      store->expect_snapshot(3, 5);
      ASSERT_TRUE(store->compact(3));
      EXPECT_EQ(store->start_index(), 4);
      EXPECT_EQ(store->next_slot(), 4);
      EXPECT_EQ(store->last_entry()->get_term(), 5);
    }

    RaftStateManager reopened{_options};
    const auto store{reopened.load_log_store()};
    EXPECT_EQ(store->start_index(), 4);
    EXPECT_EQ(store->next_slot(), 4);
    EXPECT_EQ(store->last_entry()->get_term(), 5);
    EXPECT_EQ(reopened.read_state()->get_term(), 6);
    EXPECT_EQ(reopened.read_state()->get_voted_for(), 1);
  }

  TEST_F(RaftPersistenceTest, InstalledSnapshotKeepsMatchingSuffix)
  {
    RaftStateManager state{_options};
    vote_for_self(state);
    const auto store{std::static_pointer_cast<cipple::raft::RaftLogStore>(state.load_log_store())};
    for (const auto term : {2, 2, 3, 4})
    {
      append(*store, term);
    }

    store->expect_snapshot(3, 3);
    ASSERT_TRUE(store->compact(3));
    EXPECT_EQ(store->start_index(), 4);
    EXPECT_EQ(store->next_slot(), 5);
    EXPECT_EQ(store->last_entry()->get_term(), 4);
  }

  TEST_F(RaftPersistenceTest, RejectsAnotherNodeIdentityInTheSameMembership)
  {
    {
      RaftStateManager state{_options};
      vote_for_self(state);
    }
    auto different{_options};
    different.node_id = 2;
    EXPECT_THROW(RaftStateManager{different}, std::runtime_error);
  }
}
