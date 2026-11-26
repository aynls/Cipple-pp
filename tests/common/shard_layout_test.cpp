#include <gtest/gtest.h>

#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "sharding/shard_layout.hpp"
#include "uuid.hpp"

namespace
{
  using cipple::Uuid;
  using cipple::sharding::ShardLayout;

  class TempLayoutFile
  {
  public:
    explicit TempLayoutFile(const std::string_view content)
      : _path{std::filesystem::temp_directory_path() / ("cipple-shard-layout-" + Uuid::generate().to_string())}
    {
      std::ofstream output{_path, std::ios::binary | std::ios::trunc};
      if (!output.is_open())
      {
        throw std::runtime_error{"cannot create temporary shard layout file"};
      }
      output << content;
      output.close();
      if (!output)
      {
        throw std::runtime_error{"cannot write temporary shard layout file"};
      }
    }

    ~TempLayoutFile()
    {
      std::error_code error;
      std::filesystem::remove(_path, error);
    }

    const std::filesystem::path& path() const noexcept
    {
      return _path;
    }

  private:
    std::filesystem::path _path;
  };

  template <typename Exception, typename Action>
  void expect_error_contains(Action&& action, const std::string_view expected)
  {
    try
    {
      action();
    }
    catch (const Exception& error)
    {
      EXPECT_NE(std::string_view{error.what()}.find(expected), std::string_view::npos) << error.what();
      return;
    }
    catch (...)
    {
      FAIL() << "unexpected exception type";
    }
    FAIL() << "expected an exception";
  }

  std::string unordered_layout()
  {
    return "# cluster layout\n"
           "\n"
           "10 1 127.0.0.1:9101 127.0.0.1:9201   # trailing comment\n"
           "0 3 127.0.0.1:2103 127.0.0.1:2203\n"
           "2 2 127.0.0.1:5102 127.0.0.1:5202\n"
           "0 1 127.0.0.1:2101 127.0.0.1:2201\n"
           "10 2 127.0.0.1:9102 127.0.0.1:9202\n"
           "2 1 127.0.0.1:5101 127.0.0.1:5201\n"
           "0 2 127.0.0.1:2102 127.0.0.1:2202\n";
  }

  std::string ordered_layout()
  {
    return "00 01 127.0.0.1:02101 127.0.0.1:02201\n"
           "0 2 127.0.0.1:2102 127.0.0.1:2202\n"
           "0 3 127.0.0.1:2103 127.0.0.1:2203\n"
           "2 1 127.0.0.1:5101 127.0.0.1:5201\n"
           "2 2 127.0.0.1:5102 127.0.0.1:5202\n"
           "10 1 127.0.0.1:9101 127.0.0.1:9201\n"
           "10 2 127.0.0.1:9102 127.0.0.1:9202\n";
  }

  std::string reversed_sixteen_shard_layout()
  {
    std::ostringstream content;
    for (int index{15}; index >= 0; --index)
    {
      content << index * 3 << " 1 127.0.0.1:" << 3000 + index * 2 << " 127.0.0.1:" << 3001 + index * 2 << '\n';
    }
    return content.str();
  }

  TEST(ShardLayoutTest, LoadsShardsAndMembersInSortedOrder)
  {
    const TempLayoutFile file{unordered_layout()};
    const auto layout{ShardLayout::load(file.path())};

    const auto* shard0{layout.find(0)};
    const auto* shard2{layout.find(2)};
    const auto* shard10{layout.find(10)};
    ASSERT_NE(shard0, nullptr);
    ASSERT_NE(shard2, nullptr);
    ASSERT_NE(shard10, nullptr);
    EXPECT_EQ(layout.find(7), nullptr);

    ASSERT_EQ(shard0->members.size(), 3);
    EXPECT_EQ(shard0->members[0].id, 1);
    EXPECT_EQ(shard0->members[1].id, 2);
    EXPECT_EQ(shard0->members[2].id, 3);
    ASSERT_EQ(shard2->members.size(), 2);
    EXPECT_EQ(shard2->members[0].id, 1);
    EXPECT_EQ(shard2->members[1].id, 2);
    ASSERT_EQ(shard10->members.size(), 2);
    EXPECT_EQ(shard10->members[0].id, 1);
    EXPECT_EQ(shard10->members[1].id, 2);

    const boost::asio::ip::tcp::endpoint expected_application{boost::asio::ip::address_v4::loopback(), 2101};
    const boost::asio::ip::tcp::endpoint expected_raft{boost::asio::ip::address_v4::loopback(), 2201};
    EXPECT_EQ(shard0->members[0].application, expected_application);
    EXPECT_EQ(shard0->members[0].raft, expected_raft);
  }

  TEST(ShardLayoutTest, IdentityIsCanonicalAndIndependentOfInputOrder)
  {
    const TempLayoutFile unordered{unordered_layout()};
    const TempLayoutFile ordered{ordered_layout()};
    const auto unordered_layout_object{ShardLayout::load(unordered.path())};
    const auto ordered_layout_object{ShardLayout::load(ordered.path())};

    const std::string expected{
      "cipple-shard-layout/v1\n"
      "route-hash=fnv1a64-u64be-length-prefixed/v1\n"
      "0 1 127.0.0.1:2101 127.0.0.1:2201\n"
      "0 2 127.0.0.1:2102 127.0.0.1:2202\n"
      "0 3 127.0.0.1:2103 127.0.0.1:2203\n"
      "2 1 127.0.0.1:5101 127.0.0.1:5201\n"
      "2 2 127.0.0.1:5102 127.0.0.1:5202\n"
      "10 1 127.0.0.1:9101 127.0.0.1:9201\n"
      "10 2 127.0.0.1:9102 127.0.0.1:9202\n"
    };
    EXPECT_EQ(unordered_layout_object.identity(), expected);
    EXPECT_EQ(ordered_layout_object.identity(), expected);
  }

  TEST(ShardLayoutTest, RoutesByGoldenFnv1aVectors)
  {
    const TempLayoutFile file{reversed_sixteen_shard_layout()};
    const auto layout{ShardLayout::load(file.path())};

    struct Vector
    {
      std::string_view application;
      std::string_view key;
      std::uint32_t shard_id;
    };
    const std::vector<Vector> vectors{
      {"", "", 15},
      {"app", "key", 27},
      {"shop", "sku:42", 0},
      {std::string_view{"a\0b", 3}, std::string_view{"k\0", 2}, 12},
      {"配置", "键", 0},
      {"app", "key27", 0},
      {"app", "key1", 3},
      {"app", "key21", 6},
      {"app", "key26", 9},
      {"app", "key6", 12},
      {"app", "key20", 15},
      {"app", "key8", 18},
      {"app", "key7", 21},
      {"app", "key2", 24},
      {"app", "key9", 27},
      {"app", "key4", 30},
      {"app", "key3", 33},
      {"app", "key23", 36},
      {"app", "key5", 39},
      {"app", "key0", 42},
      {"app", "key22", 45},
    };
    for (const auto& vector : vectors)
    {
      SCOPED_TRACE(vector.shard_id);
      EXPECT_EQ(layout.route(vector.application, vector.key).id, vector.shard_id);
    }
  }

  TEST(ShardLayoutTest, RejectsMissingFileAndDirectoryAsIoErrors)
  {
    const auto missing{std::filesystem::temp_directory_path() /
        ("cipple-shard-layout-missing-" + Uuid::generate().to_string())};
    expect_error_contains<std::runtime_error>([&] { static_cast<void>(ShardLayout::load(missing)); }, "cannot open");
    expect_error_contains<std::runtime_error>(
      [&] { static_cast<void>(ShardLayout::load(std::filesystem::temp_directory_path())); }, "cannot open");
  }

  TEST(ShardLayoutTest, RejectsEmptyLayout)
  {
    for (const auto content : {std::string_view{}, std::string_view{"\n # only a comment\n\t\n"}})
    {
      const TempLayoutFile file{content};
      expect_error_contains<std::invalid_argument>([&] { static_cast<void>(ShardLayout::load(file.path())); },
                                                   "no shard members");
    }
  }

  TEST(ShardLayoutTest, RejectsInvalidRowsWithLineNumbers)
  {
    struct InvalidLayout
    {
      std::string content;
      std::string expected;
    };
    const std::vector<InvalidLayout> cases{
      {"\n# comment\n\n0 1 127.0.0.1:2101\n", ":4:"},
      {"0 1 127.0.0.1:2101 127.0.0.1:2201 extra\n", ":1:"},
      {"x 1 127.0.0.1:2101 127.0.0.1:2201\n", ":1: invalid shard id"},
      {"-1 1 127.0.0.1:2101 127.0.0.1:2201\n", ":1: invalid shard id"},
      {"4294967296 1 127.0.0.1:2101 127.0.0.1:2201\n", ":1: invalid shard id"},
      {"0 0 127.0.0.1:2101 127.0.0.1:2201\n", ":1: invalid member id"},
      {"0 -1 127.0.0.1:2101 127.0.0.1:2201\n", ":1: invalid member id"},
      {"0 2147483648 127.0.0.1:2101 127.0.0.1:2201\n", ":1: invalid member id"},
      {"0 1 127.0.0.1 127.0.0.1:2201\n", ":1:"},
      {"0 1 localhost:2101 127.0.0.1:2201\n", ":1:"},
      {"0 1 127.0.0.1:x 127.0.0.1:2201\n", ":1:"},
      {"0 1 127.0.0.1:0 127.0.0.1:2201\n", ":1:"},
      {"0 1 127.0.0.1:2101 127.0.0.1:0\n", ":1:"},
      {"0 1 127.0.0.1:65536 127.0.0.1:2201\n", ":1:"},
      {"0 1 0.0.0.0:2101 127.0.0.1:2201\n", ":1:"},
      {"0 1 224.0.0.1:2101 127.0.0.1:2201\n", ":1:"},
      {"0 1 127.0.0.1:2101 255.255.255.255:2201\n", ":1:"},
      {"0 1 240.0.0.1:2101 127.0.0.1:2201\n", ":1:"},
      {"0 1 [::1]:2101 127.0.0.1:2201\n", ":1:"},
      {"0 1 127.0.0.1:2101 127.0.0.1:2201\n"
       "1 1 127.0.0.1:3101 127.0.0.1:3201\n"
       "0 1 127.0.0.1:2102 127.0.0.1:2202\n",
       ":3: duplicate member id"},
      {"0 1 127.0.0.1:2101 127.0.0.1:2201\n"
       "0 2 127.0.0.1:2201 127.0.0.1:2202\n",
       ":2: duplicate endpoint"},
      {"0 1 127.0.0.1:2101 127.0.0.1:2101\n", ":1: duplicate endpoint"},
    };
    for (const auto& invalid : cases)
    {
      SCOPED_TRACE(invalid.expected);
      const TempLayoutFile file{invalid.content};
      expect_error_contains<std::invalid_argument>([&] { static_cast<void>(ShardLayout::load(file.path())); },
                                                   invalid.expected);
    }
  }
}
