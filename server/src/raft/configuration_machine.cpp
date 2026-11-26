#include "raft/configuration_machine.hpp"

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <bcrypt.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "binary/buffer_reader.hpp"
#include "binary/buffer_writer.hpp"
#include "entity/entity_message.hpp"
#include "protocol/configuration/delete_request.hpp"
#include "protocol/configuration/delete_response.hpp"
#include "protocol/configuration/get_request.hpp"
#include "protocol/configuration/get_response.hpp"
#include "protocol/configuration/incremental_update_request.hpp"
#include "protocol/configuration/incremental_update_response.hpp"
#include "protocol/configuration/put_request.hpp"
#include "protocol/configuration/put_response.hpp"
#include "protocol/frame_codec.hpp"
#include "protocol/message.hpp"
#include "protocol/message_decoder.hpp"
#include "storage/sqlite_storage.hpp"

namespace cipple::raft
{
  struct ConfigurationMachineSnapshot
  {
    std::filesystem::path path;
    std::uint64_t size{0};
    std::array<std::uint8_t, 32> sha256{};
    std::vector<std::uint8_t> metadata;
    nuraft::ptr<nuraft::snapshot> snapshot;
  };

  namespace
  {
    constexpr std::size_t kSnapshotChunkSize{256u << 10};
    constexpr std::size_t kMaximumSnapshotMetadata{1u << 20};
    constexpr std::array<std::uint8_t, 8> kSnapshotMagic{'C', 'I', 'P', 'P', 'S', 'N', 'P', '1'};
    constexpr std::uint32_t kSnapshotFormatVersion{1};

    using storage::SqliteStorage;

    std::filesystem::path append_suffix(std::filesystem::path path, const std::string& suffix)
    {
      path += suffix;
      return path;
    }

    std::filesystem::path snapshot_path(const std::filesystem::path& database, const std::uint64_t index)
    {
      return append_suffix(database, ".snapshot." + std::to_string(index));
    }

    std::filesystem::path snapshot_temp_path(const std::filesystem::path& database, const std::uint64_t index)
    {
      return append_suffix(database, ".snapshot." + std::to_string(index) + ".tmp");
    }

    std::filesystem::path pending_snapshot_path(const std::filesystem::path& database)
    {
      return append_suffix(database, ".snapshot.pending");
    }

    std::filesystem::path pending_snapshot_done_path(const std::filesystem::path& database)
    {
      return append_suffix(database, ".snapshot.pending.done");
    }

    class FileHandle
    {
    public:
      FileHandle(const std::filesystem::path& path, const bool write)
      {
        const auto access{write ? GENERIC_WRITE : GENERIC_READ};
        const auto creation{write ? CREATE_ALWAYS : OPEN_EXISTING};
        const DWORD share{write ? 0u : FILE_SHARE_READ | FILE_SHARE_DELETE};
        _handle = CreateFileW(path.c_str(), access, share, nullptr, creation, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (_handle == INVALID_HANDLE_VALUE)
        {
          throw std::system_error{static_cast<int>(GetLastError()), std::system_category(),
                                  write ? "create snapshot file" : "open snapshot file"};
        }
      }

      ~FileHandle() noexcept
      {
        if (_handle != INVALID_HANDLE_VALUE)
        {
          CloseHandle(_handle);
        }
      }

      FileHandle(const FileHandle&) = delete;
      FileHandle& operator=(const FileHandle&) = delete;

      void write_at(const std::uint64_t offset, const std::span<const std::uint8_t> bytes) const
      {
        seek(offset);
        std::size_t written{0};
        while (written < bytes.size())
        {
          const auto chunk{static_cast<DWORD>(std::min<std::size_t>(bytes.size() - written, 1u << 30))};
          DWORD done{0};
          if (!WriteFile(_handle, bytes.data() + written, chunk, &done, nullptr) || done != chunk)
          {
            throw std::system_error{static_cast<int>(GetLastError()), std::system_category(), "write snapshot file"};
          }
          written += done;
        }
      }

      void read_at(const std::uint64_t offset, const std::span<std::uint8_t> bytes) const
      {
        seek(offset);
        DWORD count{0};
        if (!ReadFile(_handle, bytes.data(), static_cast<DWORD>(bytes.size()), &count, nullptr) || count != bytes.size())
        {
          throw std::runtime_error{"could not read a complete snapshot chunk"};
        }
      }

      void flush() const
      {
        if (!FlushFileBuffers(_handle))
        {
          throw std::system_error{static_cast<int>(GetLastError()), std::system_category(), "flush snapshot file"};
        }
      }

      void close() noexcept
      {
        if (_handle != INVALID_HANDLE_VALUE)
        {
          CloseHandle(_handle);
          _handle = INVALID_HANDLE_VALUE;
        }
      }

    private:
      void seek(const std::uint64_t offset) const
      {
        LARGE_INTEGER position{};
        position.QuadPart = static_cast<LONGLONG>(offset);
        if (!SetFilePointerEx(_handle, position, nullptr, FILE_BEGIN))
        {
          throw std::system_error{static_cast<int>(GetLastError()), std::system_category(), "seek snapshot file"};
        }
      }

      HANDLE _handle{INVALID_HANDLE_VALUE};
    };

    struct OutgoingSnapshot
    {
      explicit OutgoingSnapshot(const ConfigurationMachineSnapshot& snapshot)
        : image{snapshot}, file{snapshot.path, false}
      {
      }

      ConfigurationMachineSnapshot image;
      FileHandle file;
    };

    void rename_replace(const std::filesystem::path& from, const std::filesystem::path& to)
    {
      if (!MoveFileExW(from.c_str(), to.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
      {
        throw std::system_error{static_cast<int>(GetLastError()), std::system_category(), "publish snapshot file"};
      }
    }

    void write_file_durable(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes)
    {
      const auto temporary{append_suffix(path, ".tmp")};
      FileHandle file{temporary, true};
      file.write_at(0, bytes);
      file.flush();
      file.close();
      rename_replace(temporary, path);
    }

    void remove_pending_files(const std::filesystem::path& database)
    {
      // A visible marker must always have a corresponding complete image.
      std::filesystem::remove(pending_snapshot_done_path(database));
      std::filesystem::remove(pending_snapshot_path(database));
    }

    std::vector<std::uint8_t> read_file(const std::filesystem::path& path)
    {
      std::ifstream input{path, std::ios::binary};
      if (!input)
      {
        throw std::runtime_error{"cannot open snapshot metadata file"};
      }
      return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
    }

    struct Sha256Context
    {
      Sha256Context()
      {
        auto status{BCryptOpenAlgorithmProvider(&algorithm, BCRYPT_SHA256_ALGORITHM, nullptr, 0)};
        if (status < 0)
        {
          throw std::system_error{static_cast<int>(status), std::system_category(), "open BCrypt SHA-256"};
        }
        DWORD object_size{0};
        DWORD copied{0};
        status = BCryptGetProperty(algorithm, BCRYPT_OBJECT_LENGTH, reinterpret_cast<PUCHAR>(&object_size),
                                   sizeof(object_size), &copied, 0);
        if (status < 0 || copied != sizeof(object_size))
        {
          throw std::system_error{static_cast<int>(status), std::system_category(), "query BCrypt hash size"};
        }
        object.resize(object_size);
        status = BCryptCreateHash(algorithm, &hash, object.data(), object_size, nullptr, 0, 0);
        if (status < 0)
        {
          throw std::system_error{static_cast<int>(status), std::system_category(), "create BCrypt hash"};
        }
      }

      ~Sha256Context() noexcept
      {
        if (hash != nullptr)
        {
          BCryptDestroyHash(hash);
        }
        if (algorithm != nullptr)
        {
          BCryptCloseAlgorithmProvider(algorithm, 0);
        }
      }

      Sha256Context(const Sha256Context&) = delete;
      Sha256Context& operator=(const Sha256Context&) = delete;

      void update(const std::span<const std::uint8_t> bytes)
      {
        if (!bytes.empty())
        {
          const auto status{
            BCryptHashData(hash, const_cast<PUCHAR>(reinterpret_cast<const UCHAR*>(bytes.data())),
                           static_cast<ULONG>(bytes.size()), 0)
          };
          if (status < 0)
          {
            throw std::system_error{static_cast<int>(status), std::system_category(), "update BCrypt hash"};
          }
        }
      }

      std::array<std::uint8_t, 32> finish()
      {
        std::array<std::uint8_t, 32> digest{};
        const auto status{BCryptFinishHash(hash, digest.data(), static_cast<ULONG>(digest.size()), 0)};
        if (status < 0)
        {
          throw std::system_error{static_cast<int>(status), std::system_category(), "finish BCrypt hash"};
        }
        return digest;
      }

      BCRYPT_ALG_HANDLE algorithm{nullptr};
      BCRYPT_HASH_HANDLE hash{nullptr};
      std::vector<UCHAR> object;
    };

    std::array<std::uint8_t, 32> sha256(const std::span<const std::uint8_t> bytes)
    {
      Sha256Context hash;
      hash.update(bytes);
      return hash.finish();
    }

    std::array<std::uint8_t, 32> sha256_file(const std::filesystem::path& path, const std::uint64_t expected_size)
    {
      std::ifstream input{path, std::ios::binary};
      if (!input)
      {
        throw std::runtime_error{"cannot open snapshot image"};
      }
      Sha256Context hash;
      std::array<char, 256u << 10> chunk{};
      std::uint64_t total{0};
      while (input)
      {
        input.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        const auto count{input.gcount()};
        if (count > 0)
        {
          total += static_cast<std::uint64_t>(count);
          hash.update({reinterpret_cast<const std::uint8_t*>(chunk.data()), static_cast<std::size_t>(count)});
        }
      }
      if (!input.eof())
      {
        throw std::runtime_error{"cannot read snapshot image"};
      }
      if (total != expected_size)
      {
        throw std::runtime_error{"snapshot image size does not match its manifest"};
      }
      return hash.finish();
    }

    struct SnapshotHeader
    {
      std::uint64_t file_size{0};
      std::array<std::uint8_t, 32> sha256{};
      std::vector<std::uint8_t> metadata;
    };

    std::vector<std::uint8_t> encode_snapshot_header(const SnapshotHeader& header)
    {
      tcp::BufferWriter writer;
      writer.reserve(8 + 4 + 8 + header.metadata.size() + 8 + 32);
      writer.write_bytes(kSnapshotMagic);
      writer.write_u32(kSnapshotFormatVersion);
      writer.write_u64(header.metadata.size());
      writer.write_bytes(header.metadata);
      writer.write_u64(header.file_size);
      writer.write_bytes(header.sha256);
      return writer.bytes();
    }

    SnapshotHeader decode_snapshot_header(const std::span<const std::uint8_t> bytes)
    {
      tcp::BufferReader reader{bytes};
      const auto magic{reader.read_fixed<8>()};
      if (!std::equal(magic.begin(), magic.end(), kSnapshotMagic.begin()) ||
        reader.read_u32() != kSnapshotFormatVersion)
      {
        throw std::invalid_argument{"unsupported configuration snapshot header"};
      }
      const auto metadata_size{reader.read_u64()};
      if (metadata_size == 0 || metadata_size > kMaximumSnapshotMetadata || metadata_size > reader.remaining())
      {
        throw std::invalid_argument{"invalid configuration snapshot metadata size"};
      }
      SnapshotHeader header;
      const auto metadata{reader.read_bytes(static_cast<std::size_t>(metadata_size))};
      header.metadata.assign(metadata.begin(), metadata.end());
      header.file_size = reader.read_u64();
      const auto digest{reader.read_fixed<32>()};
      std::copy(digest.begin(), digest.end(), header.sha256.begin());
      if (reader.remaining() != 0 || header.file_size == 0)
      {
        throw std::invalid_argument{"malformed configuration snapshot header"};
      }
      return header;
    }

    std::vector<std::uint8_t> serialize_snapshot_metadata(nuraft::snapshot& snapshot)
    {
      const auto encoded{snapshot.serialize()};
      const auto* first{encoded->data_begin()};
      return {first, first + encoded->size()};
    }

    nuraft::ptr<nuraft::snapshot> deserialize_snapshot_metadata(const std::vector<std::uint8_t>& metadata)
    {
      if (metadata.empty())
      {
        throw std::invalid_argument{"empty Raft snapshot metadata"};
      }
      auto encoded{nuraft::buffer::alloc(metadata.size())};
      encoded->put_raw(metadata.data(), metadata.size());
      encoded->pos(0);
      auto snapshot{nuraft::snapshot::deserialize(*encoded)};
      if (!snapshot || snapshot->get_type() != nuraft::snapshot::logical_object)
      {
        throw std::invalid_argument{"Raft snapshot metadata is not a logical snapshot"};
      }
      return snapshot;
    }

    void validate_metadata_matches(const nuraft::snapshot& expected, const std::vector<std::uint8_t>& metadata)
    {
      const auto decoded{deserialize_snapshot_metadata(metadata)};
      if (decoded->get_last_log_idx() != expected.get_last_log_idx() ||
        decoded->get_last_log_term() != expected.get_last_log_term())
      {
        throw std::invalid_argument{"snapshot metadata does not match the Raft envelope"};
      }
    }

    SqliteStorage::RaftSnapshotInfo snapshot_info(const ConfigurationMachineSnapshot& snapshot)
    {
      return {
        .log_index = snapshot.snapshot->get_last_log_idx(),
        .log_term = snapshot.snapshot->get_last_log_term(),
        .file_size = snapshot.size,
        .sha256 = snapshot.sha256,
        .metadata = snapshot.metadata,
      };
    }

    std::int64_t checked_log_index(const nuraft::ulong index)
    {
      if (index == 0 || index > static_cast<nuraft::ulong>(std::numeric_limits<std::int64_t>::max()))
      {
        throw std::overflow_error{"Raft log index cannot be represented in SQLite"};
      }
      return static_cast<std::int64_t>(index);
    }

    std::vector<std::uint8_t> encode_message(const message::Message& response)
    {
      tcp::BufferWriter writer;
      response.encode(writer);
      return writer.bytes();
    }

    nuraft::ptr<nuraft::buffer> nuraft_buffer(const std::vector<std::uint8_t>& bytes)
    {
      auto result{nuraft::buffer::alloc(bytes.size())};
      if (!bytes.empty())
      {
        result->put_raw(bytes.data(), bytes.size());
      }
      result->pos(0);
      return result;
    }

    void copy_snapshot_bytes(const std::filesystem::path& from, const std::filesystem::path& to)
    {
      std::ifstream input{from, std::ios::binary};
      if (!input)
      {
        throw std::runtime_error{"cannot open pending snapshot image"};
      }
      FileHandle output{to, true};
      std::array<char, 256u << 10> chunk{};
      std::uint64_t offset{0};
      while (input)
      {
        input.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        const auto count{input.gcount()};
        if (count > 0)
        {
          output.write_at(offset, {reinterpret_cast<const std::uint8_t*>(chunk.data()), static_cast<std::size_t>(count)});
          offset += static_cast<std::uint64_t>(count);
        }
      }
      if (!input.eof())
      {
        throw std::runtime_error{"cannot read pending snapshot image"};
      }
      output.flush();
      output.close();
    }
  }

  struct ConfigurationMachine::IncomingSnapshot
  {
    ConfigurationMachineSnapshot image;
    std::vector<std::uint8_t> header_bytes;
    std::unique_ptr<FileHandle> output;
    std::uint64_t next_object{1};
    bool complete{false};
  };

  ConfigurationMachine::ConfigurationMachine(const std::filesystem::path& database, const std::int32_t node_id,
                                             std::function<void()> on_applied)
    : _database{database}
      , _node_id{node_id}
      , _on_applied{std::move(on_applied)}
  {
    if (node_id <= 0)
    {
      throw std::invalid_argument{"Raft state machine requires a positive node id"};
    }
    _writer = std::make_unique<SqliteStorage>(database, SqliteStorage::OpenOptions{.raft = true});
    recover_pending_snapshot();
    load_durable_snapshot();
    _reader = std::make_unique<SqliteStorage>(database, SqliteStorage::OpenOptions{.raft = true});
    _last_commit_index.store(static_cast<std::uint64_t>(_writer->raft_applied_index()));
    cleanup_snapshot_files();
  }

  ConfigurationMachine::~ConfigurationMachine() noexcept = default;

  storage::SqliteStorage& ConfigurationMachine::reader() const noexcept
  {
    return *_reader;
  }

  void ConfigurationMachine::fail() noexcept
  {
    _failed.store(true);
  }

  void ConfigurationMachine::ensure_open() const
  {
    if (_failed.load())
    {
      throw std::runtime_error{"configuration state machine is closed after a persistence failure"};
    }
  }

  void ConfigurationMachine::notify_applied() noexcept
  {
    if (!_on_applied)
    {
      return;
    }
    try
    {
      _on_applied();
    }
    catch (const std::exception& error)
    {
      std::cerr << "Configuration apply notification failed: " << error.what() << '\n';
    }
    catch (...)
    {
    }
  }

  nuraft::ptr<nuraft::buffer> ConfigurationMachine::commit(const nuraft::ulong log_idx, nuraft::buffer& data)
  {
    const std::lock_guard lock{_apply_mutex};
    try
    {
    ensure_open();
    const auto* first{data.data_begin()};
    const std::span<const std::uint8_t> command{first, data.size()};
    const auto command_hash{sha256(command)};
    auto decoded{tcp::decode_message(command)};
    const auto sqlite_index{checked_log_index(log_idx)};

    std::vector<std::uint8_t> response;
    bool data_changed{false};
    if (const auto* request{dynamic_cast<const message::PutRequest*>(decoded.get())})
    {
      const entity::EntityMessage version{
        request->uuid(), request->application_name(), request->key(), sqlite_index, 0,
        entity::EntityMessage::Update{request->value()}
      };
      const auto applied{_writer->apply_raft_mutation(version, command_hash, sqlite_index)};
      data_changed = applied.data_changed;
      response = encode_message(message::PutResponse{request->uuid(), applied.success});
    }
    else if (const auto* request{dynamic_cast<const message::DeleteRequest*>(decoded.get())})
    {
      const entity::EntityMessage version{
        request->uuid(), request->application_name(), request->key(), sqlite_index, 0,
        entity::EntityMessage::Delete{}
      };
      const auto applied{_writer->apply_raft_mutation(version, command_hash, sqlite_index)};
      data_changed = applied.data_changed;
      response = encode_message(message::DeleteResponse{request->uuid(), applied.success});
    }
    else if (const auto* request{dynamic_cast<const message::IncrementalUpdateRequest*>(decoded.get())})
    {
      const entity::EntityMessage version{
        request->uuid(), request->application_name(), request->key(), sqlite_index, 0,
        entity::EntityMessage::IncrementalUpdate{
          request->base_message_uuid(), request->atomic_operation(), request->value()
        }
      };
      const auto applied{_writer->apply_raft_mutation(version, command_hash, sqlite_index)};
      data_changed = applied.data_changed;
      response = encode_message(message::IncrementalUpdateResponse{request->uuid(), applied.success});
    }
    else if (const auto* request{dynamic_cast<const message::GetRequest*>(decoded.get())})
    {
      const auto result{_writer->apply_raft_get(request->application_name(), request->key(), sqlite_index)};
      response = encode_message(message::GetResponse{
        request->uuid(), request->application_name(), request->key(), result.value, std::move(result.versions)
      });
      if (response.size() > tcp::kDefaultMaxFrameLength)
      {
        throw std::runtime_error{"linearized GET response exceeds the application frame limit"};
      }
    }
    else
    {
      throw std::invalid_argument{"unsupported Raft application command"};
    }

    _last_commit_index.store(log_idx);
    if (data_changed)
    {
      notify_applied();
    }
    return nuraft_buffer(response);
    }
    catch (...)
    {
      fail();
      throw;
    }
  }

  nuraft::ptr<nuraft::buffer> ConfigurationMachine::pre_commit(const nuraft::ulong, nuraft::buffer&)
  {
    return nullptr;
  }

  void ConfigurationMachine::rollback(const nuraft::ulong, nuraft::buffer&)
  {
  }

  void ConfigurationMachine::commit_config(const nuraft::ulong log_idx, nuraft::ptr<nuraft::cluster_config>&)
  {
    const std::lock_guard lock{_apply_mutex};
    try
    {
      ensure_open();
      _writer->apply_raft_progress(checked_log_index(log_idx));
      _last_commit_index.store(log_idx);
    }
    catch (...)
    {
      fail();
      throw;
    }
  }

  void ConfigurationMachine::save_logical_snp_obj(nuraft::snapshot& s, nuraft::ulong& obj_id,
                                                  nuraft::buffer& data, const bool is_first_obj,
                                                  const bool is_last_obj)
  {
    try
    {
    ensure_open();
    const std::span<const std::uint8_t> bytes{data.data_begin(), data.size()};
    const std::lock_guard lock{_snapshot_mutex};
    if (obj_id == 0)
    {
      if (!is_first_obj)
      {
        throw std::invalid_argument{"snapshot header was not marked as the first object"};
      }
      auto header{decode_snapshot_header(bytes)};
      validate_metadata_matches(s, header.metadata);
      discard_incoming();

      auto incoming{std::make_unique<IncomingSnapshot>()};
      incoming->image.path = pending_snapshot_path(_database);
      incoming->image.size = header.file_size;
      incoming->image.sha256 = header.sha256;
      incoming->image.metadata = std::move(header.metadata);
      incoming->image.snapshot = deserialize_snapshot_metadata(incoming->image.metadata);
      incoming->header_bytes.assign(bytes.begin(), bytes.end());
      incoming->output = std::make_unique<FileHandle>(incoming->image.path, true);
      _incoming = std::move(incoming);
      obj_id = 1;
      return;
    }

    if (!_incoming || _incoming->image.snapshot->get_last_log_idx() != s.get_last_log_idx() ||
      _incoming->image.snapshot->get_last_log_term() != s.get_last_log_term() || _incoming->complete)
    {
      throw std::invalid_argument{"snapshot chunk has no matching pending transfer"};
    }
    auto& incoming{*_incoming};
    const auto chunk_count{(incoming.image.size + kSnapshotChunkSize - 1) / kSnapshotChunkSize};
    if (obj_id == 0 || obj_id > chunk_count)
    {
      throw std::invalid_argument{"snapshot chunk object id is out of range"};
    }
    const auto chunk_index{obj_id - 1};
    const auto offset{chunk_index * kSnapshotChunkSize};
    const auto expected{static_cast<std::size_t>(std::min<std::uint64_t>(kSnapshotChunkSize, incoming.image.size - offset))};
    if (bytes.size() != expected)
    {
      throw std::invalid_argument{"snapshot chunk size does not match its manifest"};
    }
    incoming.output->write_at(offset, bytes);
    incoming.next_object = std::max(incoming.next_object, obj_id + 1);

    const bool final_chunk{chunk_index + 1 == chunk_count};
    if (final_chunk != is_last_obj)
    {
      throw std::invalid_argument{"snapshot final-object flag does not match its manifest"};
    }
    if (final_chunk)
    {
      incoming.output->flush();
      incoming.output->close();
      if (sha256_file(incoming.image.path, incoming.image.size) != incoming.image.sha256)
      {
        throw std::runtime_error{"received snapshot image digest mismatch"};
      }
      // This durable completion marker is the recovery boundary. NuRaft compacts
      // the log before apply_snapshot(), so after this point a restart must be
      // able to finish installation without any log prefix.
      write_file_durable(pending_snapshot_done_path(_database), incoming.header_bytes);
      incoming.complete = true;
    }
    obj_id = incoming.next_object;
    }
    catch (...)
    {
      fail();
      throw;
    }
  }

  bool ConfigurationMachine::apply_snapshot(nuraft::snapshot& s)
  {
    const std::lock_guard lock{_apply_mutex};
    try
    {
      ensure_open();
      std::unique_ptr<IncomingSnapshot> incoming;
      {
        const std::lock_guard snapshot_lock{_snapshot_mutex};
        if (!_incoming || !_incoming->complete ||
          _incoming->image.snapshot->get_last_log_idx() != s.get_last_log_idx() ||
          _incoming->image.snapshot->get_last_log_term() != s.get_last_log_term())
        {
          throw std::runtime_error{"complete pending snapshot is missing at apply time"};
        }
        incoming = std::move(_incoming);
      }
      install_recovered_snapshot(incoming->image);
      remove_pending_files(_database);
      _last_commit_index.store(static_cast<std::uint64_t>(_writer->raft_applied_index()));
      notify_applied();
      return true;
    }
    catch (const std::exception& error)
    {
      std::cerr << "Applying configuration snapshot failed: " << error.what() << '\n';
      fail();
      return false;
    }
  }

  int ConfigurationMachine::read_logical_snp_obj(nuraft::snapshot& s, void*& user_snp_ctx, const nuraft::ulong obj_id,
                                                 nuraft::ptr<nuraft::buffer>& data_out, bool& is_last_obj)
  {
    try
    {
      if (!user_snp_ctx)
      {
        const std::lock_guard lock{_snapshot_mutex};
        const auto entry{_snapshots.find(s.get_last_log_idx())};
        if (entry == _snapshots.end())
        {
          throw std::runtime_error{"requested snapshot is no longer available"};
        }
        user_snp_ctx = new OutgoingSnapshot{entry->second};
      }
      const auto& outgoing{*static_cast<const OutgoingSnapshot*>(user_snp_ctx)};
      const auto& snapshot{outgoing.image};
      const auto chunk_count{(snapshot.size + kSnapshotChunkSize - 1) / kSnapshotChunkSize};
      if (obj_id == 0)
      {
        const SnapshotHeader header{snapshot.size, snapshot.sha256, snapshot.metadata};
        const auto encoded{encode_snapshot_header(header)};
        data_out = nuraft_buffer(encoded);
        is_last_obj = false;
        return 0;
      }
      if (obj_id > chunk_count)
      {
        data_out = nullptr;
        is_last_obj = true;
        return -1;
      }
      const auto chunk_index{obj_id - 1};
      const auto offset{chunk_index * kSnapshotChunkSize};
      const auto amount{static_cast<std::size_t>(std::min<std::uint64_t>(kSnapshotChunkSize, snapshot.size - offset))};
      std::vector<std::uint8_t> bytes(amount);
      outgoing.file.read_at(offset, bytes);
      data_out = nuraft_buffer(bytes);
      is_last_obj = chunk_index + 1 == chunk_count;
      return 0;
    }
    catch (...)
    {
      data_out = nullptr;
      is_last_obj = true;
      return -1;
    }
  }

  void ConfigurationMachine::free_user_snp_ctx(void*& user_snp_ctx)
  {
    delete static_cast<OutgoingSnapshot*>(user_snp_ctx);
    user_snp_ctx = nullptr;
  }

  nuraft::ptr<nuraft::snapshot> ConfigurationMachine::last_snapshot()
  {
    const std::lock_guard lock{_snapshot_mutex};
    if (_snapshots.empty())
    {
      return nullptr;
    }
    return _snapshots.rbegin()->second.snapshot;
  }

  nuraft::ulong ConfigurationMachine::last_commit_index()
  {
    return _last_commit_index.load();
  }

  void ConfigurationMachine::create_snapshot(nuraft::snapshot& s,
                                             nuraft::async_result<bool>::handler_type& when_done)
  {
    bool succeeded{false};
    nuraft::ptr<std::exception> error;
    try
    {
      const std::lock_guard lock{_apply_mutex};
      ensure_open();
      if (s.get_last_log_idx() != _last_commit_index.load() || s.get_last_log_idx() == 0)
      {
        when_done(succeeded, error);
        return;
      }

      const auto index{s.get_last_log_idx()};
      const auto temporary{snapshot_temp_path(_database, index)};
      const auto final{snapshot_path(_database, index)};
      std::error_code ignored;
      std::filesystem::remove(temporary, ignored);
      _writer->backup_database(temporary);
      const auto size{std::filesystem::file_size(temporary)};
      const auto digest{sha256_file(temporary, size)};
      rename_replace(temporary, final);

      ConfigurationMachineSnapshot snapshot;
      snapshot.path = final;
      snapshot.size = size;
      snapshot.sha256 = digest;
      snapshot.metadata = serialize_snapshot_metadata(s);
      snapshot.snapshot = deserialize_snapshot_metadata(snapshot.metadata);
      if (snapshot.snapshot->get_last_log_idx() != index || snapshot.snapshot->get_last_log_term() != s.get_last_log_term())
      {
        throw std::runtime_error{"serialized Raft snapshot metadata changed while snapshotting"};
      }
      _writer->record_raft_snapshot(snapshot_info(snapshot));
      {
        const std::lock_guard snapshot_lock{_snapshot_mutex};
        _snapshots[index] = snapshot;
        for (auto it{_snapshots.begin()}; it != _snapshots.end() && _snapshots.size() > 1;)
        {
          it = _snapshots.erase(it);
        }
      }
      cleanup_snapshot_files();
      succeeded = true;
    }
    catch (const std::exception& failure)
    {
      fail();
      error = nuraft::cs_new<std::runtime_error>(failure.what());
    }
    when_done(succeeded, error);
  }

  bool ConfigurationMachine::chk_create_snapshot()
  {
    return !_failed.load();
  }

  bool ConfigurationMachine::allow_leadership_transfer()
  {
    return !_failed.load();
  }

  void ConfigurationMachine::discard_incoming()
  {
    _incoming.reset();
    remove_pending_files(_database);
  }

  void ConfigurationMachine::install_recovered_snapshot(const ConfigurationMachineSnapshot& snapshot)
  {
    const auto info{snapshot_info(snapshot)};
    const auto index{info.log_index};
    const auto applied{static_cast<std::uint64_t>(_writer->raft_applied_index())};
    if (applied < index)
    {
      _writer->restore_raft_snapshot(snapshot.path, info);
    }
    else if (applied == index)
    {
      const auto current{_writer->raft_snapshot_info()};
      if (!current || current->log_index < index)
      {
        _writer->record_raft_snapshot(info);
      }
      else if (current->log_index == index && (current->sha256 != info.sha256 || current->metadata != info.metadata))
      {
        throw std::runtime_error{"durable Raft snapshot metadata conflicts with the pending image"};
      }
    }
    else
    {
      throw std::runtime_error{"cannot install a snapshot older than the applied state"};
    }

    const auto final{snapshot_path(_database, index)};
    const auto temporary{snapshot_temp_path(_database, index)};
    bool valid_final{false};
    std::error_code error;
    if (std::filesystem::exists(final, error) && std::filesystem::file_size(final) == info.file_size)
    {
      valid_final = sha256_file(final, info.file_size) == info.sha256;
    }
    if (!valid_final)
    {
      std::filesystem::remove(temporary, error);
      copy_snapshot_bytes(snapshot.path, temporary);
      rename_replace(temporary, final);
      if (sha256_file(final, info.file_size) != info.sha256)
      {
        throw std::runtime_error{"published snapshot image digest mismatch"};
      }
    }

    ConfigurationMachineSnapshot durable{snapshot};
    durable.path = final;
    const std::lock_guard lock{_snapshot_mutex};
    _snapshots[index] = durable;
    for (auto it{_snapshots.begin()}; it != _snapshots.end() && _snapshots.size() > 1;)
    {
      it = _snapshots.erase(it);
    }
  }

  void ConfigurationMachine::recover_pending_snapshot()
  {
    const auto done_path{pending_snapshot_done_path(_database)};
    const auto image_path{pending_snapshot_path(_database)};
    std::error_code error;
    if (!std::filesystem::exists(done_path, error))
    {
      std::filesystem::remove(image_path, error);
      return;
    }

    const auto header{decode_snapshot_header(read_file(done_path))};
    ConfigurationMachineSnapshot pending;
    pending.path = image_path;
    pending.size = header.file_size;
    pending.sha256 = header.sha256;
    pending.metadata = header.metadata;
    pending.snapshot = deserialize_snapshot_metadata(pending.metadata);
    if (!std::filesystem::exists(image_path, error) || sha256_file(image_path, pending.size) != pending.sha256)
    {
      throw std::runtime_error{"complete pending Raft snapshot is missing or corrupt; refusing to open state machine"};
    }

    const auto index{pending.snapshot->get_last_log_idx()};
    const auto applied{static_cast<std::uint64_t>(_writer->raft_applied_index())};
    const auto current{_writer->raft_snapshot_info()};
    if (applied > index || (current && current->log_index > index))
    {
      remove_pending_files(_database);
      return;
    }
    install_recovered_snapshot(pending);
    remove_pending_files(_database);
  }

  void ConfigurationMachine::load_durable_snapshot()
  {
    const auto info{_writer->raft_snapshot_info()};
    if (!info)
    {
      return;
    }
    const auto path{snapshot_path(_database, info->log_index)};
    std::error_code error;
    if (!std::filesystem::exists(path, error) || sha256_file(path, info->file_size) != info->sha256)
    {
      throw std::runtime_error{"durable Raft snapshot image is missing or corrupt"};
    }
    ConfigurationMachineSnapshot snapshot;
    snapshot.path = path;
    snapshot.size = info->file_size;
    snapshot.sha256 = info->sha256;
    snapshot.metadata = info->metadata;
    snapshot.snapshot = deserialize_snapshot_metadata(snapshot.metadata);
    if (snapshot.snapshot->get_last_log_idx() != info->log_index || snapshot.snapshot->get_last_log_term() != info->log_term)
    {
      throw std::runtime_error{"durable Raft snapshot metadata does not match storage state"};
    }
    const std::lock_guard lock{_snapshot_mutex};
    _snapshots[info->log_index] = std::move(snapshot);
  }

  void ConfigurationMachine::cleanup_snapshot_files() const noexcept
  {
    std::error_code error;
    const auto parent{_database.parent_path()};
    if (parent.empty() || !std::filesystem::exists(parent, error))
    {
      return;
    }
    const auto prefix{_database.filename().wstring() + L".snapshot."};
    const auto pending{pending_snapshot_path(_database).filename().wstring()};
    const auto pending_done{pending_snapshot_done_path(_database).filename().wstring()};
    std::vector<std::wstring> keep;
    {
      const std::lock_guard lock{_snapshot_mutex};
      for (const auto& [index, snapshot] : _snapshots)
      {
        keep.push_back(snapshot_path(_database, index).filename().wstring());
      }
    }
    for (const auto& entry : std::filesystem::directory_iterator{parent, error})
    {
      if (error || !entry.is_regular_file(error))
      {
        continue;
      }
      const auto name{entry.path().filename().wstring()};
      if (!name.starts_with(prefix) || name == pending || name == pending_done ||
        std::ranges::find(keep, name) != keep.end())
      {
        continue;
      }
      std::filesystem::remove(entry.path(), error);
    }
  }
}
