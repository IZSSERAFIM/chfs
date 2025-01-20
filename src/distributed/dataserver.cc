#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  auto num_of_version_block = (KDefaultBlockCnt * sizeof(version_t)) / bm->block_size();
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, num_of_version_block, false);
  } else {
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, num_of_version_block, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCKSIZE = block_allocator_->bm->block_size();
  std::vector<u8> buffer(0);
  if (block_id >= block_allocator_->bm->total_blocks() || offset + len > BLOCKSIZE) {
    return buffer;
  }
  std::vector<u8> block_buffer(BLOCKSIZE);
  auto res = block_allocator_->bm->read_block(block_id, block_buffer.data());
  if (res.is_err()) {
    return buffer;
  }

  const auto VERSIONPERBLOCK = BLOCKSIZE / sizeof(version_t);
  auto version_block_id = block_id / VERSIONPERBLOCK;
  auto version_offset = block_id % VERSIONPERBLOCK;
  std::vector<u8> version_buffer(BLOCKSIZE);
  res = block_allocator_->bm->read_block(version_block_id, version_buffer.data());
  if (res.is_err()) {
    return buffer;
  }
  auto local_version = *reinterpret_cast<version_t *>(version_buffer.data() + version_offset * sizeof(version_t));
  if (local_version != version) {
    return buffer;
  }

  buffer.resize(len);
  memcpy(buffer.data(), block_buffer.data() + offset, len);

  return buffer;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCKSIZE = block_allocator_->bm->block_size();
  if (block_id >= block_allocator_->bm->total_blocks() || offset + buffer.size() > BLOCKSIZE) {
    return false;
  }
  auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
  if (res.is_err()) {
    return false;
  }

  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCKSIZE = block_allocator_->bm->block_size();
  auto allocate_res = block_allocator_->allocate();
  if (allocate_res.is_err()) {
    return std::pair<block_id_t, version_t>(0,0);
  }
  auto block_id = allocate_res.unwrap();
  const auto VERSIONPERBLOCK = BLOCKSIZE / sizeof(version_t);
  auto version_block_id = block_id / VERSIONPERBLOCK;
  auto version_offset = block_id % VERSIONPERBLOCK;
  std::vector<u8> version_buffer(BLOCKSIZE);
  auto res = block_allocator_->bm->read_block(version_block_id, version_buffer.data());
  if (res.is_err()) {
    return std::pair<block_id_t, version_t>(0,0);
  }
  auto old_version = *reinterpret_cast<version_t *>(version_buffer.data() + version_offset * sizeof(version_t));
  auto new_version = old_version + 1;
  memcpy(version_buffer.data() + version_offset * sizeof(version_t), &new_version, sizeof(version_t));
  res = block_allocator_->bm->write_block(version_block_id, version_buffer.data());
  if (res.is_err()) {
    return std::pair<block_id_t, version_t>(block_id, old_version);
  }

  return std::pair<block_id_t, version_t>(block_id, new_version);
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCKSIZE = block_allocator_->bm->block_size();
  auto res = block_allocator_->deallocate(block_id);
  if (res.is_err()) {
    return false;
  }
  const auto VERSIONPERBLOCK = BLOCKSIZE / sizeof(version_t);
  auto version_block_id = block_id / VERSIONPERBLOCK;
  auto version_offset = block_id % VERSIONPERBLOCK;
  std::vector<u8> version_buffer(BLOCKSIZE);
  res = block_allocator_->bm->read_block(version_block_id, version_buffer.data());
  if (res.is_err()) {
    return false;
  }
  auto old_version = *reinterpret_cast<version_t *>(version_buffer.data() + version_offset * sizeof(version_t));
  auto new_version = old_version + 1;
  memcpy(version_buffer.data() + version_offset * sizeof(version_t), &new_version, sizeof(version_t));
  res = block_allocator_->bm->write_block(version_block_id, version_buffer.data());
  if (res.is_err()) {
    return false;
  }

  return true;
}
} // namespace chfs