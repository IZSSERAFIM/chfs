#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(global_mtx);
  if (is_log_enabled_) {
    std::vector<std::shared_ptr<BlockOperation>> log_block_operations;
    const auto block_size = operation_->block_manager_->block_size();
    std::vector<u8> bitmap_data_buffer(block_size);
    block_id_t allocated_block_id = 0;
    inode_id_t new_inode_id = 0;
    // Allocate a block for the new node
    for (uint bitmap_block_index = 0; bitmap_block_index < operation_->block_allocator_->total_bitmap_block(); bitmap_block_index++) {
      operation_->block_manager_->read_block(bitmap_block_index + operation_->block_allocator_->get_bitmap_id(), bitmap_data_buffer.data());
      auto bitmap = Bitmap(bitmap_data_buffer.data(), block_size);
      std::optional<block_id_t> free_block_result = std::nullopt;
      if (bitmap_block_index == operation_->block_allocator_->total_bitmap_block() - 1) {
        free_block_result = bitmap.find_first_free_w_bound(operation_->block_allocator_->get_last_num());
      } else {
        free_block_result = bitmap.find_first_free();
      }
      if (free_block_result) {
        bitmap.set(free_block_result.value());
        const auto total_bits_per_block = block_size * KBitsPerByte;
        allocated_block_id = bitmap_block_index * total_bits_per_block + free_block_result.value();
        std::cout << "inode block id: " << allocated_block_id << std::endl;
        break;
      }
    }
    // Allocate a new inode
    auto inode_iterator_result = BlockIterator::create(operation_->block_manager_.get(), 1 + operation_->inode_manager_->get_table_blocks(), operation_->inode_manager_->get_reserved_blocks());
    inode_id_t inode_block_count = 0;
    for (auto iter = inode_iterator_result.unwrap(); iter.has_next(); iter.next(block_size).unwrap(), inode_block_count++) {
      auto inode_bitmap_data = iter.unsafe_get_value_ptr<u8>();
      auto inode_bitmap = Bitmap(inode_bitmap_data, block_size);
      auto free_inode_index = inode_bitmap.find_first_free();
      if (free_inode_index) {
        inode_bitmap.set(free_inode_index.value());
        const auto total_bits_per_block = block_size * KBitsPerByte;
        inode_id_t raw_inode_id = inode_block_count * total_bits_per_block + free_inode_index.value();
        std::vector<u8> inode_table_buffer(block_size);
        const auto inode_per_block = block_size / sizeof(block_id_t);
        block_id_t inode_table_block_id = raw_inode_id / inode_per_block + 1;
        usize inode_index = raw_inode_id % inode_per_block;
        operation_->block_manager_->read_block(inode_table_block_id, inode_table_buffer.data());
        block_id_t *inode_entries = reinterpret_cast<block_id_t *>(inode_table_buffer.data());
        inode_entries[inode_index] = allocated_block_id;
        log_block_operations.push_back(std::make_shared<BlockOperation>(inode_table_block_id, inode_table_buffer));
        new_inode_id = raw_inode_id + 1;
        std::cout << "new inode id: " << new_inode_id << std::endl;
        break;
      }
    }
    // Create new inode
    std::vector<u8> inode_data_buffer(block_size);
    auto new_inode = Inode(type == RegularFileType ? InodeType::Regular : InodeType::Directory, block_size);
    new_inode.flush_to_buffer(inode_data_buffer.data());
    log_block_operations.push_back(std::make_shared<BlockOperation>(allocated_block_id, inode_data_buffer));
    // Update parent directory
    std::list<DirectoryEntry> parent_directory_entries;
    read_directory(operation_.get(), parent, parent_directory_entries);
    std::string directory_content = dir_list_to_string(parent_directory_entries);
    directory_content = append_to_directory(directory_content, name, new_inode_id);
    std::cout << "data is " << directory_content << std::endl;
    // Read and update parent inode
    std::vector<u8> parent_block_data(block_size);
    block_id_t parent_block_id = operation_->inode_manager_->get(parent).unwrap();
    operation_->block_manager_->read_block(parent_block_id, parent_block_data.data());
    Inode *parent_inode_ptr = reinterpret_cast<Inode *>(parent_block_data.data());
    usize old_block_count = 0;
    usize new_block_count = 0;
    u64 original_file_size = 0;
    original_file_size = parent_inode_ptr->get_size();
    old_block_count = (original_file_size % block_size) ? (original_file_size / block_size + 1) : (original_file_size / block_size);
    new_block_count = (directory_content.size() % block_size) ? (directory_content.size() / block_size + 1) : (directory_content.size() / block_size);
    // Allocate additional blocks for parent directory if needed
    if (new_block_count > old_block_count) {
      for (usize block_idx = old_block_count; block_idx < new_block_count; ++block_idx) {
        for (uint bitmap_block_index = 0; bitmap_block_index < operation_->block_allocator_->total_bitmap_block(); bitmap_block_index++) {
          operation_->block_manager_->read_block(bitmap_block_index + operation_->block_allocator_->get_bitmap_id(), bitmap_data_buffer.data());
          auto bitmap = Bitmap(bitmap_data_buffer.data(), block_size);
          std::optional<block_id_t> free_block_result = std::nullopt;
          if (bitmap_block_index == operation_->block_allocator_->total_bitmap_block() - 1) {
            free_block_result = bitmap.find_first_free_w_bound(operation_->block_allocator_->get_last_num());
          } else {
            free_block_result = bitmap.find_first_free();
          }
          if (free_block_result) {
            bitmap.set(free_block_result.value());
            const auto total_bits_per_block = block_size * KBitsPerByte;
            block_id_t parent_data_block = bitmap_block_index * total_bits_per_block + free_block_result.value();
            std::cout << "new parent inode data block id: " << parent_data_block << std::endl;
            parent_inode_ptr->set_block_direct(block_idx, parent_data_block);
            break;
          }
        }
      }
    }
    log_block_operations.push_back(std::make_shared<BlockOperation>(parent_block_id, parent_block_data));
    // Write directory content to blocks
    auto current_block_idx = 0;
    u64 written_size = 0;
    while (written_size < directory_content.size()) {
      auto write_size = ((directory_content.size() - written_size) > block_size) ? block_size : (directory_content.size() - written_size);
      std::vector<u8> block_write_buffer(block_size);
      memcpy(block_write_buffer.data(), directory_content.data() + written_size, write_size);
      block_id_t write_block_id = 0;
      if (parent_inode_ptr->is_direct_block(current_block_idx)) {
        write_block_id = parent_inode_ptr->blocks[current_block_idx];
        std::cout << "write to data block: " << write_block_id << std::endl;
      }
      log_block_operations.push_back(std::make_shared<BlockOperation>(write_block_id, block_write_buffer));
      written_size += write_size;
      current_block_idx += 1;
    }
    // Commit log operations
    commit_log->append_log(commit_log->get_log_entry_num() + 1, log_block_operations);
    commit_log->commit_log(commit_log->get_log_entry_num() + 1);
    auto log_entry_count = commit_log->get_log_entry_num();
    if (log_entry_count >= kMaxLogSize) {
      commit_log->checkpoint();
    }
  }
  // Final node creation
  auto result = operation_->mk_helper(parent, name.data(), type == RegularFileType ? InodeType::Regular : InodeType::Directory);
  if (result.is_ok() && !may_failed_) {
    return result.unwrap();
  }
  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(global_mtx);
  if (is_log_enabled_) {
    std::vector<std::shared_ptr<BlockOperation>> block_operations;
    std::string content; // This seems unused, can be removed if not needed.
    std::list<DirectoryEntry> directory_entries; // This seems unused, can be removed if not needed.
    // Look up inode ID for the given parent and file name
    auto inode_id_result = operation_->lookup(parent, name.data());
    inode_id_t target_inode_id = inode_id_result.unwrap();
    // Get the block ID of the inode
    auto block_id_result = operation_->inode_manager_->get(target_inode_id);
    if (block_id_result.is_ok()) {
      block_id_t inode_block_id = block_id_result.unwrap();
      inode_id_t raw_inode_id = target_inode_id - 1;
      const auto block_size = operation_->block_manager_->block_size();
      std::vector<u8> inode_table_buffer(block_size);
      std::vector<u8> inode_bitmap_buffer(block_size);
      const auto inode_per_block = block_size / sizeof(block_id_t);
      // Determine the block ID for the inode table
      block_id_t inode_table_block_id = raw_inode_id / inode_per_block + 1;
      usize inode_index = raw_inode_id % inode_per_block;
      // Read the inode table block
      operation_->block_manager_->read_block(inode_table_block_id, inode_table_buffer.data());
      block_id_t *inode_entries = reinterpret_cast<block_id_t *>(inode_table_buffer.data());
      inode_entries[inode_index] = 0; // Unlink the inode by setting it to 0
      block_operations.push_back(std::make_shared<BlockOperation>(inode_table_block_id, inode_table_buffer));
      // Get the block ID for the inode bitmap and clear the corresponding bit
      const auto inode_bits_per_block = block_size * KBitsPerByte;
      block_id_t inode_bit_block_id = raw_inode_id / inode_bits_per_block + operation_->inode_manager_->get_table_blocks() + 1;
      usize bitmap_index = raw_inode_id % inode_bits_per_block;
      operation_->block_manager_->read_block(inode_bit_block_id, inode_bitmap_buffer.data());
      auto inode_bitmap = Bitmap(inode_bitmap_buffer.data(), block_size);
      inode_bitmap.clear(bitmap_index);
      block_operations.push_back(std::make_shared<BlockOperation>(inode_bit_block_id, inode_bitmap_buffer));
      // Update the data block bitmap (if necessary)
      const auto total_bits_per_block = block_size * KBitsPerByte;
      std::vector<u8> data_block_bitmap_buffer(block_size);
      usize data_block_bitmap_id = inode_block_id / total_bits_per_block;
      usize data_block_bitmap_index = inode_block_id % total_bits_per_block;
      operation_->block_manager_->read_block(data_block_bitmap_id + operation_->block_allocator_->get_bitmap_id(), data_block_bitmap_buffer.data());
      auto data_block_bitmap = Bitmap(data_block_bitmap_buffer.data(), block_size);
      // If the block is in use, clear the bit in the data block bitmap
      if (data_block_bitmap.check(data_block_bitmap_index)) {
        data_block_bitmap.clear(data_block_bitmap_index);
        block_operations.push_back(std::make_shared<BlockOperation>(data_block_bitmap_id + operation_->block_allocator_->get_bitmap_id(), data_block_bitmap_buffer));
      }
    }
    // Append log operations for the commit log
    commit_log->append_log(commit_log->get_log_entry_num() + 1, block_operations);
    commit_log->commit_log(commit_log->get_log_entry_num() + 1);
    // Checkpoint if log size exceeds maximum limit
    auto current_log_size = commit_log->get_log_entry_num();
    if (current_log_size >= kMaxLogSize) {
      commit_log->checkpoint();
    }
  }
  // Perform the unlink operation
  auto unlink_result = operation_->unlink(parent, name.data());
  if (unlink_result.is_ok() && !may_failed_) {
    return true;
  }
  return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(global_mtx);
  auto res = operation_->lookup(parent, name.data());
  if(res.is_err()){
    return KInvalidInodeID;
  }
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(global_mtx);
  std::vector<BlockInfo> res;
  const auto BlockSize = operation_->block_manager_->block_size();
  std::vector<u8> buffer(BlockSize);
  auto block_id_res = operation_->inode_manager_->get(id);
  if(block_id_res.is_ok()){
    block_id_t block_id = block_id_res.unwrap();
    operation_->block_manager_->read_block(block_id,buffer.data());
    Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
    for(uint i = 0; i < inode_p->get_ntuples(); i++){
      if(std::get<0>(inode_p->block_infos[i]) == 0){
        continue;
      }
      block_id_t bid = std::get<0>(inode_p->block_infos[i]);
      mac_id_t mid = std::get<1>(inode_p->block_infos[i]);
      version_t vid = std::get<2>(inode_p->block_infos[i]);
      res.push_back(std::make_tuple(bid,mid,vid));
    }
    return res;
  }
  return res;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(global_mtx);
  mac_id_t mac_id = generator.rand(1,num_data_servers);
  auto res = clients_[mac_id]->call("alloc_block");
  block_id_t block_id = 0;
  version_t version = 0;
  if(res.is_ok()){
    auto [block_id, version] = res.unwrap()->as<std::pair<block_id_t, version_t>>();
    const auto BlockSize = operation_->block_manager_->block_size();
    std::vector<u8> buffer(BlockSize);
    block_id_t inode_block_id = operation_->inode_manager_->get(id).unwrap();
    operation_->block_manager_->read_block(inode_block_id,buffer.data());
    Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
    for(uint i = 0; i < inode_p->get_ntuples(); i++){
      if(std::get<0>(inode_p->block_infos[i]) == 0){
        std::get<0>(inode_p->block_infos[i]) = block_id;
        std::get<1>(inode_p->block_infos[i]) = mac_id;
        std::get<2>(inode_p->block_infos[i]) = version;
        inode_p->inner_attr.size += DiskBlockSize;
        break;
      }
    }
    operation_->block_manager_->write_block(inode_block_id,buffer.data());
    return std::make_tuple(block_id,mac_id,version);
  }
  return std::make_tuple(block_id,mac_id,version);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(global_mtx);
  auto res = clients_[machine_id]->call("free_block",block_id);
  if(res.is_ok() && res.unwrap()->as<bool>()){
    const auto BlockSize = operation_->block_manager_->block_size();
    std::vector<u8> buffer(BlockSize);
    auto inode_block_id_res = operation_->inode_manager_->get(id);
    if(inode_block_id_res.is_ok()){
      block_id_t inode_block_id = inode_block_id_res.unwrap();
      operation_->block_manager_->read_block(inode_block_id,buffer.data());
      Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
      for(uint i = 0; i < inode_p->get_ntuples(); i++){
        if(std::get<0>(inode_p->block_infos[i]) == block_id && std::get<1>(inode_p->block_infos[i])== machine_id){
          std::get<0>(inode_p->block_infos[i]) = 0;
          std::get<1>(inode_p->block_infos[i]) = 0;
          std::get<2>(inode_p->block_infos[i]) = 0;
          inode_p->inner_attr.size -= DiskBlockSize;
          break;
        }
      }
      operation_->block_manager_->write_block(inode_block_id,buffer.data());
      return true;
    }
  }
  return false;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::vector<std::pair<std::string, inode_id_t>> res(0);
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(operation_.get(), node, list);
  if(read_res.is_err()){
    return res;
  }
  for(auto it = list.begin(); it != list.end(); it++){
    res.push_back(std::pair<std::string, inode_id_t>(it->name, it->id));
  }

  return res;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(global_mtx);
  auto res = operation_->get_type_attr(id);
  if(res.is_err()){
    return std::tuple<u64, u64, u64, u64, u8>(0, 0, 0, 0, 0);
  }
  auto attr = res.unwrap();
  InodeType type = attr.first;
  FileAttr file_attr = attr.second;
  return std::tuple<u64, u64, u64, u64, u8>(file_attr.size, file_attr.atime, file_attr.mtime, file_attr.ctime, static_cast<u8>(type));
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs