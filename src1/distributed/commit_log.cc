#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
    init(bm);
}

void CommitLog::init(std::shared_ptr<BlockManager> bm) {
    const auto log_block_size = bm->block_size();
    std::vector<u8> log_bitmap_buffer(log_block_size);
    auto log_bitmap = Bitmap(log_bitmap_buffer.data(), log_block_size);
    log_bitmap.zeroed();
    log_block_id = bm->total_blocks() - 1024;
    for(block_id_t log_index = 0; log_index < kMaxLogSize + 1; log_index++){
        log_bitmap.set(log_index);
    }
    bitmap_block_id = log_block_id + kMaxLogSize;
    bm->write_block(bitmap_block_id, log_bitmap_buffer.data());
    bm->sync(bitmap_block_id);
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto log_block_size = bm_->block_size();
  usize log_entry_count = 0;
  for (block_id_t current_log_block = log_block_id; current_log_block < bitmap_block_id; current_log_block++) {
    std::vector<u8> log_block_buffer(log_block_size);
    bm_->read_block(current_log_block, log_block_buffer.data());
    Log_info *log_info = reinterpret_cast<Log_info *>(log_block_buffer.data());
    if (log_info->txn_id) {
      log_entry_count++;
    }
  }
  return log_entry_count;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto log_block_size = bm_->block_size();
  std::vector<u8> log_bitmap_buffer(log_block_size);
  bm_->read_block(bitmap_block_id, log_bitmap_buffer.data());
  auto log_bitmap = Bitmap(log_bitmap_buffer.data(), log_block_size);
  for (block_id_t current_log_block = log_block_id; current_log_block < bitmap_block_id; current_log_block++) {
    std::vector<u8> log_block_buffer(log_block_size);
    bm_->read_block(current_log_block, log_block_buffer.data());
    Log_info *log_info = reinterpret_cast<Log_info *>(log_block_buffer.data());
    if (log_info->txn_id == 0) {
      log_info->txn_id = txn_id;
      log_info->finished = false;
      usize block_mapping_index = 0;
      for (auto block_op = ops.begin(); block_op < ops.end(); block_op++) {
        auto free_bitmap_block = log_bitmap.find_first_free_w_bound(1024);
        if (free_bitmap_block) {
          log_bitmap.set(free_bitmap_block.value());
          block_id_t free_block_id = free_bitmap_block.value() + log_block_id;
          log_info->block_id_map[block_mapping_index].first = (*block_op)->block_id_;
          log_info->block_id_map[block_mapping_index].second = free_block_id;
          bm_->write_block(free_block_id, (*block_op)->new_block_state_.data());
          block_mapping_index++;
        }
      }
      bm_->write_block(current_log_block, log_block_buffer.data());
      break;
    }
  }
  bm_->write_block(bitmap_block_id, log_bitmap_buffer.data());
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto log_block_size = bm_->block_size();
  for (block_id_t current_log_block = log_block_id; current_log_block < bitmap_block_id; current_log_block++) {
    std::vector<u8> log_block_buffer(log_block_size);
    bm_->read_block(current_log_block, log_block_buffer.data());
    Log_info *log_info = reinterpret_cast<Log_info *>(log_block_buffer.data());
    if (log_info->txn_id == txn_id) {
      log_info->finished = true;
      bm_->write_block(current_log_block, log_block_buffer.data());
      break;
    }
  }
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto log_block_size = bm_->block_size();
  if (is_checkpoint_enabled_) {
    std::vector<u8> log_bitmap_buffer(log_block_size);
    bm_->read_block(bitmap_block_id, log_bitmap_buffer.data());
    auto log_bitmap = Bitmap(log_bitmap_buffer.data(), log_block_size);
    for (block_id_t current_log_block = log_block_id; current_log_block < bitmap_block_id; current_log_block++) {
      std::vector<u8> log_block_buffer(log_block_size);
      bm_->read_block(current_log_block, log_block_buffer.data());
      Log_info *log_info = reinterpret_cast<Log_info *>(log_block_buffer.data());
      if (log_info->txn_id && log_info->finished) {
        for (usize block_map_index = 0; block_map_index < log_info->npairs; block_map_index++) {
          block_id_t destination_block_id = log_info->block_id_map[block_map_index].first;
          if (destination_block_id) {
            block_id_t source_block_id = log_info->block_id_map[block_map_index].second;
            std::vector<u8> block_backup(log_block_size);
            bm_->read_block(source_block_id, block_backup.data());
            bm_->write_block(destination_block_id, block_backup.data());
            log_bitmap.clear(source_block_id - log_block_id);
          }
        }
        bm_->zero_block(current_log_block);
      }
    }
    bm_->write_block(bitmap_block_id, log_bitmap_buffer.data());
  }
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto log_block_size = bm_->block_size();
  std::vector<u8> log_bitmap_buffer(log_block_size);
  bm_->read_block(bitmap_block_id, log_bitmap_buffer.data());
  auto log_bitmap = Bitmap(log_bitmap_buffer.data(), log_block_size);
  for (block_id_t current_log_block = log_block_id; current_log_block < bitmap_block_id; current_log_block++) {
    std::vector<u8> log_block_buffer(log_block_size);
    bm_->read_block(current_log_block, log_block_buffer.data());
    Log_info *log_info = reinterpret_cast<Log_info *>(log_block_buffer.data());
    if (log_info->txn_id) {
      for (usize block_map_index = 0; block_map_index < log_info->npairs; block_map_index++) {
        block_id_t destination_block_id = log_info->block_id_map[block_map_index].first;
        if (destination_block_id) {
          block_id_t source_block_id = log_info->block_id_map[block_map_index].second;
          std::vector<u8> block_backup(log_block_size);
          bm_->read_block(source_block_id, block_backup.data());
          bm_->write_block(destination_block_id, block_backup.data());
        }
      }
    }
  }
}
}; // namespace chfs