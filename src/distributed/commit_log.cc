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
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm), log_current_offset(0), num_tx_in_log(0), current_txn_id(1){
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  return num_tx_in_log;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = DiskBlockSize;
  log_mtx.lock();
  ++num_tx_in_log;
  auto offset = log_current_offset;
  for (auto &op : ops){
    LogEntry log_entry;
    log_entry.txn_id = txn_id;
    log_entry.block_id = op->block_id_;
    std::vector<u8> buffer(sizeof(LogEntry) + BLOCK_SIZE);
    log_entry.flush_to_buffer(buffer.data());
    auto log_entry_ptr = reinterpret_cast<LogEntry *>(buffer.data());
    for (int i = 0; i < BLOCK_SIZE; ++i) {
      log_entry_ptr->new_block_state[i] = op->new_block_state_[i];
    }
    bm_->write_log_entry(log_current_offset, buffer.data(), sizeof(LogEntry) + BLOCK_SIZE);
    log_current_offset += sizeof(LogEntry) + BLOCK_SIZE;
  }
  auto start_block_id = offset / bm_->block_size();
  auto end_block_id = log_current_offset / bm_->block_size();
  const auto base_block_id = bm_->total_blocks();
  for (auto i = start_block_id; i < end_block_id; ++i){
    bm_->sync(i + base_block_id);
  }
  commit_log(txn_id);
  if(is_checkpoint_enabled_){
    if(log_current_offset >= BLOCK_SIZE * 1000 || num_tx_in_log >= 100){
      checkpoint();
    }
  }
  log_mtx.unlock();
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = DiskBlockSize;
  auto offset = log_current_offset;
  LogEntry log_entry;
  log_entry.txn_id = txn_id;
  log_entry.block_id = 0xFFFFFFFFFFFFFFFF;
  std::vector<u8> buffer(sizeof(LogEntry) + BLOCK_SIZE);
  log_entry.flush_to_buffer(buffer.data());
  bm_->write_log_entry(log_current_offset, buffer.data(), sizeof(LogEntry) + BLOCK_SIZE);
  log_current_offset += sizeof(LogEntry) + BLOCK_SIZE;
  auto start_block_id = offset / bm_->block_size();
  auto end_block_id = log_current_offset / bm_->block_size();
  const auto block_id = bm_->total_blocks();
  for (auto i = start_block_id; i < end_block_id; ++i){
    bm_->sync(i + block_id);
  }
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = DiskBlockSize;
  auto log_start = bm_->get_log_start();
  memset(log_start, 0, BLOCK_SIZE * 1024);
  log_current_offset = 0;
  num_tx_in_log = 0;
  bm_->flush();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = DiskBlockSize;
  const auto log_start = bm_->get_log_start();
  const auto log_end = bm_->get_log_end();
  auto it = log_start;
  while(it < log_end){
    auto log_entry_ptr = reinterpret_cast<LogEntry *>(it);
    if(log_entry_ptr->txn_id == 0){
      break;
    }
    auto entry_num = 0;
    auto id = log_entry_ptr->txn_id;
    bool committed = false;
    auto txn_it = it;
    while(txn_it < log_end){
      entry_num++;
      auto log_entry_ptr = reinterpret_cast<LogEntry *>(txn_it);
      if(log_entry_ptr->txn_id != id){
        break;
      }
      if(log_entry_ptr->block_id == 0xFFFFFFFFFFFFFFFF){
        committed = true;
        break;
      }
      txn_it += (sizeof(LogEntry) + BLOCK_SIZE);
    }
    if(!committed){
      it += (sizeof(LogEntry) + BLOCK_SIZE) * entry_num;
      continue;
    }
    auto redo_it = it;
    while(redo_it < txn_it){
      auto log_entry_ptr = reinterpret_cast<LogEntry *>(redo_it);
      auto block_id = log_entry_ptr->block_id;
      auto new_block_state = log_entry_ptr->new_block_state;
      std::vector<u8> buffer(BLOCK_SIZE);
      for (int i = 0; i < BLOCK_SIZE; ++i) {
        buffer[i] = new_block_state[i];
      }
      bm_->write_block_for_recover(block_id, buffer.data());
      redo_it += (sizeof(LogEntry) + BLOCK_SIZE);
    }
    it += (sizeof(LogEntry) + BLOCK_SIZE) * entry_num;
  }
}
}; // namespace chfs