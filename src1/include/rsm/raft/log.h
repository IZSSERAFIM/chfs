#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

template <typename content_type> struct LogEntry {
    int term = 0;
    int logic_index = 0;
    content_type content;

    int serialize_size() {
      return sizeof(term) + sizeof(logic_index) + content.size();
    }

    void serialize(std::vector<u8> &data) {
      if (!data.empty()) {
        return;
      }
      data.resize(this->serialize_size());
      u8 *ptr = data.data();
      *(reinterpret_cast<int *>(ptr)) = this->term;
      *(reinterpret_cast<int *>(ptr + 4)) = this->logic_index;
      std::vector<u8> content_data =
          this->content.serialize(this->content.size());
      memcpy((ptr + 8), content_data.data(), this->content.size());
      return;
    }

    void deserialize(std::vector<u8> &data) {
      if (data.size() != this->serialize_size()) {
        return;
      }
      u8 *ptr = data.data();
      this->term = *(reinterpret_cast<int *>(ptr));
      this->logic_index = *(reinterpret_cast<int *>(ptr + 4));
      std::vector<u8> content_data(data.begin() + 8, data.end());
      this->content.deserialize(content_data, this->content.size());
      return;
    }
};

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename StateMachine, typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    /* Lab3: Your code here */
    bool persist_state(int current_term, int leader_id);
    bool persist_log(std::vector<LogEntry<Command>> &log_list);
    bool persist_snapshot(int last_include_index, int last_include_term, std::vector<u8> &snapshot_data);
    bool recover(int &current_term, int &leader_id, std::vector<LogEntry<Command>> &log_list, std::unique_ptr<StateMachine> &state);
    bool need_recover();
private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    struct metadata_pos_config {
      int block_id;
      int offset;
      int length;
      void modify(int bid, int off, int len) {
        this->block_id = bid;
        this->offset = off;
        this->length = len;
      }
    };

    int log_num_per_block;
    int log_ser_size;

    int valid;
    int current_term;
    int leader_id;
    int snapshot_start_block;
    int snapshot_end_block;
    int log_start_block;
    int log_num;
    int snapshot_data_size;
    int snapshot_last_index;
    int snapshot_last_term;

    metadata_pos_config config_valid;
    metadata_pos_config config_current_term;
    metadata_pos_config config_leader_id;
    metadata_pos_config config_snapshot_start_block;
    metadata_pos_config config_snapshot_end_block;
    metadata_pos_config config_log_start_block;
    metadata_pos_config config_log_num;
    metadata_pos_config config_snapshot_data_size;
    metadata_pos_config config_snapshot_last_index;
    metadata_pos_config config_snapshot_last_term;

    int get_int(u8 *data, int off);
    void flush_int(u8 *dst, int src, int off);
};

template <typename StateMachine, typename Command>
RaftLog<StateMachine, Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    /* Lab3: Your code here */
    this->bm_ = bm;
    int offset = 0;
    this->config_valid.modify(0, offset, sizeof(this->valid));
    offset += sizeof(this->valid);
    this->config_current_term.modify(0, offset, sizeof(this->current_term));
    offset += sizeof(this->current_term);
    this->config_leader_id.modify(0, offset, sizeof(this->leader_id));
    offset += sizeof(this->leader_id);
    this->config_snapshot_start_block.modify(0, offset, sizeof(this->snapshot_start_block));
    offset += sizeof(this->snapshot_start_block);
    this->config_snapshot_end_block.modify(0, offset, sizeof(this->snapshot_end_block));
    offset += sizeof(this->snapshot_end_block);
    this->config_log_start_block.modify(0, offset, sizeof(this->log_start_block));
    offset += sizeof(this->log_start_block);
    this->config_log_num.modify(0, offset, sizeof(this->log_num));
    offset += sizeof(this->log_num);
    this->config_snapshot_data_size.modify(0, offset, sizeof(this->snapshot_data_size));
    offset += sizeof(this->snapshot_data_size);
    this->config_snapshot_last_index.modify(0, offset, sizeof(this->snapshot_last_index));
    offset += sizeof(this->snapshot_last_index);
    this->config_snapshot_last_term.modify(0, offset, sizeof(this->snapshot_last_term));

    u8 *metadata_block = new u8[this->bm_->block_size()];
    this->bm_->read_block(0, metadata_block);
    this->valid = this->get_int(metadata_block, config_valid.offset);
    if (this->valid == 1) {
      this->current_term = this->get_int(metadata_block, config_current_term.offset);
      this->leader_id = this->get_int(metadata_block, config_leader_id.offset);
      this->snapshot_start_block = this->get_int(metadata_block, config_snapshot_start_block.offset);
      this->snapshot_end_block = this->get_int(metadata_block, config_snapshot_end_block.offset);
      this->log_start_block = this->get_int(metadata_block, config_log_start_block.offset);
      this->log_num = this->get_int(metadata_block, config_log_num.offset);
      this->snapshot_data_size = this->get_int(metadata_block, config_snapshot_data_size.offset);
      this->snapshot_last_index = this->get_int(metadata_block, config_snapshot_last_index.offset);
      this->snapshot_last_term = this->get_int(metadata_block, config_snapshot_last_term.offset);
    } else {
      this->valid = 1;
      this->current_term = 0;
      this->leader_id = -1;
      this->snapshot_start_block = 1;
      this->snapshot_end_block = 2;
      this->log_start_block = 2;
      this->log_num = 0;
      this->snapshot_data_size = 0;
      this->snapshot_last_index = 0;
      this->snapshot_last_term = 0;
      this->flush_int(metadata_block, this->valid, this->config_valid.offset);
      this->flush_int(metadata_block, this->current_term, this->config_current_term.offset);
      this->flush_int(metadata_block, this->leader_id, this->config_leader_id.offset);
      this->flush_int(metadata_block, this->snapshot_start_block, this->config_snapshot_start_block.offset);
      this->flush_int(metadata_block, this->snapshot_end_block, this->config_snapshot_end_block.offset);
      this->flush_int(metadata_block, this->log_start_block, this->config_log_start_block.offset);
      this->flush_int(metadata_block, this->log_num, this->config_log_num.offset);
      this->flush_int(metadata_block, this->snapshot_data_size, this->config_snapshot_data_size.offset);
      this->flush_int(metadata_block, this->snapshot_last_index, this->config_snapshot_last_index.offset);
      this->flush_int(metadata_block, this->snapshot_last_term, this->config_snapshot_last_term.offset);
      this->bm_->write_block(0, metadata_block);
      this->bm_->sync(0);
    }
    delete[] metadata_block;
    LogEntry<Command> tmp;
    this->log_ser_size = tmp.serialize_size();
    this->log_num_per_block = this->bm_->block_size() / this->log_ser_size;
}

template <typename StateMachine, typename Command>
RaftLog<StateMachine, Command>::~RaftLog()
{
    /* Lab3: Your code here */
    this->bm_.reset();
}

/* Lab3: Your code here */
template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::persist_state(int current_term, int leader_id) {
  int *data = new int[2];
  data[0] = current_term;
  data[1] = leader_id;

  std::unique_lock<std::mutex> lock(this->mtx);
  this->bm_->write_partial_block(
      config_current_term.block_id, reinterpret_cast<u8 *>(data),
      config_current_term.offset, config_current_term.length);
  this->bm_->write_partial_block(
      config_leader_id.block_id, reinterpret_cast<u8 *>(data + 1),
      config_leader_id.offset, config_leader_id.length);
  this->bm_->sync(0);
  delete[] data;
  lock.unlock();
  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::persist_log(std::vector<LogEntry<Command>> &log_list) {
  std::unique_lock<std::mutex> lock(this->mtx);
  int current_block_id = this->log_start_block;
  int current_off = 0;
  std::vector<u8> log_ser;
  bool is_first = true;
  for (LogEntry<Command> item : log_list) {
    if (is_first) {
      is_first = false;
      continue;
    }
    log_ser.clear();
    item.serialize(log_ser);
    this->bm_->write_partial_block(current_block_id, log_ser.data(), current_off * (this->log_ser_size), this->log_ser_size);
    current_off += 1;
    if (current_off == this->log_num_per_block) {
      current_off = 0;
      current_block_id += 1;
    }
  }
  for (int i = this->log_start_block; i <= current_block_id; ++i) {
    this->bm_->sync(i);
  }

  this->log_num = log_list.size() - 1;
  this->bm_->write_partial_block(0, reinterpret_cast<u8 *>(&(this->log_num)), config_log_num.offset, config_log_num.length);
  this->bm_->sync(0);
  lock.unlock();
  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::persist_snapshot(int last_include_index, int last_include_term, std::vector<u8> &snapshot_data) {
  std::unique_lock<std::mutex> lock(this->mtx);
  assert((((snapshot_data.size() / this->bm_->block_size()) + ((snapshot_data.size() % this->bm_->block_size()) > 0) + this->snapshot_start_block) <= this->snapshot_end_block) && "snapshot data too large");
  this->snapshot_last_index = last_include_index;
  this->snapshot_last_term = last_include_term;
  this->snapshot_data_size = snapshot_data.size();
  this->bm_->write_partial_block(
      0, reinterpret_cast<u8 *>(&(this->snapshot_last_index)),
      config_snapshot_last_index.offset, config_snapshot_last_index.length);
  this->bm_->write_partial_block(
      0, reinterpret_cast<u8 *>(&(this->snapshot_last_term)),
      config_snapshot_last_term.offset, config_snapshot_last_term.length);
  this->bm_->write_partial_block(
      0, reinterpret_cast<u8 *>(&(this->snapshot_data_size)),
      config_snapshot_data_size.offset, config_snapshot_data_size.length);
  this->bm_->sync(0);
  if (snapshot_data.size() > 0) {
    int left_data_size = snapshot_data.size();
    int offset = 0;
    int current_block = this->snapshot_start_block;
    do {
      int chunk_size = (left_data_size >= this->bm_->block_size()) ? this->bm_->block_size() : left_data_size;
      this->bm_->write_partial_block(current_block, (snapshot_data.data() + offset), 0, chunk_size);
      this->bm_->sync(current_block);
      ++current_block;
      left_data_size -= chunk_size;
      offset += chunk_size;
    } while (left_data_size > 0);
  }
  lock.unlock();
  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::recover(
  int &current_term, int &leader_id, std::vector<LogEntry<Command>> &log_list,
  std::unique_ptr<StateMachine> &state) {
  std::unique_lock<std::mutex> lock(this->mtx);
  current_term = this->current_term;
  leader_id = this->leader_id;
  if (!log_list.empty()) {
    lock.unlock();
    return false;
  }
  if (this->snapshot_data_size > 0) {
    std::vector<u8> snapshot_data;
    snapshot_data.reserve(this->snapshot_data_size);
    int left_data_size = this->snapshot_data_size;
    int current_block = this->snapshot_start_block;
    do {
      int chunk_size = (left_data_size >= this->bm_->block_size()) ? this->bm_->block_size() : left_data_size;
      std::vector<u8> page_data(this->bm_->block_size());
      this->bm_->read_block(current_block, page_data.data());
      snapshot_data.insert(snapshot_data.end(), page_data.begin(), page_data.begin() + chunk_size);
      ++current_block;
      left_data_size -= chunk_size;
    } while (left_data_size > 0);
    state->apply_snapshot(snapshot_data);
  }
  LogEntry<Command> obj;
  obj.logic_index = this->snapshot_last_index;
  obj.term = this->snapshot_last_term;
  log_list.push_back(obj);
  int current_block_id = this->log_start_block;
  int current_off = 0;
  std::vector<u8> page_data(this->bm_->block_size());
  this->bm_->read_block(current_block_id, page_data.data());
  for (int i = 0; i < this->log_num; ++i) {
    std::vector<u8> log_ser(page_data.begin() + (current_off * this->log_ser_size), page_data.begin() + ((current_off + 1) * this->log_ser_size));
    obj.deserialize(log_ser);
    log_list.push_back(obj);

    ++current_off;
    if (current_off == this->log_num_per_block) {
      current_off = 0;
      ++current_block_id;
      this->bm_->read_block(current_block_id, page_data.data());
    }
  }

  lock.unlock();
  return true;
}

template <typename StateMachine, typename Command>
bool RaftLog<StateMachine, Command>::need_recover() {
  std::unique_lock<std::mutex> lock(this->mtx);
  return this->valid == 1;
}

template <typename StateMachine, typename Command>
int RaftLog<StateMachine, Command>::get_int(u8 *data, int off) {
  u8 *tmp = data + off;
  return *(reinterpret_cast<int *>(tmp));
}

template <typename StateMachine, typename Command>
void RaftLog<StateMachine, Command>::flush_int(u8 *dst, int src, int off) {
  int *obj = new int(src);
  u8 *pos = dst + off;
  memcpy(reinterpret_cast<char *>(pos), reinterpret_cast<char *>(obj), sizeof(int));
  delete obj;
}

} /* namespace chfs */
