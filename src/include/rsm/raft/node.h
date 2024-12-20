#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<StateMachine, Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int agree_num;
    unsigned long heartbeat_timeout;
    unsigned long last_heartbeat;
    unsigned long election_timeout;
    unsigned long election_start_time;
    std::vector<LogEntry<Command>> log_list;
    int max_committed_index;
    int max_applied_index;
    std::vector<int> match_index;
    std::vector<int> next_index;

    std::shared_ptr<BlockManager> bm;

    auto get_current_time() -> unsigned long;
    auto gen_random_timeout(int max, int min) -> unsigned long;
    auto get_last_log_term() -> int;
    auto get_last_log_index() -> int;
    auto persist_snapshot() -> bool;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */ 
    this->bm = std::make_shared<BlockManager>("/tmp/raft_log/data" + std::to_string(this->my_id));
    this->thread_pool = std::make_unique<ThreadPool>(32);
    this->log_storage = std::make_unique<RaftLog<StateMachine, Command>>(this->bm);
    this->state = std::make_unique<StateMachine>();

    if (this->log_storage->need_recover()) {
      this->log_storage->recover(this->current_term, this->leader_id, this->log_list, this->state);
      this->max_applied_index = this->log_list[0].logic_index;
      this->max_committed_index = this->log_list[0].logic_index;
    } else {
      LogEntry<Command> cmd;
      cmd.logic_index = 0;
      cmd.term = 0;
      this->log_list.push_back(cmd);
      this->max_committed_index = 0;
      this->max_applied_index = 0;
    }

    this->agree_num = 0;
    this->next_index.resize(this->node_configs.size());
    this->match_index.resize(this->node_configs.size());
    this->last_heartbeat = this->get_current_time();
    this->election_start_time = this->get_current_time();
    this->election_timeout = this->gen_random_timeout(300, 150);
    this->heartbeat_timeout = this->gen_random_timeout(300, 150);

    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
    this->bm.reset();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_current_time() -> unsigned long {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::gen_random_timeout(int max, int min) -> unsigned long {
  std::default_random_engine random_engine(
    std::chrono::system_clock::now().time_since_epoch().count()
  );
  std::uniform_int_distribution<unsigned long> distribution(min, max);
  return distribution(random_engine);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_last_log_term() -> int {
  int size = this->log_list.size();
  LogEntry<Command> last_log_entry = log_list[size - 1];
  return last_log_entry.term;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_last_log_index() -> int {
  int size = this->log_list.size();
  LogEntry<Command> last_log_entry = log_list[size - 1];
  return last_log_entry.logic_index;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::persist_snapshot() -> bool {
  std::vector<u8> data = this->state->snapshot();
  return this->log_storage->persist_snapshot(this->log_list[0].logic_index, this->log_list[0].term, data);
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */
    this->rpc_clients_map.clear();
    for(int i = 0; i < this->node_configs.size(); i++) {
        this->rpc_clients_map.insert(std::make_pair(this->node_configs[i].node_id, std::make_unique<RpcClient>(this->node_configs[i].ip_address, this->node_configs[i].port, true)));
    }
    this->stopped.store(false);

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    this->stopped.store(true);
    this->background_election->join();
    this->background_ping->join();
    this->background_commit->join();
    this->background_apply->join();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(this->mtx);
    return std::make_tuple(this->role == RaftRole::Leader, this->current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(this->mtx);
    if(this->role == RaftRole::Leader) {
      int term = this->current_term;
      int index = this->get_last_log_index() + 1;
      Command cmd;
      cmd.deserialize(cmd_data, cmd_size);
      LogEntry<Command> log_entry;
      log_entry.term = term;
      log_entry.logic_index = index;
      log_entry.content = cmd;
      this->log_list.push_back(log_entry);
      this->log_storage->persist_log(this->log_list);
      return std::make_tuple(true, term, index);
    }
    return std::make_tuple(false, this->current_term, this->get_last_log_index());
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    std::lock_guard<std::mutex> lock(this->mtx);
    int max_applied_idx = this->max_applied_index >= this->log_list[0].logic_index ? this->max_applied_index - this->log_list[0].logic_index : -1;
    int max_committed_idx = this->max_committed_index >= this->log_list[0].logic_index ? this->max_committed_index - this->log_list[0].logic_index : -1;
    for(int i = max_applied_idx + 1; i < max_committed_idx; i++) {
      if(i >= 1 && i < this->log_list.size()) {
        this->state->apply_log(this->log_list[i].content);
      }
      return false;
    }
    this->max_applied_index = this->max_committed_index;
    this->log_list[0].logic_index = this->max_committed_index;
    this->log_list[0].term = this->log_list[max_committed_idx].term;
    for(auto it = this->log_list.begin() + 1; it != this->log_list.end(); ) {
      if(it->logic_index <= this->max_committed_index) {
        it = this->log_list.erase(it);
      } else {
        break;
      }
    }
    if(this->role == RaftRole::Leader) {
      for(int i = 0; i < this->rpc_clients_map.size(); i++) {
        this->next_index[i] = this->max_committed_index + 1;
      }
    }
    this->persist_snapshot();
    this->log_storage->persist_log(this->log_list);
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return this->get_snapshot_direct();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    RequestVoteReply reply;
    std::lock_guard<std::mutex> lock(this->mtx);
    bool data_changed = false;
    if (args.candidate_term > this->current_term) {
      this->current_term = args.candidate_term;
      data_changed = true;
      this->leader_id = -1;
      this->role = RaftRole::Follower;
      this->agree_num = 0;
      this->heartbeat_timeout = this->gen_random_timeout(300, 150);
      this->last_heartbeat = this->get_current_time();
    }
    reply.vote_id = this->my_id;
    reply.vote_term = this->current_term;
    bool term_qualified = args.candidate_term == this->current_term;
    bool vote_qualified = this->leader_id == -1 || this->leader_id == args.candidate_id;
    bool log_qualified = !((this->get_last_log_term() > args.last_log_term) || ((this->get_last_log_term() == args.last_log_term) && (this->get_last_log_index() > args.last_log_index)));
    if (term_qualified && vote_qualified && log_qualified) {
      this->last_heartbeat = this->get_current_time();
      reply.vote_granted = true;
      this->leader_id = args.candidate_id;
      data_changed = true;
      this->role = RaftRole::Follower;
    } else {
      reply.vote_granted = false;
    }
    if (data_changed) {
      this->log_storage->persist_state(this->current_term, this->leader_id);
    }
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(this->mtx);
    bool data_changed = false;
    if(reply.vote_term > this->current_term) {
      this->current_term = reply.vote_term;
      this->leader_id = -1;
      this->role = RaftRole::Follower;
      this->agree_num = 0;
      this->heartbeat_timeout = this->gen_random_timeout(300, 150);
      this->last_heartbeat = this->get_current_time();
      data_changed = true;
    }
    if(reply.vote_granted) {
      this->agree_num++;
      if(this->agree_num > this->rpc_clients_map.size() / 2 && this->role == RaftRole::Candidate) {
        this->role = RaftRole::Leader;
        int last_log_index = this->get_last_log_index() + 1;
        for(int i = 0; i < this->rpc_clients_map.size(); i++) { 
          this->next_index[i] = last_log_index;
          this->match_index[i] = 0;
        }
      }
    }
    if(data_changed) {
      this->log_storage->persist_state(this->current_term, this->leader_id);
    }
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    AppendEntriesReply reply;
    reply.success = false;
    AppendEntriesArgs < Command > arg = transform_rpc_append_entries_args < Command > (rpc_arg);
    std::lock_guard < std::mutex > lock(this -> mtx);
    bool data_changed = true;
    if (arg.leader_term >= this -> current_term) {
      this -> current_term = arg.leader_term;
      this -> leader_id = arg.leader_id;
      this -> role = RaftRole::Follower;
      this -> last_heartbeat = this -> get_current_time();
      this -> heartbeat_timeout = this -> gen_random_timeout(300, 150);
      this -> agree_num = 0;
      data_changed = true;
    }
    if (arg.leader_term == this -> current_term) {
      int last_log_index;
      this -> last_heartbeat = this -> get_current_time();
      if (arg.entries.size() == 0) {
        int phy_last_log_index = arg.prev_log_index < this -> log_list[0].logic_index ? -1 : arg.prev_log_index - this -> log_list[0].logic_index;
        if (phy_last_log_index == -1 || this -> log_list[phy_last_log_index].term != arg.prev_log_term || this -> log_list.size() <= phy_last_log_index) {
          if (data_changed) {
            this->log_storage->persist_state(this->current_term, this->leader_id);
          }
          reply.term = this -> current_term;
          return reply;
        } else {
          reply.success = true;
          last_log_index = this -> log_list[phy_last_log_index].logic_index;
        }
      } else {
        int phy_last_log_index = arg.prev_log_index < this -> log_list[0].logic_index ? -1 : arg.prev_log_index - this -> log_list[0].logic_index;
        if (phy_last_log_index == -1 || this -> log_list[phy_last_log_index].term != arg.prev_log_term || this -> log_list.size() <= phy_last_log_index) {
          if (data_changed) {
            this->log_storage->persist_state(this->current_term, this->leader_id);
          }
          reply.term = this -> current_term;
          return reply;
        }
        reply.success = true;
        last_log_index = this -> log_list[phy_last_log_index].logic_index;
        phy_last_log_index++;
        for (int i = 0; i < arg.entries.size(); i++) {
          if (phy_last_log_index + i < this -> log_list.size()) {
            this -> log_list[phy_last_log_index + i].content = arg.entries[i];
            this -> log_list[phy_last_log_index + i].term = arg.leader_term;
            last_log_index++;
          } else {
            last_log_index++;
            LogEntry < Command > new_entry;
            new_entry.content = arg.entries[i];
            new_entry.term = arg.leader_term;
            new_entry.logic_index = last_log_index;
            this -> log_list.push_back(new_entry);
          }
        }
        this->log_storage->persist_log(this->log_list);
      }
      if (this -> max_committed_index < arg.leader_commit) {
        this -> max_committed_index = std::min(arg.leader_commit, last_log_index);
      }
    }
    if (data_changed) {
      this->log_storage->persist_state(this->current_term, this->leader_id);
    }
    reply.term = this -> current_term;
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::lock_guard < std::mutex > lock(this -> mtx);
    bool data_changed = false;
    if (reply.term > this -> current_term) {
      this -> current_term = reply.term;
      this -> leader_id = -1;
      this -> role = RaftRole::Follower;
      this -> heartbeat_timeout = this -> gen_random_timeout(300, 150);
      this -> last_heartbeat = this -> get_current_time();
      data_changed = true;
    }
    if (this -> role != RaftRole::Leader) {
      if (data_changed) {
        this->log_storage->persist_state(this->current_term, this->leader_id);
      }
      return;
    }
    if (reply.success) {
      if (arg.entries.size() > 0) {
        this -> match_index[node_id] = arg.entries.size() + arg.prev_log_index;
        this -> next_index[node_id] = this -> match_index[node_id] + 1;
        for (int i = this -> max_committed_index + 1 > this -> log_list[0].logic_index ? this -> max_committed_index + 1 - this -> log_list[0].logic_index : -1; i < this -> log_list.size(); i++) {
          int count = 0;
          if (this -> log_list[i].term != this -> current_term) {
            continue;
          }
          int log_index = -1;
          if (i >= 0 && i < this -> log_list.size()) {
            log_index = this -> log_list[i].logic_index;
          }
          for (int j = 0; j < this -> rpc_clients_map.size(); j++) {
            if (j == this -> my_id) {
              count++;
              continue;
            }
            if (this -> match_index[j] >= log_index) {
              count++;
            }
          }
          if (count > this -> rpc_clients_map.size() / 2) {
            this -> max_committed_index = log_index;
          }
        }
      }
    } else {
      if (this -> next_index[node_id] - this -> log_list[0].logic_index == 1) {
        InstallSnapshotArgs args;
        args.leader_id = this -> my_id;
        args.leader_term = this -> current_term;
        args.last_included_index = this -> log_list[0].logic_index;
        args.last_included_term = this -> log_list[0].term;
        args.snapshot = this -> state -> snapshot();
        this -> thread_pool -> enqueue([this, node_id, args]() {
          this -> send_install_snapshot(node_id, args);
        });
      } else {
        this -> next_index[node_id] = this -> next_index[node_id] - 1;
      }
    }
    if (data_changed) {
      this->log_storage->persist_state(this->current_term, this->leader_id);
    }
      return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    InstallSnapshotReply reply;
    std::lock_guard < std::mutex > lock(this -> mtx);
    if(args.leader_term >= this -> current_term) {
      this -> current_term = args.leader_term;
      this -> leader_id = args.leader_id;
      this -> role = RaftRole::Follower;
      this -> last_heartbeat = this -> get_current_time();
      this -> heartbeat_timeout = this -> gen_random_timeout(300, 150);
      this -> agree_num = 0;
      this->log_storage->persist_state(this->current_term, this->leader_id);
    }
    if(args.leader_term == this -> current_term) {
      this -> last_heartbeat = this -> get_current_time();
    }
    if(args.leader_term == this -> current_term && args.last_included_index > this -> log_list[0].logic_index) {
      this -> state -> apply_snapshot(args.snapshot);
      this -> log_list[0].term = args.last_included_term;
      this -> log_list[0].logic_index = args.last_included_index;
      for(auto it = this -> log_list.begin() + 1; it != this -> log_list.end();) {
        if((*it).logic_index <= args.last_included_index) {
          it = this -> log_list.erase(it);
        } else {
          break;
        }
      }
      this -> max_committed_index = std::max(this -> max_committed_index, args.last_included_index);
      this -> max_applied_index = args.last_included_index;
      this -> persist_snapshot();
      this->log_storage->persist_log(this->log_list);
    }
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(this->mtx);
    if (reply.term > this->current_term) {
      this->leader_id = -1;
      this->current_term = reply.term;
      this->role = RaftRole::Follower;
      this->heartbeat_timeout = this->gen_random_timeout(300, 150);
      this->last_heartbeat = this->get_current_time();
      this->log_storage->persist_state(this->current_term, this->leader_id);
    }
    if (this->role == RaftRole::Leader) {
      this->match_index[node_id] = std::max(arg.last_included_index, this->match_index[node_id]);
      this->next_index[node_id] = this->match_index[node_id] + 1;
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(this->mtx);
            bool data_changed = false;
            bool follower_timeout = this->get_current_time() - this->last_heartbeat > this->heartbeat_timeout && this->role == RaftRole::Follower;
            bool candidate_timeout = this->get_current_time() - this->election_start_time > this->election_timeout && this->role == RaftRole::Candidate;
            if(follower_timeout || candidate_timeout) {
              this->role = RaftRole::Candidate;
              this->current_term++;
              this->agree_num = 1;
              this->leader_id = this->my_id;
              data_changed = true;
              this->election_start_time = this->get_current_time();
              this->election_timeout = this->gen_random_timeout(300, 150);
              for(int i = 0; i < this->rpc_clients_map.size(); i++) {
                if(i != this->my_id) {
                  RequestVoteArgs args;
                  args.candidate_id = this->my_id;
                  args.candidate_term = this->current_term;
                  args.last_log_index = this->get_last_log_index();
                  args.last_log_term = this->get_last_log_term();
                  this->thread_pool->enqueue([this, i, args]() {
                    this->send_request_vote(i, args);
                  });
                }
              }
            }
            if(data_changed) {
              this->log_storage->persist_state(this->current_term, this->leader_id);
            }
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(this->mtx);
            if(this->role == RaftRole::Leader){
              for(int i = 0; i < this->rpc_clients_map.size(); i++) {
                if(i != this->my_id) {
                  AppendEntriesArgs < Command > args;
                  int prev_idx = this->next_index[i] - 1 >= this->log_list[0].logic_index ? this->next_index[i] - 1 - this->log_list[0].logic_index : -1;
                  for(int j = prev_idx + 1; j < this->log_list.size(); j++) {
                    args.entries.push_back(this->log_list[j].content);
                  }
                  if(!args.entries.empty()) {
                    args.leader_id = this->my_id;
                    args.leader_term = this->current_term;
                    args.leader_commit = this->max_committed_index;
                    int next_idx = this->next_index[i];
                    args.prev_log_index = next_idx - 1;
                    args.prev_log_term = this->log_list[prev_idx].term;
                    this->thread_pool->enqueue([this, i, args]() {
                      this->send_append_entries(i, args);
                    });
                  }
                }
              }
            }
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(40));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(this->mtx);
            int last_apply_idx = this->max_applied_index + 1;
            int apply_idx = last_apply_idx - this->log_list[0].logic_index;
            for(int log_index = last_apply_idx, i = apply_idx; log_index <= this->max_committed_index; log_index++, i++) {
              if(i >= 1 && i < this->log_list.size()) {
                this->state->apply_log(this->log_list[i].content);
                this->max_applied_index++;
              }
            }
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(this->mtx);
            if (this->role == RaftRole::Leader) {
              for (int i = 0; i < this->rpc_clients_map.size(); ++i) {
                if (i != this->my_id){
                  AppendEntriesArgs<Command> args;
                  args.leader_id = this->my_id;
                  args.leader_term = this->current_term;
                  int next_idx = this->next_index[i];
                  args.prev_log_index = next_idx - 1;
                  args.prev_log_term = this->log_list[next_idx - 1 >= this->log_list[0].logic_index ? next_idx - 1 - this->log_list[0].logic_index : -1].term;
                  args.leader_commit = this->max_committed_index;
                  this->thread_pool->enqueue([this, i, args]() {
                    this->send_append_entries(i, args);
                  });
                }
              }
            }
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}