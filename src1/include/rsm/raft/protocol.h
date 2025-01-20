#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int candidate_id;
    int candidate_term;
    int last_log_index;
    int last_log_term;
    MSGPACK_DEFINE(
        candidate_id,
        candidate_term,
        last_log_index,
        last_log_term
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int vote_id;
    int vote_term;
    bool vote_granted;
    MSGPACK_DEFINE(
        vote_id,
        vote_term,
        vote_granted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int leader_id;
    int leader_term;
    int prev_log_index;
    int prev_log_term;
    int leader_commit;
    std::vector<Command> entries;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int leader_id;
    int leader_term;
    int prev_log_index;
    int prev_log_term;
    int leader_commit;
    std::vector<u8> entries;
    MSGPACK_DEFINE(
        leader_id,
        leader_term,
        prev_log_index,
        prev_log_term,
        leader_commit,
        entries
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    RpcAppendEntriesArgs rpc_arg;
    rpc_arg.leader_id = arg.leader_id;
    rpc_arg.leader_term = arg.leader_term;
    rpc_arg.prev_log_index = arg.prev_log_index;
    rpc_arg.prev_log_term = arg.prev_log_term;
    rpc_arg.leader_commit = arg.leader_commit;
    for (Command entry : arg.entries) {
        std::vector<u8> entry_bytes = entry.serialize(entry.size());
        rpc_arg.entries.insert(rpc_arg.entries.end(), entry_bytes.begin(), entry_bytes.end());
    }
    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> arg;
    arg.leader_id = rpc_arg.leader_id;
    arg.leader_term = rpc_arg.leader_term;
    arg.prev_log_index = rpc_arg.prev_log_index;
    arg.prev_log_term = rpc_arg.prev_log_term;
    arg.leader_commit = rpc_arg.leader_commit;
    Command cmd;
    std::vector<u8> entry_bytes;
    int current = 0;
    int required = cmd.size();
    for(auto byte : rpc_arg.entries) {
        entry_bytes.push_back(byte);
        current++;
        if(current == required) {
            cmd.deserialize(entry_bytes, required);
            arg.entries.push_back(cmd);
            entry_bytes.clear();
            current = 0;
        }
    }
    return arg;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;
    bool success;
    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int leader_id;
    int leader_term;
    int last_included_index;
    int last_included_term;
    std::vector<u8> snapshot;
    MSGPACK_DEFINE(
        leader_id,
        leader_term,
        last_included_index,
        last_included_term,
        snapshot
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    int term;
    bool success;
    MSGPACK_DEFINE(
        term,
        success
    )
};

} /* namespace chfs */