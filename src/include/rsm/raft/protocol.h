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
    int term;           // candidate’sterm
    int candidateId;    // candidate requesting vote
    int lastLogIndex;   // index of candidate’s last log entry
    int lastLogTerm;    // term of candidate’s last log entry

    RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm)
        : term(term), candidateId(candidateId), lastLogIndex(lastLogIndex), lastLogTerm(lastLogTerm) {}
    
    RequestVoteArgs() {}
    
    MSGPACK_DEFINE(
        term,
        candidateId,
        lastLogIndex,
        lastLogTerm
    )
};

struct RequestVoteReply {
    int term;           // currentTerm, for candidate to update itself
    bool voteGranted;   // true means candidate received vote

    RequestVoteReply(int term, bool voteGranted)
        : term(term), voteGranted(voteGranted) {}

    RequestVoteReply() {}

    MSGPACK_DEFINE(
        term,
        voteGranted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    int term;           // leader’s term
    int leaderId;       // so follower can redirect clients
    int prevLogIndex;   // index of log entry immediately preceding new ones
    int prevLogTerm;    // term of prevLogIndex entry
    std::vector<RaftLogEntry<Command>> entries; // log entries to store (empty for heartbeat; may send more than one for efficiency)
    int leaderCommit;   // leader’s commitIndex

    AppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm, std::vector<RaftLogEntry<Command>> entries, int leaderCommit)
        : term(term), leaderId(leaderId), prevLogIndex(prevLogIndex), prevLogTerm(prevLogTerm), entries(entries), leaderCommit(leaderCommit) {}

    AppendEntriesArgs() {}

    MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit
    )
};

struct RpcAppendEntriesArgs {
    int term;           // leader’s term
    int leaderId;       // so follower can redirect clients
    int prevLogIndex;   // index of log entry immediately preceding new ones
    int prevLogTerm;    // term of prevLogIndex entry
    std::vector<u8> entries; // log entries to store (empty for heartbeat; may send more than one for efficiency)
    int leaderCommit;   // leader’s commitIndex

    RpcAppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm, std::vector<u8> entries, int leaderCommit)
        : term(term), leaderId(leaderId), prevLogIndex(prevLogIndex), prevLogTerm(prevLogTerm), entries(entries), leaderCommit(leaderCommit) {}

    RpcAppendEntriesArgs() {}

    MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    RpcAppendEntriesArgs rpc_arg;
    rpc_arg.term = arg.term;
    rpc_arg.leaderId = arg.leaderId;
    rpc_arg.prevLogIndex = arg.prevLogIndex;
    rpc_arg.prevLogTerm = arg.prevLogTerm;
    rpc_arg.leaderCommit = arg.leaderCommit;
    rpc_arg.entries = {};
    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    AppendEntriesArgs<Command> arg;
    arg.term = rpc_arg.term;
    arg.leaderId = rpc_arg.leaderId;
    arg.prevLogIndex = rpc_arg.prevLogIndex;
    arg.prevLogTerm = rpc_arg.prevLogTerm;
    arg.leaderCommit = rpc_arg.leaderCommit;
    arg.entries = {};
    return arg;
}

struct AppendEntriesReply {
    int term;           // currentTerm, for leader to update itself
    bool success;       // true if follower contained entry matching prevLogIndex and prevLogTerm

    AppendEntriesReply(int term, bool success)
        : term(term), success(success) {}

    AppendEntriesReply() {}

    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

} /* namespace chfs */