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
    std::vector<int> logEntryTerms;
    std::vector<int> logEntryIndexs;
    std::vector<std::vector<u8>> logEntryCommands; // log entries to store (empty for heartbeat; may send more than one for efficiency)
    int leaderCommit;   // leader’s commitIndex

    RpcAppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm, std::vector<int> logEntryTerms, std::vector<int> logEntryIndexs, std::vector<std::vector<u8>> logEntryCommands, int leaderCommit)
        : term(term), leaderId(leaderId), prevLogIndex(prevLogIndex), prevLogTerm(prevLogTerm), logEntryTerms(logEntryTerms), logEntryIndexs(logEntryIndexs), logEntryCommands(logEntryCommands), leaderCommit(leaderCommit) {}

    RpcAppendEntriesArgs() {}

    MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        logEntryTerms,
        logEntryIndexs,
        logEntryCommands,
        leaderCommit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    RpcAppendEntriesArgs rpc_arg(arg.term, arg.leaderId, arg.prevLogIndex, arg.prevLogTerm, {}, {}, {}, arg.leaderCommit);
    for (auto entry : arg.entries) {
        rpc_arg.logEntryTerms.push_back(entry.term);
        rpc_arg.logEntryIndexs.push_back(entry.index);
        std::vector<u8> cmd_data;
        cmd_data = entry.command.serialize(entry.command.size());
        rpc_arg.logEntryCommands.push_back(cmd_data);
    }
    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    AppendEntriesArgs<Command> arg(rpc_arg.term, rpc_arg.leaderId, rpc_arg.prevLogIndex, rpc_arg.prevLogTerm, {}, rpc_arg.leaderCommit);
    for (int i = 0; i < rpc_arg.logEntryIndexs.size(); i++) {
        Command cmd;
        cmd.deserialize(rpc_arg.logEntryCommands[i], rpc_arg.logEntryCommands[i].size());
        arg.entries.push_back(RaftLogEntry<Command>(rpc_arg.logEntryTerms[i], rpc_arg.logEntryIndexs[i], cmd));
    }
    return arg;
}

struct AppendEntriesReply {
    int term;           // currentTerm, for leader to update itself
    bool success;       // true if follower contained entry matching prevLogIndex and prevLogTerm
    int followerIndex = -1; // follower's last log index

    AppendEntriesReply(int term, bool success, int followerIndex = -1)
        : term(term), success(success), followerIndex(followerIndex) {}

    AppendEntriesReply() {}

    MSGPACK_DEFINE(
        term,
        success,
        followerIndex
    )
};

struct InstallSnapshotArgs {
    int term;                   // leader’s term
    int leaderId;               // so follower can redirect clients
    int lastIncludedIndex;      // the snapshot replaces all entries up through and including this index
    int lastIncludedTerm;       // term of lastIncludedIndex
    std::vector<u8> snapshot;   // raw bytes of the snapshot chunk

    InstallSnapshotArgs(int term, int leaderId, int lastIncludedIndex, int lastIncludedTerm, std::vector<u8> snapshot)
        : term(term), leaderId(leaderId), lastIncludedIndex(lastIncludedIndex), lastIncludedTerm(lastIncludedTerm), snapshot(snapshot) {}

    InstallSnapshotArgs() {}

    MSGPACK_DEFINE(
        term,
        leaderId,
        lastIncludedIndex,
        lastIncludedTerm,
        snapshot
    )
};

struct InstallSnapshotReply {
    int term;           // currentTerm, for leader to update itself

    InstallSnapshotReply(int term): term(term) {}

    InstallSnapshotReply() {}

    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */