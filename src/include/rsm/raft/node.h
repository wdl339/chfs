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
    
#define DEBUG_LOG(fmt, args...) \
    if(debug_log_enabled) { RAFT_LOG(fmt, args) }  

#define ELECTION_TIMEOUT_LOW 150    
#define ELECTION_TIMEOUT_HIGH 300
#define CANDIDATE_TIMEOUT_LOW 500
#define CANDIDATE_TIMEOUT_HIGH 1000                                                                 

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
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
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

    int voted_for;
    std::vector<RaftLogEntry<Command>>log;

    int commit_idx;
    int last_applied;

    std::vector<int> next_idx;
    std::vector<int> match_idx;

    int n_nodes;
    std::vector<bool> vote_res;

    std::chrono::time_point<std::chrono::system_clock> last_heartbeat_from_leader;
    std::chrono::time_point<std::chrono::system_clock> last_time_become_candidate;
    std::chrono::milliseconds election_timeout;
    std::chrono::milliseconds candidate_timeout;

    bool debug_log_enabled;

};

std::chrono::milliseconds generate_random_timeout_between(int low, int high){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(low, high);
    return std::chrono::milliseconds(dis(gen));
}

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

    thread_pool = std::make_unique<ThreadPool>(48);

    std::string file_path = "/tmp/raft_log/" + std::to_string(my_id);
    auto bm = std::make_shared<BlockManager>(file_path, KDefaultBlockCnt);
    log_storage = std::make_unique<RaftLog<Command>>(bm);

    state = std::make_unique<StateMachine>();

    log_storage->my_id = my_id;
    log_storage->recover();
    if (log_storage->has_log == 1) {
        current_term = log_storage->current_term;
        voted_for = log_storage->voted_for;
        log = log_storage->log;
    } else {
        current_term = 0;
        voted_for = -1;
        log.clear();
        log.push_back(RaftLogEntry<Command>(0, 0, Command(0)));

        log_storage->init_all(current_term, voted_for, log);
    }

    commit_idx = 0;
    last_applied = 0;

    n_nodes = configs.size();
    vote_res.resize(n_nodes, false);

    last_heartbeat_from_leader = std::chrono::system_clock::now();
    last_time_become_candidate = std::chrono::system_clock::now();
    election_timeout = generate_random_timeout_between(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH);
    candidate_timeout = generate_random_timeout_between(CANDIDATE_TIMEOUT_LOW, CANDIDATE_TIMEOUT_HIGH);

    debug_log_enabled = true;

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
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    std::unique_lock<std::mutex> lock(mtx);
    stopped.store(false);
    role = RaftRole::Follower;

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    for (int i = 0; i < n_nodes; i++) {
        auto cli = std::make_unique<RpcClient>(node_configs[i].ip_address, node_configs[i].port, true);
        rpc_clients_map.insert(std::make_pair(node_configs[i].node_id, std::move(cli)));
    }
    lock.unlock();

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    std::unique_lock<std::mutex> lock(mtx);
    stopped.store(true);

    if (background_election) {
        background_election->join();
        background_election.reset();
    }
    if (background_ping) {
        background_ping->join();
        background_ping.reset();
    }
    if (background_commit) {
        background_commit->join();
        background_commit.reset();
    }
    if (background_apply) {
        background_apply->join();
        background_apply.reset();
    }

    for (int i = 0; i < n_nodes; i++) {
        if (rpc_clients_map[i]) {
            rpc_clients_map[i].reset();
        }
    }
    lock.unlock();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    std::unique_lock<std::mutex> lock(mtx);
    auto res = std::make_tuple(role == RaftRole::Leader, current_term);
    lock.unlock();
    return res;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    std::unique_lock<std::mutex> lock(mtx);
    int prev_log_idx = log.back().index;
    if (role != RaftRole::Leader) {
        lock.unlock();
        return std::make_tuple(false, current_term, prev_log_idx);
    }

    DEBUG_LOG("Node %d receive new command", my_id);
    int new_log_idx = prev_log_idx + 1;
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);
    RaftLogEntry<Command> log_entry(current_term, new_log_idx, cmd);
    log.push_back(log_entry);
    log_storage->append_log_entry(log_entry);

    next_idx[my_id] = new_log_idx + 1;
    match_idx[my_id] = new_log_idx;

    lock.unlock();
    return std::make_tuple(true, current_term, new_log_idx);

}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    std::unique_lock<std::mutex> lock(mtx);
    DEBUG_LOG("Handle request vote RPC from node %d", args.candidateId);
    RequestVoteReply reply;

    if (args.term < current_term) {
        reply.term = current_term;
        reply.voteGranted = false;
    } else {
        if (args.term > current_term) {
            current_term = args.term;
            role = RaftRole::Follower;
            leader_id = -1;
            voted_for = -1;
            log_storage->update_metadata(current_term, voted_for);
        }
        reply.term = current_term;
        last_heartbeat_from_leader = std::chrono::system_clock::now();
        bool log_up_to_date = args.lastLogTerm > log.back().term || (args.lastLogTerm == log.back().term && args.lastLogIndex >= log.back().index);
        if ((voted_for == -1 || voted_for == args.candidateId) && log_up_to_date) {
            reply.voteGranted = true;
            voted_for = args.candidateId;
            log_storage->update_metadata(current_term, voted_for);
        } else {
            reply.voteGranted = false;
        }
    }

    lock.unlock();
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);
    DEBUG_LOG("Handle request vote reply from node %d", target);
    if (reply.term > current_term) {
        current_term = reply.term;
        role = RaftRole::Follower;
        leader_id = -1;
        voted_for = -1;
        log_storage->update_metadata(current_term, voted_for);
    }

    if (role == RaftRole::Candidate) {
        if (reply.voteGranted && !vote_res[target]) {
            vote_res[target] = true;
            int n_got_votes = std::count(vote_res.begin(), vote_res.end(), true);
            if (n_got_votes > n_nodes / 2) {
                DEBUG_LOG("Node %d becomes leader", my_id);
                role = RaftRole::Leader;
                leader_id = my_id;

                next_idx.resize(n_nodes, log.back().index + 1);
                match_idx.resize(n_nodes, 0);
                match_idx[my_id] = log.back().index;
            }
        }
    }
    lock.unlock();
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    std::unique_lock<std::mutex> lock(mtx);
    last_heartbeat_from_leader = std::chrono::system_clock::now();
    DEBUG_LOG("Handle append entries RPC from node %d", rpc_arg.leaderId);
    AppendEntriesReply reply;

    if (rpc_arg.term < current_term) {
        reply.success = false;
        DEBUG_LOG("Node %d reject append entries from node %d because of arg term mismatch: %d vs %d", my_id, rpc_arg.leaderId, rpc_arg.term, current_term);
    } else {
        if (rpc_arg.term > current_term) {
            current_term = rpc_arg.term;
            role = RaftRole::Follower;
            leader_id = rpc_arg.leaderId;
            voted_for = -1;
            log_storage->update_metadata(current_term, voted_for);
        }

        AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);

        if (arg.entries.empty()) {
            reply.success = true;
            DEBUG_LOG("Node %d receive heartbeat from node %d", my_id, rpc_arg.leaderId);

            if (arg.leaderCommit > commit_idx) {
                commit_idx = std::min(arg.leaderCommit, log.back().index);
            }
        } else if (rpc_arg.prevLogIndex > log.back().index) {
            reply.success = false;
            reply.followerIndex = log.back().index;
            DEBUG_LOG("Node %d reject append entries from node %d because of log index mismatch: %d vs %d", my_id, rpc_arg.leaderId, rpc_arg.prevLogIndex, log.back().index);
        } else if (rpc_arg.prevLogIndex > 0 && log[rpc_arg.prevLogIndex].term != rpc_arg.prevLogTerm) {
            reply.success = false;
            DEBUG_LOG("Node %d reject append entries from node %d because of log term mismatch: %d vs %d", my_id, rpc_arg.leaderId, rpc_arg.prevLogTerm, log[rpc_arg.prevLogIndex].term);
        } else {
            reply.success = true;
                DEBUG_LOG("Node %d accept append entries from node %d", my_id, rpc_arg.leaderId);
                if (arg.prevLogIndex < log.back().index) {
                    DEBUG_LOG("Node %d truncate log from index %d to %d", my_id, arg.prevLogIndex + 1, log.back().index);
                    auto log_size = log.size();
                    log.erase(log.begin() + arg.prevLogIndex + 1, log.end());
                    log_storage->erase_log_entry(arg.prevLogIndex + 1, log_size);
                }
                for (auto entry : arg.entries) {
                    log.push_back(entry);
                    log_storage->append_log_entry(entry);
                }
                printf("Node %d log back index: %d\n", my_id, log.back().index);

            if (arg.leaderCommit > commit_idx) {
                commit_idx = std::min(arg.leaderCommit, log.back().index);
            }
        }
    }

    reply.term = current_term;
    lock.unlock();
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);
    DEBUG_LOG("Handle append entries reply from node %d, success: %d", node_id, reply.success);
    if (reply.term > current_term) {
        current_term = reply.term;
        role = RaftRole::Follower;
        leader_id = -1;
        voted_for = -1;
        log_storage->update_metadata(current_term, voted_for);
    } 
    if (role == RaftRole::Leader) {
        if (reply.success) {
            if (!arg.entries.empty()) {
                next_idx[node_id] = arg.prevLogIndex + arg.entries.size() + 1;
                match_idx[node_id] = next_idx[node_id] - 1;
            }
            
            // If there exists an N such that N>commitIndex, a majority of matchIndex[i]≥N, and log[N].term==currentTerm: set commitIndex=N
            for (int i = commit_idx + 1; i <= log.back().index; i++) {
                int n_match = 0;
                for (int j = 0; j < n_nodes; j++) {
                    if (match_idx[j] >= i) {
                        n_match++;
                    }
                }
                if (n_match > n_nodes / 2 && log[i].term == current_term) {
                    commit_idx = i;
                    break;
                }
            }
        } else {
            DEBUG_LOG("next_idx[%d]: %d, arg.prevLogIndex: %d", node_id, next_idx[node_id], arg.prevLogIndex);
            if (reply.followerIndex != -1) {
                next_idx[node_id] = reply.followerIndex + 1;
            } else {
                next_idx[node_id] = next_idx[node_id] - 1;
            }
            // AppendEntriesArgs<Command> arg2(current_term, my_id, next_idx[node_id] - 1, log[next_idx[node_id] - 1].term, {}, commit_idx);
            // for (int j = next_idx[node_id]; j <= log.back().index; j++) {
            //     arg2.entries.push_back(log[j]);
            // }
            // thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, arg2);
        }
    }
    lock.unlock();
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    return InstallSnapshotReply();
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
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
    while (true) {
        if (is_stopped()) {
            return;
        }
        std::unique_lock<std::mutex> lock(mtx);
        std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
        bool timeout_in_follower = (role == RaftRole::Follower && now - last_heartbeat_from_leader > election_timeout);
        bool timeout_in_candidate = (role == RaftRole::Candidate && now - last_time_become_candidate > candidate_timeout);
        if (timeout_in_follower || timeout_in_candidate) {
            DEBUG_LOG("Node %d start election because of %s", my_id, timeout_in_follower ? "follower timeout" : "candidate timeout");
            long now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            if (timeout_in_follower) {
                long last_heartbeat_from_leader_ms = std::chrono::duration_cast<std::chrono::milliseconds>(last_heartbeat_from_leader.time_since_epoch()).count();
                long election_timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(election_timeout).count();
                DEBUG_LOG("Node %d : now: %ld, last_heartbeat_from_leader: %ld, election_timeout: %ld", 
                    my_id, now_ms, last_heartbeat_from_leader_ms, election_timeout_ms);
            } else {
                long last_time_become_candidate_ms = std::chrono::duration_cast<std::chrono::milliseconds>(last_time_become_candidate.time_since_epoch()).count();
                long candidate_timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(candidate_timeout).count();
                DEBUG_LOG("Node %d : now: %ld, last_time_become_candidate: %ld, candidate_timeout: %ld", 
                    my_id, now_ms, last_time_become_candidate_ms, candidate_timeout_ms);
            }
            role = RaftRole::Candidate;
            current_term++;

            leader_id = -1;
            voted_for = my_id;
            log_storage->update_metadata(current_term, voted_for);
            vote_res.clear();
            vote_res.resize(n_nodes, false);
            vote_res[my_id] = true;

            election_timeout = generate_random_timeout_between(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH);
            candidate_timeout = generate_random_timeout_between(CANDIDATE_TIMEOUT_LOW, CANDIDATE_TIMEOUT_HIGH);

            RequestVoteArgs arg(current_term, my_id, log.back().index, log.back().term);
            for (int i = 0; i < n_nodes; i++) {
                if (i != my_id) {
                    thread_pool->enqueue(&RaftNode::send_request_vote, this, i, arg);
                }
            }

            last_time_become_candidate = std::chrono::system_clock::now();
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) {
            return;
        }
        std::unique_lock<std::mutex> lock(mtx);
        if (role == RaftRole::Leader) {
            for (int i = 0; i < n_nodes; i++) {
                if (i != my_id) {
                    if (next_idx[i] <= log.back().index) {
                        DEBUG_LOG("Node %d send append entries to node %d, next_idx[%d]: %d, log.back().index: %d", my_id, i, i, next_idx[i], log.back().index);
                        AppendEntriesArgs<Command> arg(current_term, my_id, next_idx[i] - 1, log[next_idx[i] - 1].term, {}, commit_idx);
                        for (int j = next_idx[i]; j <= log.back().index; j++) {
                            arg.entries.push_back(log[j]);
                        }
                        thread_pool->enqueue(&RaftNode::send_append_entries, this, i, arg);
                    }
                }
            }
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.
    while (true) {
        if (is_stopped()) {
            return;
        }
        std::unique_lock<std::mutex> lock(mtx);
        if (commit_idx > last_applied) {
            DEBUG_LOG("Node %d apply log from %d to %d", my_id, last_applied + 1, commit_idx);
            for (int i = last_applied + 1; i <= commit_idx; i++) {
                state->apply_log(log[i].command);
                last_applied = i;
            }
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) {
            return;
        }
        std::unique_lock<std::mutex> lock(mtx);
        if (role == RaftRole::Leader) {
            DEBUG_LOG("Node %d send heartbeat, current term: %d", my_id, current_term);
            AppendEntriesArgs<Command> arg(current_term, my_id, 0, 0, {}, commit_idx);
            for (int i = 0; i < n_nodes; i++) {
                if (i != my_id) {
                    arg.prevLogIndex = next_idx[i] - 1;
                    arg.prevLogTerm = log[arg.prevLogIndex].term;
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, i, arg);
                }
            }
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
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