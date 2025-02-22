#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

template <typename Command>
class RaftLogEntry {
public:
    int term;
    int index;
    Command command;

    RaftLogEntry(int term, int index, Command command)
        : term(term), index(index), command(command) {}
};

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {

#define METADATA_BLOCK_ID 2
#define SNAPSHOT_BLOCK_ID 3
#define LOG_BLOCK_BEGIN_POS 4
#define BLOCK_SIZE KDefaultBlockCnt

public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();
    
    void recover();
    void init_all(int c, int v, std::vector<RaftLogEntry<Command>> l, std::vector<u8> s);
    void append_log_entry(RaftLogEntry<Command> entry);
    void erase_log_entry(int begin, int end);
    void update_metadata(int term, int vote);
    void save_snapshot(std::vector<u8> s);

    int has_log;
    int n_log_entries;
    int n_snapshot_bytes;
    
    int current_term;
    int voted_for;

    std::vector<RaftLogEntry<Command>> log;
    std::vector<u8> snapshot;

    int my_id = -1;

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;

    void save_metadata();
    void get_metadata();

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    bm_ = bm;
}

template <typename Command>
RaftLog<Command>::~RaftLog() {}

template <typename Command>
void RaftLog<Command>::recover()
{
    std::unique_lock<std::mutex> lock(mtx);
    get_metadata();
    if (has_log == 0) {
        lock.unlock();
        return;
    }

    log.clear();
    // printf("my_id:%d, recover, n_log_entries: %d\n", my_id, n_log_entries);
    for (int i = 0; i < n_log_entries; i++) {
        std::vector<u8> log_block(BLOCK_SIZE);
        bm_->read_block(LOG_BLOCK_BEGIN_POS + i, log_block.data());
        int term, index;
        memcpy(&term, log_block.data(), sizeof(int));
        memcpy(&index, log_block.data() + sizeof(int), sizeof(int));
        Command cmd;
        std::vector<u8> cmd_data(log_block.data() + 2 * sizeof(int), log_block.data() + BLOCK_SIZE);
        cmd.deserialize(cmd_data, cmd.size());
        log.push_back(RaftLogEntry<Command>(term, index, cmd));
    }

    std::vector<u8> snapshot_block(BLOCK_SIZE);
    bm_->read_block(SNAPSHOT_BLOCK_ID, snapshot_block.data());
    memcpy(&n_snapshot_bytes, snapshot_block.data(), sizeof(int));
    snapshot.clear();
    snapshot.insert(snapshot.end(), snapshot_block.data() + sizeof(int), snapshot_block.data() + sizeof(int) + n_snapshot_bytes);
    // printf("my_id:%d, recover, log size: %zu, log content: %d\n", my_id, log.size(), log[0].command.value);
    lock.unlock();
}

template <typename Command>
void RaftLog<Command>::init_all(int c, int v, std::vector<RaftLogEntry<Command>> l, std::vector<u8> s) 
{
    std::unique_lock<std::mutex> lock(mtx);
    has_log = 1;
    n_log_entries = 0;
    current_term = c;
    voted_for = v;
    log = l;
    save_metadata();
    for (int i = 0; i < l.size(); i++){
        lock.unlock();
        append_log_entry(l[i]);
        lock.lock();
    }
    lock.unlock();
    save_snapshot(s);
}

template <typename Command>
void RaftLog<Command>::append_log_entry(RaftLogEntry<Command> entry)
{
    has_log = 1;
    std::unique_lock<std::mutex> lock(mtx);
    log.push_back(entry);
    std::vector<u8> log_block(BLOCK_SIZE);
    memcpy(log_block.data(), &entry.term, sizeof(int));
    memcpy(log_block.data() + sizeof(int), &entry.index, sizeof(int));
    std::vector<u8> cmd_data = entry.command.serialize(entry.command.size());
    memcpy(log_block.data() + 2 * sizeof(int), cmd_data.data(), cmd_data.size());
    bm_->write_block(LOG_BLOCK_BEGIN_POS + n_log_entries, log_block.data());
    n_log_entries++;
    // printf("my_id:%d, n_log_entries: %d, cmd content: %d, entry term: %d, entry index: %d\n", my_id, n_log_entries, entry.command.value, entry.term, entry.index);
    save_metadata();
    lock.unlock();
}

template <typename Command>
void RaftLog<Command>::erase_log_entry(int begin, int end)
{
    has_log = 1;
    std::unique_lock<std::mutex> lock(mtx);
    // printf("my_id:%d, erase_log_entry: %d, %d\n", my_id, begin, end);
    if (begin == 0) {
        for (int i = end + 1; i < n_log_entries + 1; i++) {
            std::vector<u8> log_block(BLOCK_SIZE);
            memcpy(log_block.data(), &log[i].term, sizeof(int));
            memcpy(log_block.data() + sizeof(int), &log[i].index, sizeof(int));
            std::vector<u8> cmd_data = log[i].command.serialize(log[i].command.size());
            memcpy(log_block.data() + 2 * sizeof(int), cmd_data.data(), cmd_data.size());
            // printf("my_id:%d, remain log content: %d, log term: %d, log index: %d\n", my_id, log[i].command.value, log[i].term, log[i].index);
            bm_->write_block(LOG_BLOCK_BEGIN_POS + (i - 1 - end), log_block.data());
        }
        n_log_entries -= (end - begin);
        log.erase(log.begin() + begin, log.begin() + end);
    } else {
        n_log_entries -= (end - begin);
        log.erase(log.begin() + begin, log.begin() + end);
        for (int i = begin; i < end; i++) {
            bm_->zero_block(LOG_BLOCK_BEGIN_POS + i);
        }
    }
    save_metadata();
    lock.unlock();
}

template <typename Command>
void RaftLog<Command>::update_metadata(int term, int vote)
{
    has_log = 1;
    std::unique_lock<std::mutex> lock(mtx);
    current_term = term;
    voted_for = vote;
    save_metadata();
    lock.unlock();
}

template <typename Command>
void RaftLog<Command>::save_snapshot(std::vector<u8> s)
{
    std::unique_lock<std::mutex> lock(mtx);
    snapshot = s;
    std::vector<u8> snapshot_block(BLOCK_SIZE);
    n_snapshot_bytes = s.size();
    memcpy(snapshot_block.data(), &n_snapshot_bytes, sizeof(int));
    memcpy(snapshot_block.data() + sizeof(int), s.data(), s.size());
    bm_->write_block(SNAPSHOT_BLOCK_ID, snapshot_block.data());
    lock.unlock();
}

template <typename Command>
void RaftLog<Command>::save_metadata()
{
    has_log = 1;
    std::vector<u8> meta_block(BLOCK_SIZE);
    // save has_log, n_log_entries, current_term, voted_for
    memcpy(meta_block.data(), &has_log, sizeof(int));
    memcpy(meta_block.data() + sizeof(int), &n_log_entries, sizeof(int));
    memcpy(meta_block.data() + 2 * sizeof(int), &current_term, sizeof(int));
    memcpy(meta_block.data() + 3 * sizeof(int), &voted_for, sizeof(int));
    bm_->write_block(METADATA_BLOCK_ID, meta_block.data());
}

template <typename Command>
void RaftLog<Command>::get_metadata()
{
    std::vector<u8> meta_block(BLOCK_SIZE);
    bm_->read_block(METADATA_BLOCK_ID, meta_block.data());
    memcpy(&has_log, meta_block.data(), sizeof(int));
    memcpy(&n_log_entries, meta_block.data() + sizeof(int), sizeof(int));
    memcpy(&current_term, meta_block.data() + 2 * sizeof(int), sizeof(int));
    memcpy(&voted_for, meta_block.data() + 3 * sizeof(int), sizeof(int));
}


} /* namespace chfs */
