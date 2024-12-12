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
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    /* Lab3: Your code here */

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    bm_ = bm;
}

template <typename Command>
RaftLog<Command>::~RaftLog() {}

/* Lab3: Your code here */

} /* namespace chfs */
