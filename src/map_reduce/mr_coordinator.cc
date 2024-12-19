#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    AskReply Coordinator::askTask(int) {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        if (this->current_stage == MAP_STAGE) {
            for (Task &task : this->map_tasks) {
                if (!task.isAssigned) {
                    printTime("map task " + std::to_string(task.index) + " assign time");
                    task.isAssigned = true;
                    return AskReply(task.taskType, task.index, task.outputFileName, task.files);
                }
            }
            return AskReply(WAIT, 0, "", {});
        } else if (this->current_stage == REDUCE_STAGE) {
            for (Task &task : this->reduce_tasks) {
                if (!task.isAssigned) {
                    printTime("reduce task " + std::to_string(task.index) + " assign time");
                    task.isAssigned = true;
                    return AskReply(task.taskType, task.index, task.outputFileName, task.files);
                }
            }
            return AskReply(WAIT, 0, "", {});
        } else if (this->current_stage == FINAL_REDUCE) {
            Task* task = &this->final_reduce_task;
            if (!task->isAssigned) {
                printTime("final reduce task assign time");
                task->isAssigned = true;
                return AskReply(task->taskType, task->index, task->outputFileName, task->files);
            }
            return AskReply(NONE, 0, "", {});
        }
        return AskReply(WAIT, 0, "", {});
    }

    int Coordinator::submitTask(int taskType, int index) {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        if (taskType == MAP) {
            if (this->current_stage != MAP_STAGE) {
                return 0;
            }
            printTime("map task " + std::to_string(index) + " submit time");
            this->map_tasks[index].isSubmitted = true;
            for (Task &task : this->map_tasks) {
                if (!task.isSubmitted) {
                    return 0;
                }
            }
            this->current_stage = REDUCE_STAGE;
        } else if (taskType == REDUCE) {
            if (this->current_stage == REDUCE_STAGE) {
                printTime("reduce task " + std::to_string(index) + " submit time");
                this->reduce_tasks[index].isSubmitted = true;
                for (Task &task : this->reduce_tasks) {
                    if (!task.isSubmitted) {
                        return 0;
                    }
                }
                this->current_stage = FINAL_REDUCE;
            } else if (this->current_stage == FINAL_REDUCE) {
                if (this->final_reduce_task.isSubmitted || index != -1) {
                    return 0;
                } 
                printTime("final reduce task submit time");
                this->final_reduce_task.isSubmitted = true;
                this->isFinished = true;
            }
        }
        return 1;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);

        this->start_time = std::chrono::system_clock::now();
        this->debug_enabled = false;
        this->n_files = files.size();
        this->current_stage = MAP_STAGE;
        for (int i = 0; i < n_files; i++) {
            Task task = Task(MAP, i, "if" + std::to_string(i), {files[i]});
            this->map_tasks.push_back(task);
        }

        this->n_reduce = nReduce;
        std::vector<std::vector<std::string>> reduce_files;
        for (int i = 0; i < n_reduce; i++) {
            reduce_files.push_back({});
        }

        for (int i = 0; i < n_files; i++) {
            int reduce_index = i % n_reduce;
            reduce_files[reduce_index].push_back("if" + std::to_string(i));
        }

        std::vector<std::string> final_files;
        for (int i = 0; i < n_reduce; i++) {
            Task task = Task(REDUCE, i, "rf" + std::to_string(i), reduce_files[i]);
            this->reduce_tasks.push_back(task);
            final_files.push_back("rf" + std::to_string(i));
        }
        
        this->final_reduce_task = Task(FINAL_REDUCE, -1, config.resultFile, final_files);
        printTime("init time");
    }

    void Coordinator::printTime(std::string prefix) {
        if (this->debug_enabled) {
            auto end_time = std::chrono::system_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            printf("%s: %ld\n", prefix.c_str(), duration.count());
        }
    }
}