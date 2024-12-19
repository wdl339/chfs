#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
    }

    void Worker::doMap(int index, const std::string &filename) {
        std::string content = get_file_content(chfs_client.get(), filename);
        auto keyVals = Map(content);
        chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, my_task.outputFileName);
        write_to_file(chfs_client.get(), my_task.outputFileName, keyVals);
        doSubmit(MAP, index);
    }

    void Worker::doReduce(int index, int nfiles) {
        std::vector<KeyVal> kvs;
        for (int i = 0; i < nfiles; i++) {
            std::string filename = my_task.files[i];
            std::string content = get_file_content(chfs_client.get(), filename);
            if (content.empty()) {
                continue;
            }
            std::stringstream stringstream(content);
            std::string key, value;
            while (stringstream >> key >> value) {
                kvs.emplace_back(key, value);
            }
        }
        std::vector<KeyVal> res_kvs = sort_and_reduce(kvs);
        chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, my_task.outputFileName);
        write_to_file(chfs_client.get(), my_task.outputFileName, res_kvs);
        doSubmit(REDUCE, index);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        mr_client->call(SUBMIT_TASK, (int)taskType, index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            auto reply = mr_client->call(ASK_TASK, 0);
            if(reply.is_err()){
    			std::this_thread::sleep_for(std::chrono::milliseconds(30));
                continue;
            }
            auto res = reply.unwrap()->as<AskReply>();
            my_task = Task(res);
            if (res.taskType == WAIT) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                continue;
            } else if (res.taskType == MAP) {
                doMap(res.index, res.files[0]);
            } else if (res.taskType == REDUCE) {
                doReduce(res.index, res.files.size());
            } else if (res.taskType == NONE) {
                break;
            }
        }
    }
}