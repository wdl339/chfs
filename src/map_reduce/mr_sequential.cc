#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
    }

    void SequentialMapReduce::doWork() {
        std::vector<KeyVal> kvs;
        for (const std::string &file : files) {
            std::string content = get_file_content(chfs_client.get(), file);
            if (content.empty()) {
                continue;
            }
            auto keyVals = Map(content);
            kvs.insert(kvs.end(), keyVals.begin(), keyVals.end());
        }
        std::vector<KeyVal> res_kvs = sort_and_reduce(kvs);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        write_to_file(chfs_client.get(), outPutFile, res_kvs);
    }
}