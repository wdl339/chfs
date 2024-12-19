#include <string>
#include <utility>
#include <vector>
#include <algorithm>

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
            auto res_lookup = chfs_client->lookup(1, file);
            if (res_lookup.is_err()) {
                printf("file %s not found\n", file.c_str());
                continue;
            }
            auto inode_id = res_lookup.unwrap();
            auto res_get_type = chfs_client->get_type_attr(inode_id);
            if (res_get_type.is_err()) {
                printf("get type attr of file %s failed\n", file.c_str());
                continue;
            }
            auto size = res_get_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, size);
            if (res_read.is_err()) {
                printf("read file %s failed\n", file.c_str());
                continue;
            }
            auto read_vec = res_read.unwrap();
            std::string content(read_vec.begin(), read_vec.end());
            auto keyVals = Map(content);
            kvs.insert(kvs.end(), keyVals.begin(), keyVals.end());
        }

        std::sort(kvs.begin(), kvs.end(), [](const KeyVal &a, const KeyVal &b) {
            return a.key < b.key;
        });

        std::vector<std::string> values;
        std::vector<KeyVal> res_kvs;

        std::string key = kvs[0].key;
        for (const KeyVal &kv : kvs) {
            if (kv.key == key) {
                values.push_back(kv.val);
            } else {
                std::string result = Reduce(key, values);
                res_kvs.emplace_back(key, result);
                key = kv.key;
                values.clear();
                values.push_back(kv.val);
            }
        }

        std::string result = Reduce(key, values);
        res_kvs.emplace_back(key, result);

        std::string res_content;
        for (const KeyVal &kv : res_kvs) {
            res_content += kv.key + " " + kv.val + "\n";
        }
        std::vector<chfs::u8> data(res_content.begin(), res_content.end());
        auto res_lookup = chfs_client->lookup(1, outPutFile);
        if (res_lookup.is_err()) {
            printf("output file not found\n");
            return;
        }
        auto output_inode_id = res_lookup.unwrap();
        auto res_write = chfs_client->write_file(output_inode_id, 0, data);
        if (res_write.is_err()) {
            printf("write file failed\n");
        }
    }
}