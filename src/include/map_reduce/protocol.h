#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

//Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal(){}
        std::string key;
        std::string val;
    };

    std::string get_file_content(chfs::ChfsClient *chfs_client, const std::string &filename);
    void write_to_file(chfs::ChfsClient *chfs_client, const std::string &filename, std::vector<KeyVal> res_kvs);
    std::vector<KeyVal> sort_and_reduce(std::vector<KeyVal> &kvs);

    enum mr_tasktype {
        NONE = 0,
        MAP,
        REDUCE,
        WAIT
    };

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };


    struct AskReply {
        int taskType;
        int index;
        std::string outputFileName;
        std::vector<std::string> files;

        AskReply() : taskType(0), index(0) {}

        AskReply(int taskType, int index, std::string outputFileName, std::vector<std::string> files) :
                taskType(taskType), index(index), outputFileName(std::move(outputFileName)), files(std::move(files)) {}

        MSGPACK_DEFINE(
            taskType,
            index,
            outputFileName,
            files
        )
    };

    class Task {
    public:
        int taskType;
        int index;
        std::string outputFileName;
        std::vector<std::string> files;

        bool isAssigned;
        bool isSubmitted;

        Task(AskReply reply) : taskType(reply.taskType), index(reply.index), outputFileName(reply.outputFileName),
                                   files(reply.files), isAssigned(false), isSubmitted(false) {}

        Task() : taskType(0), index(0), isAssigned(false), isSubmitted(false) {}

        Task(int taskType, int index, std::string outputFileName, std::vector<std::string> files) :
                taskType(taskType), index(index), outputFileName(std::move(outputFileName)), files(std::move(files)),
                isAssigned(false), isSubmitted(false) {}
    };

    enum stage {
        MAP_STAGE,
        REDUCE_STAGE,
        FINAL_REDUCE
    };

    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        AskReply askTask(int);
        int submitTask(int taskType, int index);
        bool Done();

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;

        int n_files;
        std::vector<Task> map_tasks;
        int n_reduce;
        std::vector<Task> reduce_tasks;
        Task final_reduce_task;
        
        stage current_stage;
        std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
        bool debug_enabled = false;

        void printTime(std::string prefix);
    };

    class Worker {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles);
        void doSubmit(mr_tasktype taskType, int index);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;

        Task my_task;
    };
}