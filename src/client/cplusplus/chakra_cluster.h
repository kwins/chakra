#ifndef CLIENT_CHAKRA_CLUSTER_H
#define CLIENT_CHAKRA_CLUSTER_H


#include <array>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "chakra.h"
#include "peer.pb.h"

namespace chakra::client {

class ChakraCluster {
public:
    struct Options {
        std::string ip;
        int port = 7290;
        int maxIdleConns = 3; /* 初始化连接数 */
        int maxConns = 10; /* 最大连接数 */
        std::chrono::milliseconds connectTimeOut { 100 };
        std::chrono::milliseconds readTimeOut { 100 };
        std::chrono::milliseconds writeTimeOut { 100 };
        std::chrono::milliseconds backupUpdateMs { 1000 }; /* 集群状态更新间隔 */
    };
public:
    /* key 是 peer name value 是 peer connect pool */
    using Clients = std::unordered_map<std::string, std::shared_ptr<client::Chakra>>;
    using DBS = std::unordered_map<std::string, std::vector<std::shared_ptr<client::Chakra>>>;
public:
    explicit ChakraCluster(Options opts);
    ~ChakraCluster();
    error::Error connectCluster();
    error::Error meet(const std::string& ip, int port);
    void close();

private:
    int next();
    bool clusterChanged();
    Options options;
    std::atomic<int> index;
    std::array<Clients, 2> clusterConns; /* 维护所有节点的连接 */
    std::array<DBS, 2> dbConns; /* 节点db对应的连接 */
    std::array<proto::peer::ClusterState, 2> clusterStates;
    std::shared_ptr<client::Chakra> client;
    std::shared_ptr<std::thread> th;
    std::atomic_bool exitTH;
    std::mutex mutex = {};
    std::condition_variable cond = {};
};

}

#endif // CLIENT_CHAKRA_CLUSTER_H