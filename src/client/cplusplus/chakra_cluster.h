#ifndef CLIENT_CHAKRA_CLUSTER_H
#define CLIENT_CHAKRA_CLUSTER_H


#include <array>
#include <client.pb.h>
#include <error/err.h>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include "chakra.h"
#include "chakra_db.h"
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

    /* key 是 peer name value 是 peer connect pool */
    using Clients = std::unordered_map<std::string, std::shared_ptr<client::Chakra>>;
    /* db:key pair list */
    using DBKeyPairList = std::vector<std::pair<std::string, std::string>>;
    /* 指向同一个 DB 所有分片的节点 */
    using DBS = std::unordered_map<std::string, std::shared_ptr<ChakraDB>>;

public:
    // 线程安全
    explicit ChakraCluster(Options opts);
    ~ChakraCluster();
    error::Error connectCluster();
    error::Error meet(const std::string& ip, int port);
    error::Error setdb(const std::string& nodename, const std::string& dbname, int cached);

    error::Error get(const proto::client::GetMessageRequest& request, proto::client::GetMessageResponse& response, bool hash = false);
    error::Error mget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN = 20, bool hash = false);

    error::Error set(const proto::client::SetMessageRequest& request, proto::client::SetMessageResponse& response);
    error::Error mset(const proto::client::MSetMessageRequest& request, proto::client::MSetMessageResponse& response);
    
    error::Error push(const proto::client::PushMessageRequest& request, proto::client::PushMessageResponse& response);
    error::Error mpush(const proto::client::MPushMessageRequest& request, int splitN, proto::client::MPushMessageResponse& response);
    
    error::Error scan(const proto::client::ScanMessageRequest& request, proto::client::ScanMessageResponse& response);

    error::Error incr(const proto::client::IncrMessageRequest& request, proto::client::IncrMessageResponse& response);
    error::Error mincr(const proto::client::MIncrMessageRequest& request, proto::client::MIncrMessageResponse& response);
    void close();
private:
    int next();
    bool clusterChanged();
    error::Error dbnf(const std::string& dbname);
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