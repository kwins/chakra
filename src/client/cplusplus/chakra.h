//
// Created by kwins on 2021/6/4.
//

#ifndef CLIENT_CHAKRA_H
#define CLIENT_CHAKRA_H
#include "net/connect.h"

#include <cstdint>
#include <mutex>
#include <deque>
#include <chrono>
#include "peer.pb.h"
#include "types.pb.h"
#include "error/err.h"
#include "element.pb.h"

namespace chakra::client {

class Chakra {
public:
    struct Options {
        std::string name; /* 节点名称 */
        std::string ip;
        int port = 7290;
        int maxIdleConns = 3; /* 初始化连接数 */
        int maxConns = 10; /* 最大连接数 */
        std::chrono::milliseconds connectTimeOut { 100 };
        std::chrono::milliseconds readTimeOut { 100 };
        std::chrono::milliseconds writeTimeOut { 100 };
    };
public:
    explicit Chakra(Options opts);
    error::Error connect();
    std::string peerName() const;
    error::Error meet(const std::string& ip, int port);
    
    error::Error set(const std::string& dbname, const std::string& key, const std::string& value, int64_t ttl = 0);
    error::Error set(const std::string& dbname, const std::string& key, int64_t value, int64_t ttl = 0);
    error::Error set(const std::string& dbname, const std::string& key, float value, int64_t ttl = 0);

    error::Error get(const std::string& dbname, const std::string& key, proto::element::Element& element);
    
    error::Error setdb(const std::string& dbname, int cached);
    error::Error state(proto::peer::ClusterState& clusterState);
    error::Error setEpoch(int64_t epoch, bool increasing);
    void close();

private:
    Options options;
    std::shared_ptr<net::Connect> connnectGet();
    void connectBack(std::shared_ptr<net::Connect> conn);
    error::Error executeCmd(::google::protobuf::Message& msg, proto::types::Type type, ::google::protobuf::Message& reply);
    std::deque<std::shared_ptr<net::Connect>> conns;
    std::mutex mutex;
};

}




#endif // CLIENT_CHAKRA_H
