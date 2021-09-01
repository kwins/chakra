//
// Created by kwins on 2021/5/13.
//

#ifndef LIBGOSSIP_PEER_H
#define LIBGOSSIP_PEER_H

#include <string>
#include <list>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include "../net/connect.h"
#include "types.pb.h"
#include <google/protobuf/message.h>
#include <ev++.h>

namespace chakra::cluster{
using namespace std::chrono;

class Peer : public std::enable_shared_from_this<Peer> {
public:
    struct Link{
        explicit Link(int sockfd);
        explicit Link(const std::string& ip, int port, const std::shared_ptr<Peer>& peer);
        static void onPeerRead(ev::io& watcher, int event);
        void startEvRead();
        bool connected() const;
        void close() const;
        ~Link();

        std::shared_ptr<net::Connect> conn;
        std::shared_ptr<ev::io> rio;
        std::shared_ptr<Peer> reletedPeer;
    };

    struct FailReport{
        // 报告目标节点已经下线的节点
        std::shared_ptr<Peer> peer;
        // 最后一次从 node 节点收到下线报告的时间
        // 程序使用这个时间戳来检查下线报告是否过期
        // ms
        long time;
    };

    struct DB{
        std::string name;
        bool memory;
        int shard;
        int shardSize;
        long cached;
    };
    const static uint64_t FLAG_NF = 0;
    const static uint64_t FLAG_MYSELF = (1 << 0);
    const static uint64_t FLAG_HANDSHAKE = (1 << 1);
    const static uint64_t FLAG_MEET = (1 << 2);
    const static uint64_t FLAG_PFAIL = (1 << 3);
    const static uint64_t FLAG_FAIL = (1 <<4 );

public:
    Peer() = default;
    const std::string &getName() const;
    const std::string &getIp() const;
    int getPort() const;
    uint64_t getFg() const;
    uint64_t getEpoch() const;

    void setIp(const std::string& ip);
    void setPort(int port);
    void setName(const std::string& name);
    void setEpoch(uint64_t epoch);
    void setFlag(uint64_t fg);
    void delFlag(uint64_t fd);
    long getFailTime() const;
    void setFailTime(long failTime);
    long getRetryLinkTime() const;
    void setRetryLinkTime(long timeMs);
    void setDB(const std::string& name, const DB& st);
    void removeDB(const std::string& name);
    bool servedDB(const std::string& name);
    const std::unordered_map<std::string, DB>& getPeerDBs() const;

    long getLastPingSend() const;
    void setLastPingSend(long lastPingSend);
    long getLastPongRecv() const;
    void setLastPongRecv(long lastPongRecv);
    long createTimeMs();
    void setCreatTimeMs(long millsec);
    nlohmann::json dumpPeer();

    bool isMyself() const;
    bool isHandShake() const;
    bool isMeet() const;
    bool isPfail() const;
    bool isFail() const;
    bool connected() const;
    bool connect();

    std::string desc();
    void linkFree();
    void addFailReport(const std::shared_ptr<Peer>& sender);
    bool delFailReport(const std::shared_ptr<Peer>& sender);
    size_t cleanFailReport(long timeOutMillSec);

    void sendMsg(::google::protobuf::Message& msg, proto::types::Type type);
    ~Peer();
private:

    // 节点的名字，由 40 个十六进制字符组成
    // 例如 68eef66df23420a5862208ef5b1a7005b806f2ff
    std::string name;
    std::string ip;
    int port = 0;
    uint64_t fg = FLAG_NF;
    long ctime = 0;
    long failTime = 0;
    uint64_t epoch = 0;
    long retryLinkTime = 0; // 重试连接peer时间
    Link* link = nullptr;
    std::unordered_map<std::string,std::shared_ptr<FailReport>> failReports{};

    // <db name,db info>
    std::unordered_map<std::string, DB> dbs{};
    long last_ping_send = 0;
    long last_pong_recv = 0;
};

}



#endif //LIBGOSSIP_PEER_H
