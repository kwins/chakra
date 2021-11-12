//
// Created by kwins on 2021/5/13.
//

#ifndef LIBGOSSIP_PEER_H
#define LIBGOSSIP_PEER_H

#include <string>
#include <list>
#include <unordered_map>
#include "net/connect.h"
#include "types.pb.h"
#include <google/protobuf/message.h>
#include <ev++.h>
#include "peer.pb.h"
#include "net/link.h"
#include "error/err.h"

namespace chakra::cluster{
using namespace std::chrono;

class Peer : public std::enable_shared_from_this<Peer> {
public:
    struct Link : public chakra::net::Link{
        explicit Link(int sockfd);
        explicit Link(const std::string& ip, int port, const std::shared_ptr<Peer>& peer);
        void onPeerRead(ev::io& watcher, int event);
        void startEvRead();
        ~Link();
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

    const static uint64_t FLAG_NF = 0;
    const static uint64_t FLAG_MYSELF = (1 << 0);
    const static uint64_t FLAG_HANDSHAKE = (1 << 1);
    const static uint64_t FLAG_MEET = (1 << 2);
    const static uint64_t FLAG_PFAIL = (1 << 3);
    const static uint64_t FLAG_FAIL = (1 << 4);
//    const static uint64_t FLAG_SUCCESS = (1 << 5);

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
    void setFailTime(long failtime);
    long getRetryLinkTime() const;
    void setRetryLinkTime(long timeMs);
    void updateMetaDB(const std::string& dbname, const proto::peer::MetaDB& st);
    void removeMetaDB(const std::string& dbname);
    bool servedDB(const std::string& dbname);
    const std::unordered_map<std::string, proto::peer::MetaDB>& getPeerDBs() const;

    long getLastPingSend() const;
    void setLastPingSend(long lastPingSend);
    long getLastPongRecv() const;
    void setLastPongRecv(long lastPongRecv);
    long createTimeMs() const;
    void setCreatTimeMs(long millsec);
    void dumpPeer(proto::peer::PeerState& peerState);
    bool isMyself() const;
    static bool isMyself(uint64_t flag);
    bool isHandShake() const;
    static bool isHandShake(uint64_t flag);
    bool isMeet() const;
    static bool isMeet(uint64_t flag);
    bool isPfail() const;
    static bool isPfail(uint64_t flag);
    bool isFail() const;
    static bool isFail(uint64_t flag);
    bool connected() const;
    bool connect();

    std::string desc();
    void linkFree();
    void addFailReport(const std::shared_ptr<Peer>& sender);
    bool delFailReport(const std::shared_ptr<Peer>& sender);
    size_t cleanFailReport(long timeOutMillSec);
    void updateSelf(const proto::peer::GossipSender& sender);
    error::Error sendMsg(::google::protobuf::Message& msg, proto::types::Type type);
    void stateDesc(proto::peer::PeerState& peerState);
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
    long retryLinkTime = 0; // 首次重试连接peer时间
//    std::shared_ptr<Peer::Link> link = nullptr;
    Link* link = nullptr;
    std::unordered_map<std::string,std::shared_ptr<FailReport>> failReports{};

    // <db name,db info>
    std::unordered_map<std::string, proto::peer::MetaDB> dbs{};
    long last_ping_send = 0;
    long last_pong_recv = 0;
};

}



#endif //LIBGOSSIP_PEER_H
