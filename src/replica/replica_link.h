//
// Created by kwins on 2021/6/30.
//

#ifndef CHAKRA_REPLICA_LINK_H
#define CHAKRA_REPLICA_LINK_H

#include <string>
#include <ev++.h>
#include <google/protobuf/message.h>
#include "types.pb.h"
#include "net/connect.h"
#include <nlohmann/json.hpp>
#include <rocksdb/db.h>

namespace chakra::replica{
class Link {
public:
    enum class State{
        CONNECT = 1,       // 准备和 master 建立连接
        CONNECTING,        // 已经和 master 建立连接，未进行通信确认
        RECEIVE_PONG,      // 发送 Ping 到 master ，准备接受 master Pong 确认
        TRANSFOR,          // 已经和master建立连接，并且 PING-PONG 确认连接有效,开始全量同步
        CONNECTED,         // 首次全量同步已经完成，和master连接正常连接，开始正常增量同步，一开始会有增量延迟情况
    };

    struct Options{
        int sockfd;
        std::string dir;
        std::string dbName;
        std::string ip;
        int port = 0;
    };

    Link() = default;
    explicit Link(Options options);
    // 开始接收命令
    void startReplicaRecv();
    // 处理命令
    static void onProcessRecv(ev::io& watcher, int event);
    // 触发全量复制
    void startSendBulk(rocksdb::Iterator* iter , rocksdb::SequenceNumber seq);
    // 执行全量复制
    void onSendBulk(ev::io& watcher, int event);
    // 触发接收全量数据
    void startRecvBulk();
    // 执行接收全量数据同步
    void onRecvBulk(ev::io& watcher, int event);
    // 触发增量拉取
    void startPullDelta();
    // 执行增量拉取
    void onPullDelta(ev::timer& watcher, int event);

    void connectPrimary();
    void onHandshake(ev::io& watcher, int event);
    void replicaEventLoop();
    void deConnectPrimary();
    void abortTransfer();
    void heartBeat() const;
    bool tryPartialReSync();

    nlohmann::json dumpLink();
    void loadLink(const nlohmann::json& j);
    bool isTimeout() const;

    const std::string &getPrimaryName() const;
    void setPrimaryName(const std::string &name);
    State getState() const;
    const std::string &getIp() const;
    void setIp(const std::string &ip);
    int getPort() const;
    void setPort(int port);
    void setState(State state);
    const Options& getOpts();
    int64_t getRocksSeq() const;
    void setRocksSeq(int64_t seq);
    const std::string &getDbName() const;
    void setDbName(const std::string &dbName);
    long getLastTransferMs() const;
    void setLastTransferMs(long lastTransferMs);
    long getLastInteractionMs() const;
    void setLastInteractionMs(long lastInteractionMs);
    utils::Error sendSyncMsg(::google::protobuf::Message& msg, proto::types::Type type,
                     ::google::protobuf::Message& reply);
    void close();
private:
    std::shared_ptr<net::Connect> conn = nullptr;
    Options opts = {};
    ev::io replior;
    ev::io repliow;
    long cronLoops = 0;
    std::string dir;

    // 同步缓存数据，启动时会从文件加载
    std::string primary;
    std::string dbName;
    std::string ip;
    int port = 0;
    long lastInteractionMs = 0;
    long lastTransferMs = 0;
    State state = State::CONNECT;

    // 全量同步使用
    rocksdb::Iterator* bulkiter = nullptr;
    int64_t deltaSeq = -1;
    ev::io transferIO;
    static const int TRANSFER_LEN = 1024 * 16;
    static const int DELTA_LEN = 1024 * 4;
    // 定时拉取delta数据
    ev::timer deltaIO;
};

}



#endif //CHAKRA_REPLICA_LINK_H
