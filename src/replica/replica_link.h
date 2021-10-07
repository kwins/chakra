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
#include "net/link.h"

namespace chakra::replica{
class Link {
public:
    enum Type{
        NEGATIVE = 1,
        POSITIVE = 2,
    };
    enum class State{
        CONNECT = 1,       // 准备和 master 建立连接
        CONNECTING,        // 已经和 master 建立连接，未进行通信确认
        RECEIVE_PONG,      // 发送 Ping 到 master ，准备接受 master Pong 确认
        TRANSFOR,          // 已经和master建立连接，并且 PING-PONG 确认连接有效,开始全量同步
        CONNECTED,         // 首次全量同步已经完成，和master连接正常连接，开始正常增量同步，一开始会有增量延迟情况
    };

    struct NegativeOptions{
        int sockfd;
        std::string dir;
    };

    struct PositiveOptions{
        std::string ip;
        int port = 0;
        std::string dir;
        std::string dbName;
    };

    explicit Link(const NegativeOptions& options);
    explicit Link(const PositiveOptions& options);
    // 开始接收命令
    void startReplicaRecv();
    // 处理命令
    static void onProcessRecv(ev::io& watcher, int event);
    // 创建db snapshot, 为后续传输 db 做准备
    utils::Error snapshotBulk(const std::string& dbname);
    // 触发全量复制
    void startSendBulk();
    // 执行全量复制
    void onSendBulk(ev::timer& watcher, int event);
    // 触发增量拉取
    void startPullDelta();
    void onPullDelta(ev::timer& watcher, int event);

    void connectPrimary();
    void replicaEventHandler();
    void deConnectPrimary();
    void abortTransfer();
    void heartBeat();
    utils::Error tryPartialReSync();

    nlohmann::json dumpLink();
    bool isTimeout() const;
    const std::string &getPeerName() const;
    void setPeerName(const std::string &name);
    State getState() const;
    const std::string &getIp() const;
    void setIp(const std::string &ip);
    int getPort() const;
    void setPort(int port);
    void setState(State state);
    int64_t getRocksSeq() const;
    void setRocksSeq(int64_t seq);
    const std::string &getDbName() const;
    void setDbName(const std::string &dbName);
    long getLastTransferMs() const;
    void setLastTransferMs(long lastTransferMs);
    long getLastInteractionMs() const;
    void setLastInteractionMs(long lastInteractionMs);
    utils::Error sendMsg(::google::protobuf::Message& msg, proto::types::Type type);
    void close();
    ~Link();

private:
    std::shared_ptr<net::Connect> conn = nullptr;
    ev::io rio;
    ev::timer deltaIO;
    std::string dir;
    // 同步缓存数据，启动时会从文件加载
    std::string peerName;
    std::string dbName;
    std::string ip;
    int port = 0;
    long lastInteractionMs = 0;
    long lastTransferMs = 0;
    State state = State::CONNECT;
    Type type;
    // 全量同步使用
    rocksdb::Iterator* bulkiter = nullptr;
    int64_t deltaSeq = -1;
    ev::timer transferIO;
    static const int BULK_LEN = 1024 * 4;
    static const int DELTA_LEN = 1024 * 4;
};

}



#endif //CHAKRA_REPLICA_LINK_H
