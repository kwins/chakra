//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_REPLICA_H
#define CHAKRA_REPLICA_H

#include <ev++.h>
#include <string>
#include <vector>
#include <array>
#include <list>
#include <unordered_map>
#include <fstream>
#include <google/protobuf/message.h>
#include <rocksdb/db.h>

#include "net/connect.h"
#include "net/link.h"
#include "types.pb.h"
#include "utils/nocopy.h"
#include "error/err.h"
#include "replica.pb.h"

namespace chakra::replica {

class Replicate : public utils::UnCopyable {
public:
    class Link : public chakra::net::Link , std::enable_shared_from_this<Link>{
    public:
        enum Type {
            NEGATIVE = 1,
            POSITIVE = 2,
        };

        enum class State {
            CONNECT = 1,       // 初始连接
            CONNECTING,        // socket建立完成，准备接收或发送PING
            CONNECTED,         // 首次全量同步已经完成

            REPLICA_INIT,              // ReplicateDB INIT
            REPLICA_TRANSFORING,       // 全量同步中
            REPLICA_TRANSFORED,        // 全量同步完成
        };

        struct ReplicateDB { /* 复制的DB信息*/
            void startPullDelta(); // 触发拉取增量数据
            void onPullDelta(ev::timer& watcher, int event); // 执行增量拉取

            void startSendBulk(); // 触发全量复制
            void onSendBulk(ev::timer& watcher, int event); // 执行全量复制

            void reset(); // 清空所有成员配置
            void close(); // 关闭当前正在进行的任务，成员信息保持不变
            ~ReplicateDB();

            std::string name; /* DB name */
            long lastTransferMs = 0;
            long lastTryReSyncMs = 0;
            State state = State::REPLICA_INIT; /* 复制状态 */
            rocksdb::SequenceNumber deltaSeq = 0; /* 增量同步使用 */

            ev::timer deltaIO;
            ev::timer transferIO;
            rocksdb::Iterator* bulkiter = nullptr; /* 全量同步使用 */
            Link* link = nullptr;
        };
        
        struct NegativeOptions {
            int sockfd;
        };

        struct PositiveOptions {
            std::string ip;
            int port = 0;
            bool connect = true; /* 是否立即连接 */
        };

        explicit Link(const PositiveOptions& options);
        explicit Link(const NegativeOptions& options);
        
        // 创建db snapshot, 为后续传输 db 做准备
        error::Error snapshotDB(const std::string& name, rocksdb::SequenceNumber& lastSeq);

        void startReplicateRecvMsg(); // 开始接收命令
        void onReplicateRecvMsg(ev::io& watcher, int event);

        void handshake();
        void heartbeat();
        void reconnect();

        void tryPartialReSync(const std::string& name);

        void startPullDelta(const std::string& name); // 触发拉取增量数据
        void startSendBulk(const std::string& name); // 触发全量复制

        void setRocksSeq(const std::string& name, int64_t seq);
        // void setReplicate
        void close();
        bool isTimeout() const;

        /* 针对这个连接上所有DB定时检查是否有事件需要处理 */
        void replicateEventLoop();
        void replicaEvent(const std::string& name);
        void replicateLinkEvent();

        void replicateState(proto::replica::ReplicaStates& rs);
        void setReplicateDB(std::shared_ptr<ReplicateDB> relicateDB);
        std::shared_ptr<ReplicateDB> getReplicateDB(const std::string& name);
        const std::unordered_map<std::string, std::shared_ptr<ReplicateDB>>& getReplicas();
        void setPeerName(const std::string& name);
        std::string getPeerName() const;
        void setLastInteractionMs(int64_t ms);
        int64_t getLastInteractionMs() const;
        void setLastTransferMs(const std::string& name, int64_t ms);
        int64_t getLastTransferMs(const std::string& name);
        State getState() const;
        void setState(State st);
        std::string getIp() const;
        void setIp(const std::string& v);
        int getPort() const;
        void setPort(int p);
        
    private:  
        std::string ip;
        int port = 0;
        long lastInteractionMs = 0;
        Type type;
        State state; /* 网络连接状态 */
        std::string peerName;
        /* 当前连接上复制的所有DB，key是 DB 名称，value 是DB的信息 */
        std::unordered_map<std::string, std::shared_ptr<ReplicateDB>> replicas;
    };

public:
    Replicate();
    void onAccept(ev::io& watcher, int event);
    static std::shared_ptr<Replicate> get();
    // 检查当前节点是否已经复制了节点为 peername DB为 dbname的数据
    bool replicatedDB(const std::string& peername, const std::string &dbname);
    error::Error setReplicateDB(const std::string& peername, const std::string &dbname, const std::string& ip, int port);
    void startReplicaCron();
    void onReplicaCron(ev::timer& watcher, int event);
    void dumpReplicateStates();
    
    // 获取state状态下，所有db以及db对应的所有连接
    // 用于检查新副本复制是否已经完成　
    std::unordered_map<std::string, std::vector<Link*>> dbLinks(chakra::replica::Replicate::Link::State state);
    std::unordered_map<std::string, std::vector<Link*>> dbTransferedLinks();
    void stop();

private:
    void loadLastStateDB();
    uint64_t cronLoops = 0;
    ev::io replicaio;
    ev::timer cronIO;
    ev::timer transferIO;
    int sfd = -1;
    std::unordered_map<std::string, Link*> positiveLinks; // key=peername
    std::list<Link*> negativeLinks;
    static const std::string REPLICA_FILE_NAME;
};

}



#endif //CHAKRA_REPLICA_H
