//
// Created by kwins on 2021/6/30.
//

#include "replica_link.h"
#include <glog/logging.h>
#include "net/packet.h"
#include "cmds/command_pool.h"
#include "utils/basic.h"
#include "replica.pb.h"
#include <chrono>
#include "utils/file_helper.h"
#include "database/db_family.h"
#include <rocksdb/db.h>
#include "cluster/cluster.h"

DECLARE_int32(replica_timeout_ms);
DECLARE_int64(connect_buff_size);
DECLARE_double(replica_delta_pull_interval_sec);
DECLARE_double(replica_bulk_send_interval_sec);

chakra::replica::Link::Link(const chakra::replica::Link::NegativeOptions& options) {
    setState(State::CONNECT);
    type = NEGATIVE;
    dir = options.dir;
    setLastTransferMs(utils::Basic::getNowMillSec());
    setLastInteractionMs(utils::Basic::getNowMillSec());
    conn = std::make_shared<net::Connect>(net::Connect::Options{ .fd = options.sockfd });
    startReplicaRecv();
}

chakra::replica::Link::Link(const chakra::replica::Link::PositiveOptions& options){
    setState(State::CONNECT);
    type = POSITIVE;
    dir = options.dir;
    setDbName(options.dbName);
    setIp(options.ip);
    setPort(options.port);
    setLastTransferMs(utils::Basic::getNowMillSec());
    setLastInteractionMs(utils::Basic::getNowMillSec());
}

void chakra::replica::Link::onProcessRecv(ev::io &watcher, int event) {
    auto link = static_cast<chakra::replica::Link*>(watcher.data);
    auto err = link->conn->receivePack([link](char *req, size_t reqLen) {

        proto::types::Type msgType = chakra::net::Packet::getType(req, reqLen);
        DLOG(INFO) << "-- REPL received message type "
                   << proto::types::Type_Name(msgType) << ":" << msgType
                   << " FROM " << link->getPeerName();
        auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);

        error::Error err;
        cmdsptr->execute(req, reqLen, link, [link, &err](char *resp, size_t respLen) {
            proto::types::Type respType = chakra::net::Packet::getType(resp, respLen);
            DLOG(INFO) << "   REPL reply message type " << proto::types::Type_Name(respType) << ":" << respType;
            err = link->conn->send(resp, respLen);
            return err;
        });
        return err;
    });

    if (!err.success()){
        LOG(ERROR) << "I/O error remote addr " << link->conn->remoteAddr() << " " << err.what();
        link->close();
    }
}

chakra::error::Error chakra::replica::Link::sendMsg(::google::protobuf::Message &msg, proto::types::Type type) {
    return chakra::net::Packet::serialize(msg, type, [this](char* data, size_t len){
        return conn->send(data, len);
    });
}

void chakra::replica::Link::startReplicaRecv() {
    rio.set<&chakra::replica::Link::onProcessRecv>(this);
    rio.set(ev::get_default_loop());
    rio.start(conn->fd(), ev::READ);
}

void chakra::replica::Link::replicaEventHandler() {
    auto nowMillSec = utils::Basic::getNowMillSec();
    if ((state == State::CONNECTING || state == State::RECEIVE_PONG)
        && nowMillSec - lastInteractionMs > FLAGS_replica_timeout_ms){
        LOG(WARNING) << "REPL connecting to primary " << getPeerName() << " timeout.";
        deConnectPrimary();
    }

    if (state == State::TRANSFOR
        && nowMillSec - lastTransferMs > FLAGS_replica_timeout_ms){
        LOG(WARNING) << "REPL receiving bulk data from " << getPeerName() << " timeout..."
                     << " If the problem persists try to set the 'repl-timeout' parameter in chakra.conf to a larger value.";
        abortTransfer();
    }

    if (state == State::CONNECTED
        && nowMillSec -  lastInteractionMs > FLAGS_replica_timeout_ms){
        LOG(WARNING) << "REPL timeout no data nor PING received from " << getPeerName() << "(" << nowMillSec << "-" << lastInteractionMs << ">" << FLAGS_replica_timeout_ms << ")";
        close();
    }
    // 尝试开始连接被复制的服务器
    if (state == State::CONNECT) connectPrimary();
    // 定时心跳
    if (state == State::CONNECTED) heartBeat();
}

chakra::error::Error chakra::replica::Link::snapshotBulk(const std::string &dbname) {
    auto& dbptr = chakra::database::FamilyDB::get();
    rocksdb::SequenceNumber lastSeq;
    auto err = dbptr.snapshot(dbname, &bulkiter, lastSeq);
    if (!err.success()){
        LOG(ERROR) << "REPL create db " << dbname << " snapshot error " << err.what();
    } else {
        LOG(INFO) << "REPL start send bulk seq " << deltaSeq;
        state = State::TRANSFOR;
        deltaSeq = lastSeq;
        bulkiter->SeekToFirst();
        startSendBulk();
    }
    return err;
}


void chakra::replica::Link::startSendBulk() {
    transferIO.set<chakra::replica::Link, &chakra::replica::Link::onSendBulk>(this);
    transferIO.set(ev::get_default_loop());
    transferIO.start(FLAGS_replica_bulk_send_interval_sec);
}

void chakra::replica::Link::onSendBulk(ev::timer &watcher, int event) {
    LOG(INFO) << "REPL on send bulk... state:" << (int)state;
    if (state != State::TRANSFOR) return;

    int size = 0;
    int num = 0;
    proto::replica::BulkMessage bulkMessage;
    bulkMessage.set_seq(deltaSeq);
    bulkMessage.set_db_name(getDbName());
    while (bulkiter->Valid()){
        size += bulkiter->key().size();
        size += bulkiter->value().size();
        if (size > BULK_LEN || size > FLAGS_connect_buff_size)
            break;
        auto data = bulkMessage.mutable_kvs()->Add();
        data->set_key(bulkiter->key().ToString());
        data->set_value(bulkiter->value().ToString());
        bulkiter->Next();
        num++;
    }

    if (bulkiter->Valid()){
        bulkMessage.set_end(false);
        startSendBulk(); // 继续传输
    } else {
        LOG(INFO) << "REPL send bulk finished";
        // 全量发送完成
        bulkMessage.set_end(true);
        transferIO.stop();
        state = State::CONNECTED;
        delete bulkiter;
        bulkiter = nullptr;
    }

    auto err = sendMsg(bulkMessage, proto::types::R_BULK);
    if (!err.success()){
        LOG(ERROR) << "REPL send bulk message to error " << err.what();
    } else {
        LOG(INFO) << "REPL send bulk message size " << size << " is end " << bulkMessage.end() << " num " << num;
    }
}

void chakra::replica::Link::startPullDelta() {
    deltaIO.set<chakra::replica::Link, &chakra::replica::Link::onPullDelta>(this);
    deltaIO.set(ev::get_default_loop());
    deltaIO.start(FLAGS_replica_delta_pull_interval_sec);
}

void chakra::replica::Link::onPullDelta(ev::timer &watcher, int event) {
    proto::replica::DeltaMessageRequest deltaMessageRequest;
    deltaMessageRequest.set_db_name(dbName);
    deltaMessageRequest.set_seq(deltaSeq);
    deltaMessageRequest.set_size(DELTA_LEN);
    auto err = sendMsg(deltaMessageRequest, proto::types::R_DELTA_REQUEST);
    if (!err.success()){
        LOG(INFO) << "REPL send delta pull message eror " << err.what();
    } else {
        LOG(INFO) << "REPL send db "
                  << deltaMessageRequest.db_name() << " seq " << deltaMessageRequest.seq()
                  << " delta message success.";
    }
}

void chakra::replica::Link::connectPrimary() {
    LOG(INFO) << "REPL state CONNECTING";
    if (type != POSITIVE){
        LOG(ERROR) << "REPL link type is not positive.";
        return;
    }

    conn = std::make_shared<net::Connect>(net::Connect::Options{ .host = ip, .port = port });
    auto err = conn->connect();
    if (!err.success()){
        LOG(ERROR) << "REPL connect primary "
                   << "[" << ip << ":" << port << "] "
                   << "error " << err.what();
        return;
    }

    state = State::CONNECTING;
    proto::replica::PingMessage ping;
    ping.set_sender_name(cluster::Cluster::get()->getMyself()->getName());
    ping.set_db_name(getDbName());
    err = sendMsg(ping, proto::types::R_PING);
    if (!err.success()){
        LOG(ERROR) << "REPL connect primary "
                   << ip << ":" << port
                   << " error " << err.what();
    } else {
        setLastInteractionMs(utils::Basic::getNowMillSec());
        state = State::RECEIVE_PONG;
        startReplicaRecv();
        LOG(INFO) << "REPLICA -> PRIMARY connect success.";
    }
}

void chakra::replica::Link::deConnectPrimary() {
    assert(state == State::CONNECTING || state == State::RECEIVE_PONG);
    state = State::CONNECT;
}

void chakra::replica::Link::abortTransfer() {
    assert(state == State::TRANSFOR);
    state = State::CONNECT;
}

void chakra::replica::Link::heartBeat() {
    proto::replica::HeartBeatMessage heart;
    heart.set_heartbeat_ms(utils::Basic::getNowMillSec());
    heart.set_seq(deltaSeq);
    auto err = sendMsg(heart, proto::types::R_HEARTBEAT);
    if (!err.success()){
        LOG(ERROR) << "REPL heartbeat error " << err.what();
    }
}

chakra::error::Error chakra::replica::Link::tryPartialReSync() {
    if (state != Link::State::RECEIVE_PONG)
        return error::Error("partial resync state isn't RECEIVE_PONG");

    proto::replica::SyncMessageRequest syncMessageRequest;
    syncMessageRequest.set_db_name(dbName);
    if (deltaSeq > 0){ // 不是第一次同步，尝试从上次结束的地方开始
        syncMessageRequest.set_seq(deltaSeq);
    } else {
        syncMessageRequest.set_seq(-1);
    }
    LOG(INFO) << "REPL try a partial sync request " << syncMessageRequest.DebugString();
    return sendMsg(syncMessageRequest, proto::types::R_SYNC_REQUEST);
}

proto::replica::ReplicaState chakra::replica::Link::dumpLink() {
    proto::replica::ReplicaState metaReplica;
    metaReplica.set_primary(peerName);
    metaReplica.set_db_name(dbName);
    metaReplica.set_ip(ip);
    metaReplica.set_port(port);
    metaReplica.set_delta_seq(deltaSeq);
    return metaReplica;
}

bool chakra::replica::Link::isTimeout() const {
    auto now = utils::Basic::getNowMillSec();
    return (state == State::CONNECT && (now - lastInteractionMs >  FLAGS_replica_timeout_ms));
}

const std::string &chakra::replica::Link::getPeerName() const { return peerName; }

void chakra::replica::Link::setPeerName(const std::string &name) { peerName = name; }

chakra::replica::Link::State chakra::replica::Link::getState() const { return state; }

void chakra::replica::Link::setState(chakra::replica::Link::State st) { Link::state = st; }

const std::string &chakra::replica::Link::getDbName() const { return dbName; }

void chakra::replica::Link::setDbName(const std::string &name) { Link::dbName = name; }

int64_t chakra::replica::Link::getRocksSeq() const { return deltaSeq;}

void chakra::replica::Link::setRocksSeq(int64_t seq) { Link::deltaSeq = seq; }

long chakra::replica::Link::getLastTransferMs() const { return lastTransferMs; }

void chakra::replica::Link::setLastTransferMs(long ms) { Link::lastTransferMs = ms; }

long chakra::replica::Link::getLastInteractionMs() const { return lastInteractionMs; }

void chakra::replica::Link::setLastInteractionMs(long ms) { Link::lastInteractionMs = ms; }

const std::string &chakra::replica::Link::getIp() const {return ip; }

void chakra::replica::Link::setIp(const std::string &ip) {Link::ip = ip; }

int chakra::replica::Link::getPort() const {return port;}

void chakra::replica::Link::setPort(int port) { Link::port = port; }

void chakra::replica::Link::close() {
    // 这里一定要把三个 ev io 关闭掉，否则会在下一轮使用无效的 link
    rio.stop();
    deltaIO.stop();
    transferIO.stop();
    if (conn)
        conn->close();
    state = State::CONNECT;
}

chakra::replica::Link::~Link() {
    delete bulkiter;
    if (conn) conn = nullptr;
    if (bulkiter) bulkiter = nullptr;
}
