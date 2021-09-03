//
// Created by kwins on 2021/6/30.
//

#include "replica_link.h"
#include <glog/logging.h>
#include "net/packet.h"
#include "cmds/command_pool.h"
#include "utils/basic.h"
#include "rotate_binary_log.h"
#include "replica.pb.h"
#include <thread>
#include <chrono>
#include "utils/file_helper.h"
#include "database/db_family.h"
#include <rocksdb/db.h>

chakra::replica::Link::Link(chakra::replica::Link::Options options){
    opts = std::move(options);
    this->dir = opts.dir;
    state = State::CONNECT;
    dbName = opts.dbName;
    ip = opts.ip;
    port = opts.port;
    if (opts.sockfd > 0){
        conn = std::make_shared<net::Connect>(net::Connect::Options{ .fd = opts.sockfd });
    }
}

void chakra::replica::Link::onProcessRecv(ev::io &watcher, int event) {
    auto link = static_cast<chakra::replica::Link*>(watcher.data);

    auto err = link->conn->receivePack([&link](char *req, size_t reqLen) {

        proto::types::Type msgType = chakra::net::Packet::getType(req, reqLen);
        LOG(INFO) << "-- REPL received message type " << proto::types::Type_Name(msgType) << ":" << msgType;

        auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);

        utils::Error err;
        cmdsptr->execute(req, reqLen, link, [&link, &err](char *resp, size_t respLen) {
            proto::types::Type respType = chakra::net::Packet::getType(resp, respLen);
            LOG(INFO) << "  REPL reply message type " << proto::types::Type_Name(respType) << ":" << respType;
            err = link->conn->send(resp, respLen);
            return err;
        });
        return err;
    });

    if (!err.success()){
        LOG(ERROR) << "I/O error remote addr " << link->conn->remoteAddr() << " " << err.toString();
        link->close();
    }
}

void chakra::replica::Link::startReplicaRecv() {
    replior.set<&chakra::replica::Link::onProcessRecv>(this);
    replior.set(ev::get_default_loop());
    replior.start(conn->fd(), ev::READ);
}

void chakra::replica::Link::replicaEventLoop() {
    auto nowMillSec = utils::Basic::getNowMillSec();
    if ((state == State::CONNECTING || state == State::RECEIVE_PONG)
        && nowMillSec - lastInteractionMs > opts.replicaTimeoutMs){
        LOG(WARNING) << "REPL connecting to primary timeout.";
        deConnectPrimary();
    }

    if (state == State::TRANSFOR
        && nowMillSec - lastTransferMs > opts.replicaTimeoutMs){
        LOG(WARNING) << "REPL receiving bulk data from PRIMARY... If the problem persists try to set the 'repl-timeout' parameter in naruto.conf to a larger value.";
        abortTransfer();
    }

    if (state == State::CONNECTED
        && nowMillSec -  lastInteractionMs > opts.replicaTimeoutMs){
        LOG(WARNING) <<"REPL timeout no data nor PING received...";
        close();
    }

    // 尝试开始连接被复制的服务器
    if (state == State::CONNECT)
        connectPrimary();

    if (state == State::CONNECTED)
        heartBeat();
}

void chakra::replica::Link::startSendBulk(rocksdb::Iterator* iter, rocksdb::SequenceNumber seq) {
    state = State::TRANSFOR;
    bulkiter = iter;
    deltaSeq = seq;
    transferIO.set<chakra::replica::Link, &chakra::replica::Link::onSendBulk>(this);
    transferIO.set(ev::get_default_loop());
    transferIO.start(conn->fd(), ev::WRITE);
}

void chakra::replica::Link::onSendBulk(ev::io &watcher, int event) {
    LOG(INFO) << "REPL primary send bulk to replicator.";
    if (state != State::TRANSFOR) return;

    int size = 0;
    proto::replica::BulkMessage bulkMessage;
    bulkMessage.set_seq(deltaSeq);
    while (bulkiter->Valid()){
        if (size > TRANSFER_LEN)
            break;

        size += bulkiter->key().size();
        size += bulkiter->value().size();
        auto data = bulkMessage.mutable_kvs()->Add();
        data->set_key(bulkiter->key().ToString());
        data->set_value(bulkiter->value().ToString());

        bulkiter->Next();
    }

    if (bulkiter->Valid()){
        bulkMessage.set_end(false);
    } else {
        // 全量发送完成
        bulkMessage.set_end(true);
        transferIO.stop();
        state = State::CONNECTED;
        delete bulkiter;
        bulkiter = nullptr;
    }

    chakra::net::Packet::serialize(bulkMessage, proto::types::R_BULK, [this](char* req, size_t reqLen){
        auto err = this->conn->send(req, reqLen);
        if (!err.success()){
            LOG(ERROR) << "REPL send bulk message to error " << strerror(errno);
        }
        return err;
    });
}

void chakra::replica::Link::startRecvBulk() {
    replior.set<chakra::replica::Link, &chakra::replica::Link::onRecvBulk>(this);
    replior.set(ev::get_default_loop());
    replior.start(conn->fd(), ev::READ);
}

void chakra::replica::Link::startPullDelta() {
    deltaIO.set<chakra::replica::Link, &chakra::replica::Link::onPullDelta>(this);
    deltaIO.set(ev::get_default_loop());
    deltaIO.start(opts.cronInterval);
}

void chakra::replica::Link::onPullDelta(ev::timer &watcher, int event) {
    auto dbptr = database::FamilyDB::get();
    proto::replica::DeltaMessageRequest deltaMessageRequest;
    deltaMessageRequest.set_db_name(dbName);
    deltaMessageRequest.set_seq(deltaSeq);

    proto::replica::DeltaMessageResponse deltaMessageResponse;
    sendSyncMsg(deltaMessageRequest, proto::types::R_PULL, deltaMessageResponse);

    for (int i = 0; i < deltaMessageResponse.seqs_size(); ++i) {
        auto& seq = deltaMessageResponse.seqs(i);
        rocksdb::WriteBatch batch(seq.data());
        dbptr->put(dbName, batch);
        deltaSeq = seq.seq();
    }

    startPullDelta();
}

void chakra::replica::Link::onRecvBulk(ev::io &watcher, int event) {
    LOG(INFO) << "REPL recv bulk data ...";
    auto err = conn->receivePack([this](char* data, size_t len){
        proto::replica::BulkMessage bulkMessage;
        auto err = chakra::net::Packet::deSerialize(data, len, bulkMessage, proto::types::R_BULK);
        if (!err.success())
            return err;

        auto dbptr = chakra::database::FamilyDB::get();
        rocksdb::WriteBatch batch;
        for(auto& it : bulkMessage.kvs()){
            batch.Put(it.key(), it.value());
        }
        dbptr->put(dbName, batch);

        if (bulkMessage.end()){
            state = State::CONNECTED;
            deltaSeq = bulkMessage.seq();
            startPullDelta();
        }
        return err;
    });
    if (!err.success()) LOG(ERROR) << "REPL pull bulk error " << err.toString();
}

void chakra::replica::Link::onHandshake(ev::io &watcher, int event) {
    if (state == State::CONNECTING){
        LOG(INFO) << "REPL state CONNECTING";
        state = State::RECEIVE_PONG;
        repliow.stop();

        proto::replica::PingMessage ping;
        ping.set_ping_ms(utils::Basic::getNowMillSec());
        ping.set_my_name("");
        chakra::net::Packet::serialize(ping, proto::types::R_PING, [this](char* req, size_t reqLen){
            return conn->send(req, reqLen);
        });
        return;
    }

    if (state == State::RECEIVE_PONG){
        LOG(INFO) << "REPL state RECEIVE_PONG";
        replior.stop();

        proto::replica::PongMessage pong;
        auto err = conn->receivePack([this, &pong](char *resp, size_t respLen) {
            return chakra::net::Packet::deSerialize(resp, respLen, pong, proto::types::R_PONG);
        });

        if (!err.success() || pong.error().errcode() || !tryPartialReSync()){
            std::string errmsg;
            if (!err.success())
                errmsg = err.toString();
            if (pong.error().errcode())
                errmsg = pong.error().errmsg();
            if (!errmsg.empty())
                LOG(ERROR) << "REPL recv error " << errmsg;
            close();
        }
    }
}

void chakra::replica::Link::connectPrimary() {
    conn = std::make_shared<net::Connect>(
            net::Connect::Options{ .host = ip, .port = port });
    auto err = conn->connect();
    if (!err.success()){
        LOG(ERROR) << "Link connect to primary " << ip << ":" << ip << " error " << err.toString();
        return;
    }
    replior.set<chakra::replica::Link, &chakra::replica::Link::onHandshake>(this);
    replior.set(ev::get_default_loop());
    replior.start(conn->fd(), ev::READ);

    repliow.set<chakra::replica::Link, &chakra::replica::Link::onHandshake>(this);
    repliow.set(ev::get_default_loop());
    repliow.start(conn->fd(), ev::WRITE);

    lastTransferMs = utils::Basic::getNowMillSec();
    state = State::CONNECTING;
    LOG(INFO) << "PRIMASY <-> REPLICA connect success.";
}

void chakra::replica::Link::deConnectPrimary() {
    assert(state == State::CONNECTING || state == State::RECEIVE_PONG);
    replior.stop();
    repliow.stop();
    state = State::CONNECT;
}

void chakra::replica::Link::abortTransfer() {
    assert(state == State::TRANSFOR);
    replior.stop();
    state = State::CONNECT;
}

void chakra::replica::Link::heartBeat() const {
    proto::replica::HeartBeatMessage heart;
    heart.set_heartbeat_ms(utils::Basic::getNowMillSec());
    heart.set_seq(deltaSeq);
    chakra::net::Packet::serialize(heart, proto::types::R_HEARTBEAT, [this](char* data, size_t len){
        return conn->send(data, len);
    });
}

bool chakra::replica::Link::tryPartialReSync() {
    proto::replica::SyncMessageRequest syncMessageRequest;
    syncMessageRequest.set_db_name(dbName);
    if (deltaSeq > 0){ // 不是第一次同步，尝试从上次结束的地方开始
        syncMessageRequest.set_seq(deltaSeq);
    } else {
        syncMessageRequest.set_seq(-1);
    }

    LOG(INFO) << "Try a partial resync from " << syncMessageRequest.DebugString();

    proto::replica::SyncMessageResponse syncMessageResponse;
    sendSyncMsg(syncMessageRequest, proto::types::R_PSYNC, syncMessageResponse);
    if (syncMessageResponse.error().errcode()){
        LOG(ERROR) << "Unexpected errcode to PSYNC from primary " << syncMessageResponse.error().errmsg();
        return false;
    }

    if (syncMessageResponse.psync_type() == proto::types::R_FULLSYNC){
        LOG(INFO) << "PRIMARY <-> REPLICATE accepted a FULL sync.";
        state = State::TRANSFOR;
        startRecvBulk();

    } else if (syncMessageResponse.psync_type() == proto::types::R_PARTSYNC){
        LOG(INFO) << "PRIMARY <-> REPLICATE accepted a PART sync.";
        state = State::CONNECTED;
        deltaSeq = syncMessageRequest.seq();
        startReplicaRecv();
    } else {
        LOG(INFO) << "PRIMARY <-> REPLICATE accepted a BAD sync.";
        return false;
    }
    return true;
}

void chakra::replica::Link::sendSyncMsg(google::protobuf::Message &msg, proto::types::Type type,
                                        google::protobuf::Message &reply) {
    chakra::net::Packet::serialize(msg, type, [this, &reply, type](char* req, size_t reqLen){
        auto err = this->conn->send(req, reqLen);
        if (err.success()){
            return this->conn->receivePack([&reply, type](char *resp, size_t respLen) {
                return chakra::net::Packet::deSerialize(resp, respLen, reply, type);
            });
        }
        LOG(ERROR) << "REPL send message to [" << opts.ip << ":" << opts.port << "] error " << strerror(errno);
        return err;
    });
}

nlohmann::json chakra::replica::Link::dumpLink() {
    nlohmann::json j;
    j["primary"] = primary;
    j["db_name"] = dbName;
    j["ip"] = ip;
    j["port"] = port;
    j["delta_seq"] = deltaSeq;
    return j;
}

void chakra::replica::Link::loadLink(const nlohmann::json &j) {
    j.at("primary").get_to(primary);
    j.at("db_name").get_to(dbName);
    j.at("ip").get_to(ip);
    j.at("port").get_to(port);
    j.at("delta_seq").get_to(deltaSeq);
}

void chakra::replica::Link::close() {
    replior.stop();
    repliow.stop();
    deltaIO.stop();
    transferIO.stop();
    if (conn)
        conn->close();
    state = State::CONNECT;
}

bool chakra::replica::Link::isTimeout() const {
    auto now = utils::Basic::getNowMillSec();
    return (state == State::CONNECT && (now - lastInteractionMs >  opts.replicaTimeoutMs));
}

const std::string &chakra::replica::Link::getPrimaryName() const { return primary; }

void chakra::replica::Link::setPrimaryName(const std::string &name) { Link::primary = name; }

chakra::replica::Link::State chakra::replica::Link::getState() const { return state; }

void chakra::replica::Link::setState(chakra::replica::Link::State st) { Link::state = st; }

const chakra::replica::Link::Options &chakra::replica::Link::getOpts() { return opts; }

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
