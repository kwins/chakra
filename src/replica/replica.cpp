//
// Created by kwins on 2021/6/9.
//

#include "replica.h"

#include <algorithm>
#include <cstddef>
#include <ev++.h>
#include <exception>
#include <rocksdb/options.h>
#include <rocksdb/snapshot.h>
#include <thread>
#include <glog/logging.h>
#include <netinet/in.h>
#include <gflags/gflags.h>
#include <vector>

#include "net/network.h"
#include "utils/file_helper.h"
#include "cluster/cluster.h"
#include "replica.pb.h"
#include "error/err.h"
#include "net/packet.h"
#include "cmds/command_pool.h"
#include "utils/basic.h"
#include "database/db_family.h"

DECLARE_int64(connect_buff_size);
DECLARE_string(replica_dir);
DECLARE_int32(server_tcp_backlog);
DECLARE_int32(replica_timeout_ms);
DECLARE_double(replica_cron_interval_sec);
DECLARE_int32(replica_timeout_retry);
DECLARE_double(replica_delta_pull_interval_sec);
DECLARE_double(replica_bulk_send_interval_sec);
DECLARE_int64(replica_delta_batch_bytes);
DECLARE_int64(replica_bulk_batch_bytes);
DECLARE_int64(replica_last_try_resync_timeout_ms);

chakra::replica::Replicate::Replicate() {
    LOG(INFO) << "[replication] init";
    loadLastStateDB(); // 加载配置数据
    auto err = net::Network::tpcListen(utils::Basic::rport(), FLAGS_server_tcp_backlog, sfd);
    if (err) {
        LOG(ERROR) << "[replication] listen on " << utils::Basic::rport() << " error " << err.what();
        exit(1);
    }

    replicaio.set<chakra::replica::Replicate, &chakra::replica::Replicate::onAccept>(this);
    replicaio.set(ev::get_default_loop());
    replicaio.start(sfd, ev::READ);
    startReplicaCron();
    LOG(INFO) << "[replication] init end";
    LOG(INFO) << "[replication] listen on " << utils::Basic::rport();
}

void chakra::replica::Replicate::startReplicaCron() {
    cronIO.set<chakra::replica::Replicate, &chakra::replica::Replicate::onReplicaCron>(this);
    cronIO.set(ev::get_default_loop());
    cronIO.start(FLAGS_replica_cron_interval_sec);
}

void chakra::replica::Replicate::loadLastStateDB() {
    DLOG(INFO) << "[replication] load";
    std::string filename = FLAGS_replica_dir + "/" + REPLICA_FILE_NAME;
    try {
        proto::replica::ReplicaStates replicaStates;
        utils::FileHelper::loadFile(filename, replicaStates);
        for(auto& replica : replicaStates.link_replicas()) {
            chakra::replica::Replicate::Link::PositiveOptions options;
            options.ip = replica.ip();
            options.port = replica.port();
            options.connect = false;
            auto link = new chakra::replica::Replicate::Link(options);
            link->setPeerName(replica.peer_name());
            positiveLinks.emplace(link->getPeerName(), link);
            for (auto db : replica.replicas()) {
                if (db.db_name().empty()) {
                    LOG(ERROR) << "[replication] load empty db";
                    exit(-1);
                }
                auto replicateDB = std::make_shared<chakra::replica::Replicate::Link::ReplicateDB>();
                replicateDB->name = db.db_name();
                replicateDB->deltaSeq = db.delta_seq();
                replicateDB->link = link;
                replicateDB->lastTransferMs = utils::Basic::getNowMillSec();
                link->setReplicateDB(replicateDB);
            }
        }
    } catch (const error::FileError& err) {
        return;
    } catch (const std::exception& err) {
        LOG(ERROR) << "[replication] load file " << filename  << " error " << err.what();
        exit(-1);
    }
    DLOG(INFO) << "[replication] load success";
}

std::shared_ptr<chakra::replica::Replicate> chakra::replica::Replicate::get() {
    static auto replicaptr = std::make_shared<Replicate>();
    return replicaptr;
}

void chakra::replica::Replicate::onReplicaCron(ev::timer &watcher, int event) {
    cronLoops++;
    // client side
    for(auto link : positiveLinks) {
        link.second->replicateEventLoop();
    }
    
    if (cronLoops % 10 == 0)
        dumpReplicateStates();

    // server side
    negativeLinkLoop();

    startReplicaCron();
}

void chakra::replica::Replicate::negativeLinkLoop() {
    negativeLinks.remove_if([](Link* l){
        auto timeout = l->isTimeout();
        if (!timeout) {
            l->heartbeat(false);
        } else {
            LOG(WARNING)<< "[replication] close and delete timeout connect " << l->getPeerName();
            delete l; l = nullptr;
        }
        return timeout;
    });
}

bool chakra::replica::Replicate::replicatedDB(const std::string& peername, const std::string &dbname) {
    auto it = positiveLinks.find(peername);
    if (it == positiveLinks.end()) return false;
    return it->second->getReplicateDB(dbname) != nullptr;
}

void chakra::replica::Replicate::dumpReplicateStates() {
    if (positiveLinks.empty()) return;

    std::string tofile = FLAGS_replica_dir + "/" + REPLICA_FILE_NAME;
    proto::replica::ReplicaStates replicaState;
    for(auto lit : positiveLinks){
        auto state = replicaState.mutable_link_replicas()->Add();
        lit.second->replicateState(*state);
    }

    auto err = chakra::utils::FileHelper::saveFile(replicaState, tofile);
    if (err) {
        LOG(ERROR) << "[replication] flush state error " << err.what();
    } else {
        DLOG(INFO) << "[replication] flush state to file " << tofile << " success.";
    }
}

void chakra::replica::Replicate::setReplicateDB(const std::string& peername, const std::string &dbname,
                                                                         const std::string& ip, int port) {
    auto replicateDB = std::make_shared<chakra::replica::Replicate::Link::ReplicateDB>();
    replicateDB->name = dbname;
    replicateDB->state = Link::State::REPLICA_INIT;

    auto it = positiveLinks.find(peername);
    if (it != positiveLinks.end()) {
        replicateDB->link = it->second;
        it->second->setReplicateDB(replicateDB);
        return;
    }

    chakra::replica::Replicate::Link::PositiveOptions options;
    options.ip = ip;
    options.port = port;
    options.connect = false; /* 这里先不连接，保证Link对象构建成功，会在后续轮训时进行连接 */ 
    auto link = new chakra::replica::Replicate::Link(options);
    link->setPeerName(peername);

    replicateDB->link = link;
    link->setReplicateDB(replicateDB);
    positiveLinks.emplace(peername, link);
}

void chakra::replica::Replicate::onAccept(ev::io &watcher, int event) {
    sockaddr_in addr{};
    socklen_t slen = sizeof(addr);
    int sockfd = ::accept(watcher.fd, (sockaddr*)&addr, &slen);
    if (sockfd < 0){
        LOG(ERROR) << "[replication] on accept error " << strerror(errno);
        return;
    }

    chakra::replica::Replicate::Link::NegativeOptions options;
    options.sockfd = sockfd;
    auto link = new chakra::replica::Replicate::Link(options);
    link->startReplicateRecvMsg(); /* 开始接收消息 */
    negativeLinks.push_back(link);
}

std::unordered_map<std::string, std::vector<chakra::replica::Replicate::Link*>> 
chakra::replica::Replicate::dbLinks(chakra::replica::Replicate::Link::State state) {
    std::unordered_map<std::string, std::vector<chakra::replica::Replicate::Link*>> dblinks;
    for (auto link : positiveLinks) {
        if (link.second->getState() != Link::State::CONNECTED) continue;
        for(auto it : link.second->getReplicas()){
            if (it.second->state != state) continue;
            dblinks[it.first].push_back(link.second);
        }
    }
    return dblinks;
}

std::unordered_map<std::string, std::vector<chakra::replica::Replicate::Link*>> chakra::replica::Replicate::dbTransferedLinks() {
    return dbLinks(chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED);
}

void chakra::replica::Replicate::stop() {
    if (sfd != -1) ::close(sfd);
    replicaio.stop();
    cronIO.stop();
    dumpReplicateStates();
    clearNegativeLinks();
    LOG(INFO) << "[replication] stop";
}

void chakra::replica::Replicate::clearNegativeLinks() {
    for(auto nLink : negativeLinks) {
        nLink->close();
        delete nLink; nLink = nullptr;
    }
    negativeLinks.clear();
}

const std::string chakra::replica::Replicate::REPLICA_FILE_NAME = "replicas.json";

// ------------------------ Replicate::Link ------------------------
chakra::replica::Replicate::Link::Link(const PositiveOptions& options) 
    : chakra::net::Link(options.ip, options.port, options.connect), ip(options.ip), port(options.port) {

    type = POSITIVE;
    setLastInteractionMs(utils::Basic::getNowMillSec());
    if (options.connect) {
        setState(chakra::replica::Replicate::Link::State::CONNECTING);
    } else {
        setState(chakra::replica::Replicate::Link::State::CONNECT);
    }
}

chakra::replica::Replicate::Link::Link(const NegativeOptions& options) : chakra::net::Link(options.sockfd) {
    type = NEGATIVE;
    setLastInteractionMs(utils::Basic::getNowMillSec());
    std::string addr = conn->remoteAddr();
    auto pos = addr.find(":");
    if (pos != addr.npos){
        setIp(addr.substr(0, pos));
        setPort(std::stoi(addr.substr(pos + 1, addr.size())));
    }
    setState(chakra::replica::Replicate::Link::State::CONNECTING);
}

void chakra::replica::Replicate::Link::replicateState(proto::replica::LinkReplicaState& rs){
    rs.set_peer_name(getPeerName());
    rs.set_ip(getIp());
    rs.set_port(getPort());
    for (auto & db : replicas){
        auto replicaMeta = rs.mutable_replicas()->Add();
        replicaMeta->set_db_name(db.second->name);
        replicaMeta->set_delta_seq(db.second->deltaSeq);
    }
}

void chakra::replica::Replicate::Link::startReplicateRecvMsg() {
    rio.set<chakra::replica::Replicate::Link, &chakra::replica::Replicate::Link::onReplicateRecvMsg>(this);
    rio.set(ev::get_default_loop());
    rio.start(conn->fd(), ev::READ);
}

void chakra::replica::Replicate::Link::onReplicateRecvMsg(ev::io& watcher, int event) {
    try {
        conn->receive([this](char *req, size_t rl) {

            proto::types::Type msgType = chakra::net::Packet::type(req, rl);
            DLOG(INFO) << "-- [replication] received message type "
                    << proto::types::Type_Name(msgType) << ":" << msgType
                    << " FROM " << (getPeerName().empty() ? conn->remoteAddr() : getPeerName());
            auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);
            cmdsptr->execute(req, rl, this);
            return error::Error();
        });
    } catch (const error::ConnectClosedError& e1) {
        close();
    } catch (const error::Error& e) {
        LOG(ERROR) << "[replication] i/o error " << e.what();
        close();
    }
}

void chakra::replica::Replicate::Link::asyncSendMsg(::google::protobuf::Message& msg, proto::types::Type type) {
    DLOG(INFO) << "[chakra] async send message type " << proto::types::Type_Name(type) << ":" << type << " to default loop";
    conn->writeBuffer(msg, type);
    
    if (wio.is_active()) return;
    wio.set<chakra::replica::Replicate::Link, &chakra::replica::Replicate::Link::onReplicateWriteMsg>(this);
    wio.set(ev::get_default_loop());
    wio.start(conn->fd(), ev::WRITE);
}

void chakra::replica::Replicate::Link::onReplicateWriteMsg(ev::io& watcher, int event) {
    DLOG(INFO) << "[chakra] on replicate write data len " << conn->sendBufferLength();
    try {
        conn->sendBuffer();
    } catch (const error::ConnectClosedError& err) {
        DLOG(ERROR) << err.what();
    }  catch (const std::exception& err) {
        LOG(ERROR) << err.what();
    }
    if (wio.is_active())
        wio.stop();
}

const std::unordered_map<std::string, std::shared_ptr<chakra::replica::Replicate::Link::ReplicateDB>>& 
chakra::replica::Replicate::Link::getReplicas() { return replicas; }

void chakra::replica::Replicate::Link::reconnect() {
    assert(state == chakra::replica::Replicate::Link::State::CONNECT);

    auto err = conn->connect();
    if (err) {
        LOG(ERROR) << "[replication] retry connect to (" << conn->remoteAddr() << ") error " << err.what();
    } else {
        setState(chakra::replica::Replicate::Link::State::CONNECTING);
        setLastInteractionMs(utils::Basic::getNowMillSec());
        startReplicateRecvMsg();
        LOG(INFO) << "[replication] retry connect to (" << conn->remoteAddr()<< ") success.";
    }
}

void chakra::replica::Replicate::Link::heartbeat(bool positive) {
    if (state != chakra::replica::Replicate::Link::State::CONNECTED) return;

    auto dbptr = chakra::database::FamilyDB::get();
    auto seqs = dbptr->dbSeqs();

    proto::replica::HeartBeatMessage heart;
    heart.set_positive(positive);
    for (auto& seq : seqs) {
        auto addSeq = heart.mutable_seqs()->Add();
        addSeq->CopyFrom(seq);
    }
    asyncSendMsg(heart, proto::types::R_HEARTBEAT);
}

void chakra::replica::Replicate::Link::handshake() {
    auto clsptr = cluster::Cluster::get();
    proto::replica::PingMessage ping;
    ping.set_sender_name(clsptr->getMyself()->getName());

    asyncSendMsg(ping, proto::types::R_PING);
    setLastInteractionMs(utils::Basic::getNowMillSec());
    LOG(ERROR) << "[replication] handshake to (" << ip << ":" << port << ")"
                << " and send ping success, start waiting for pong message.";
}

void chakra::replica::Replicate::Link::startSendBulk(const std::string& name) {
    auto replicateDB = replicas.find(name);
    if (replicateDB != replicas.end()){
        replicateDB->second->startSendBulk();
    }
}

void chakra::replica::Replicate::Link::tryPartialReSync(const std::string& name) {
    if (state != Link::State::CONNECTED)
        return;
    
    auto replicateDB = replicas.find(name);
    if (replicateDB == replicas.end()){
        LOG(ERROR) << "[replication] not found db " << name << " when try a partial sync.";
        return;
    }
    
    if (replicateDB->second->state != Link::State::REPLICA_INIT) return;
    /* 防止发送重同步次数过多，最终会以最后一次为准 */
    if (utils::Basic::getNowMillSec() - replicateDB->second->lastTryReSyncMs < FLAGS_replica_last_try_resync_timeout_ms) return;
    replicateDB->second->lastTryReSyncMs = utils::Basic::getNowMillSec();

    proto::replica::SyncMessageRequest syncMessageRequest;
    syncMessageRequest.set_db_name(name);
    syncMessageRequest.set_seq(replicateDB->second->deltaSeq); // 不是第一次同步，尝试从上次结束的地方开始

    LOG(INFO) << "[replication] try a partial sync request " << syncMessageRequest.DebugString() << " to " << getPeerName();
    asyncSendMsg(syncMessageRequest, proto::types::R_SYNC_REQUEST);
}

void chakra::replica::Replicate::Link::setRocksSeq(const std::string& name, int64_t seq) {
    auto replicateDB = replicas.find(name);
    if (replicateDB != replicas.end()){
        replicateDB->second->deltaSeq = seq;
    }
}

void chakra::replica::Replicate::Link::startPullDelta(const std::string& name){
    auto replicateDB = replicas.find(name);
    if (replicateDB != replicas.end()){
        replicateDB->second->startPullDelta();
    }
}

std::shared_ptr<chakra::replica::Replicate::Link::ReplicateDB> chakra::replica::Replicate::Link::getReplicateDB(const std::string& name) {
    auto replicaDB = replicas.find(name);
    if (replicaDB != replicas.end()) return replicaDB->second;
    return nullptr;
}
void chakra::replica::Replicate::Link::setReplicateDB(std::shared_ptr<ReplicateDB> replicateDB) { replicas.emplace(replicateDB->name, replicateDB); }

chakra::error::Error chakra::replica::Replicate::Link::snapshotDB(const std::string& dbname, rocksdb::SequenceNumber& lastSeq) {
    auto replicateDB = getReplicateDB(dbname); // 可能client请求了多次，以第一次为准
    if (replicateDB != nullptr 
        && replicateDB->state == State::REPLICA_TRANSFORING
        && replicateDB->bulkiter != nullptr 
        && replicateDB->link != nullptr
        && replicateDB->snapshot != nullptr) {
        return error::Error();
    }

    if (replicateDB) replicateDB->reset();

    // 第一次
    replicateDB = std::make_shared<ReplicateDB>();
    replicateDB->name = dbname;
    replicateDB->lastTransferMs = utils::Basic::getNowMillSec();
    auto dbptr = chakra::database::FamilyDB::get();
    try {
        replicateDB->snapshot = dbptr->snapshot(replicateDB->name);
        if (replicateDB->snapshot == nullptr) {
            return error::Error("snapshot db " + replicateDB->name + " error");
        }
        
        lastSeq = replicateDB->snapshot->GetSequenceNumber();
        rocksdb::ReadOptions readOptions;
        readOptions.snapshot = replicateDB->snapshot;
        replicateDB->bulkiter = dbptr->iterator(replicateDB->name, readOptions);
        replicateDB->state = State::REPLICA_TRANSFORING; /* 全量同步标识 */
        replicateDB->deltaSeq = lastSeq;
        replicateDB->bulkiter->SeekToFirst();
        replicateDB->link = this;
        setReplicateDB(replicateDB);

        /* 开始周期性的发送全量数据，直到发送完成 */
        startSendBulk(dbname);
        LOG(INFO) << "[replication] snapshot db " << dbname << " success and start send bulk with seq " << lastSeq;
        return error::Error();
    } catch (const std::exception& err) {
        LOG(ERROR) << "[replication] snapshot db " << dbname << " error " << err.what();
        throw err;
    }
}

void chakra::replica::Replicate::Link::replicateLinkEvent() {
    auto nowMillSec = utils::Basic::getNowMillSec();
    switch (state) {
    case chakra::replica::Replicate::Link::State::CONNECTING:
    {
        if (nowMillSec - getLastInteractionMs() > FLAGS_replica_timeout_ms) {
            LOG(WARNING) << "[replication] handshake timeout " << (nowMillSec - getLastInteractionMs() - FLAGS_replica_timeout_ms) 
                         << " ms, no PONG received from " << getPeerName();
            close();
        } else {
            handshake();
        }
        break;
    }
    case chakra::replica::Replicate::Link::State::CONNECTED:
    {
        if (nowMillSec - getLastInteractionMs() > FLAGS_replica_timeout_ms) {
            LOG(WARNING) << "[replication] connect timeout " << (nowMillSec - getLastInteractionMs() - FLAGS_replica_timeout_ms) 
                         << " ms, no DATA nor HEADBEAT received from " << getPeerName();
            close();
        } else {
            heartbeat(true);
        }
        break;
    }
    case chakra::replica::Replicate::Link::State::CONNECT:
    {
        reconnect();
        break;
    }
    default:
        break;
    }
}

// Positive Links 才会执行
void chakra::replica::Replicate::Link::replicateEventLoop() {
    replicateLinkEvent();
    // 必须握手完成后才能执行后续逻辑
    if (state != chakra::replica::Replicate::Link::State::CONNECTED) return;

    for (auto db : replicas){
        replicaEvent(db.second->name);
    }
}

void chakra::replica::Replicate::Link::replicaEvent(const std::string& name) {
    auto replicateDB = replicas.find(name);
    if (replicateDB == replicas.end()) return;

    if (state != chakra::replica::Replicate::Link::State::CONNECTED) return;

    auto nowMillSec = utils::Basic::getNowMillSec();
    switch (replicateDB->second->state){
    case chakra::replica::Replicate::Link::State::REPLICA_INIT:
    {
        tryPartialReSync(name);
        break;
    }
    case chakra::replica::Replicate::Link::State::REPLICA_TRANSFORING:
    {
        if (nowMillSec - getLastTransferMs(name) > FLAGS_replica_timeout_ms) {
            LOG(WARNING) << "[replication] bulk data from " << getPeerName() << " timeout..."
                        << " If the problem persists try to set the 'replica_timeout_ms' parameter in chakra.conf to a larger value.";
            replicateDB->second->close();
        }
        break;
    }
    default:
        break;
    }
}

void chakra::replica::Replicate::Link::close() {
    rio.stop();
    wio.stop();
    for(auto db : replicas){
        db.second->close(); /* 连接断了，相关的DB复制任务也需要close */
    }
    if (conn)
        conn->close();
    setState(State::CONNECT);
}

bool chakra::replica::Replicate::Link::isTimeout() const {
    auto now = utils::Basic::getNowMillSec();
    return (state == State::CONNECT && (now - getLastInteractionMs() >  FLAGS_replica_timeout_ms));
}

void chakra::replica::Replicate::Link::setPeerName(const std::string& name) { peerName = name; }
std::string chakra::replica::Replicate::Link::getPeerName() const { return peerName; }
void chakra::replica::Replicate::Link::setLastInteractionMs(int64_t ms) { lastInteractionMs = ms; }
int64_t chakra::replica::Replicate::Link::getLastInteractionMs() const { return lastInteractionMs; }
chakra::replica::Replicate::Link::State chakra::replica::Replicate::Link::getState() const { return state; }
void chakra::replica::Replicate::Link::setState(State st) { state = st; }
void chakra::replica::Replicate::Link::setLastTransferMs(const std::string& name, int64_t ms) {
    auto replicaDB = getReplicateDB(name);
    if (!replicaDB) return;
    replicaDB->lastTransferMs = ms;
}

int64_t chakra::replica::Replicate::Link::getLastTransferMs(const std::string& name) {
    auto replicaDB = getReplicateDB(name);
    if (!replicaDB) return 0;
    return replicaDB->lastTransferMs;
}
std::string chakra::replica::Replicate::Link::getIp() const { return ip; }
void chakra::replica::Replicate::Link::setIp(const std::string& v) { ip = v; }
int chakra::replica::Replicate::Link::getPort() const { return port; }
void chakra::replica::Replicate::Link::setPort(int p) { port = p; }


// ------------------------ Replicate::Link::ReplicateDB ------------------------
void chakra::replica::Replicate::Link::ReplicateDB::startPullDelta() {
    deltaIO.set<chakra::replica::Replicate::Link::ReplicateDB, &chakra::replica::Replicate::Link::ReplicateDB::onPullDelta>(this);
    deltaIO.set(ev::get_default_loop());
    deltaIO.start(FLAGS_replica_delta_pull_interval_sec);
}

void chakra::replica::Replicate::Link::ReplicateDB::onPullDelta(ev::timer& watcher, int event) {
    if (state != chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED) return;
    
    proto::replica::DeltaMessageRequest deltaMessageRequest;
    deltaMessageRequest.set_db_name(name);
    deltaMessageRequest.set_seq(deltaSeq);
    deltaMessageRequest.set_size(FLAGS_replica_delta_batch_bytes);
    link->asyncSendMsg(deltaMessageRequest, proto::types::R_DELTA_REQUEST);
    DLOG(INFO) << "[replication] pull delta message from  " << link->getPeerName() 
                    << " success " << deltaMessageRequest.DebugString();
}


void chakra::replica::Replicate::Link::ReplicateDB::startSendBulk() {
    transferIO.set<chakra::replica::Replicate::Link::ReplicateDB, &chakra::replica::Replicate::Link::ReplicateDB::onSendBulk>(this);
    transferIO.set(ev::get_default_loop());
    transferIO.start(FLAGS_replica_bulk_send_interval_sec);
}

void chakra::replica::Replicate::Link::ReplicateDB::onSendBulk(ev::timer& watcher, int event) {
    if (state != State::REPLICA_TRANSFORING ||
        bulkiter == nullptr) return;
    
    proto::replica::BulkMessage bulkMessage;
    bulkMessage.set_seq(deltaSeq);
    bulkMessage.set_db_name(name);

    int size = 0, num = 0;
    while (bulkiter->Valid()) {
        size += bulkiter->key().size();
        size += bulkiter->value().size();
        if (size > FLAGS_replica_bulk_batch_bytes)
            break;
        auto data = bulkMessage.mutable_kvs()->Add();
        data->set_key(bulkiter->key().ToString());
        data->set_value(bulkiter->value().ToString());
        bulkiter->Next();
        num++;
    }

    itsize += bulkMessage.kvs_size();
    bulkMessage.set_count(itsize);

    if (bulkiter->Valid()){
        bulkMessage.set_end(false);
        startSendBulk(); // next
    } else {
        transferIO.stop();
        bulkMessage.set_end(true);
        state = State::REPLICA_TRANSFORED;
        delete bulkiter;
        bulkiter = nullptr;
        auto dbptr = chakra::database::FamilyDB::get();
        dbptr->releaseSnapshot(name, snapshot);
    }
    
    if (size > 0 || bulkMessage.end()) { /* 可能出现某个DB为空，此时size=0 */
        transferTimes++;
        link->asyncSendMsg(bulkMessage, proto::types::R_BULK);
        if (bulkMessage.end())
            LOG(INFO) << "[replication] send full db " << name << " to " << link->getPeerName() << " success with " 
                      << itsize << " kv and " << transferTimes << " times.";
    }
}

void chakra::replica::Replicate::Link::ReplicateDB::reset() {
    close();
    name = "";
    itsize = 0;
    lastTransferMs = 0;
    link = nullptr;
    deltaSeq = 0;
}

void chakra::replica::Replicate::Link::ReplicateDB::close() {
    deltaIO.stop();
    transferIO.stop();
    state = chakra::replica::Replicate::Link::State::REPLICA_INIT;
    if (bulkiter != nullptr) {
        delete bulkiter;
        bulkiter = nullptr;
    }
}

chakra::replica::Replicate::Link::ReplicateDB::~ReplicateDB() { if (bulkiter != nullptr) { delete bulkiter; bulkiter = nullptr; } }