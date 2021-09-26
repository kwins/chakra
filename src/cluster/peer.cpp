//
// Created by kwins on 2021/5/13.
//

#include "peer.h"
#include "peer.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "cmds/command_pool.h"
#include <glog/logging.h>
#include "replica/replica.h"

const std::string &chakra::cluster::Peer::getName() const { return name; }

const std::string &chakra::cluster::Peer::getIp() const { return ip; }

int chakra::cluster::Peer::getPort() const { return port; }

uint64_t chakra::cluster::Peer::getFg() const { return fg; }

uint64_t chakra::cluster::Peer::getEpoch() const { return epoch; }

void chakra::cluster::Peer::setIp(const std::string &ip) { this->ip = ip; }

void chakra::cluster::Peer::setPort(int port) { this->port = port; }

void chakra::cluster::Peer::setName(const std::string &peerName) { this->name = peerName; }

void chakra::cluster::Peer::setEpoch(uint64_t eh) { this->epoch = eh; }

void chakra::cluster::Peer::setFlag(uint64_t flag) { this->fg |= flag; }

void chakra::cluster::Peer::delFlag(uint64_t flag) { this->fg &= ~flag; }

bool chakra::cluster::Peer::isMyself() const { return this->fg & FLAG_MYSELF; }

bool chakra::cluster::Peer::isHandShake() const { return this->fg & FLAG_HANDSHAKE; }

bool chakra::cluster::Peer::isMeet() const { return this->fg & FLAG_MEET; }

bool chakra::cluster::Peer::isPfail() const { return this->fg & FLAG_PFAIL; }

bool chakra::cluster::Peer::isFail() const { return this->fg & FLAG_FAIL; }

bool chakra::cluster::Peer::isMyself(uint64_t flag) {
    return flag & FLAG_MYSELF;
}

bool chakra::cluster::Peer::isHandShake(uint64_t flag) {
    return flag & FLAG_HANDSHAKE;
}

bool chakra::cluster::Peer::isMeet(uint64_t flag) {
    return flag & FLAG_MEET;
}

bool chakra::cluster::Peer::isPfail(uint64_t flag) {
    return flag & FLAG_PFAIL;
}

bool chakra::cluster::Peer::isFail(uint64_t flag) {
    return flag & FLAG_FAIL;
}

void chakra::cluster::Peer::setDB(const std::string &name, const chakra::cluster::Peer::DB &st) { dbs[name] = st; }

void chakra::cluster::Peer::removeDB(const std::string &name) { dbs.erase(name); }

long chakra::cluster::Peer::createTimeMs() { return ctime; }

void chakra::cluster::Peer::setCreatTimeMs(long millsec) { this->ctime = millsec; }

bool chakra::cluster::Peer::connected() const {
    return link != nullptr && link->connected();
}

bool chakra::cluster::Peer::connect() {
    try {
        link = new Link(ip, port, shared_from_this());
    } catch (std::exception& e) {
        link = nullptr; // ~Link
        if (!retryLinkTime)
            retryLinkTime = utils::Basic::getNowMillSec();
        LOG(ERROR) << "Connect peer " << getName() << "[" << ip + ":" << port << "] retry time " << retryLinkTime << " error " << e.what();
        return false;
    }
    retryLinkTime = 0;
    link->startEvRead();
    return true;
}

const std::unordered_map<std::string, chakra::cluster::Peer::DB> &chakra::cluster::Peer::getPeerDBs() const { return dbs; }

void chakra::cluster::Peer::dumpPeer(proto::peer::PeerState& peerState) {
    peerState.set_name(name);
    peerState.set_ip(ip);
    peerState.set_port(port);
    peerState.set_epoch(epoch);
    peerState.set_flag(fg);
    for (auto& it : dbs){
        auto db = peerState.mutable_dbs()->Add();
        db->set_name(it.second.name);
        db->set_memory(it.second.memory);
        db->set_shard(it.second.shard);
        db->set_shard_size(it.second.shardSize);
        db->set_cached(it.second.cached);
    }
}

long chakra::cluster::Peer::getLastPingSend() const { return last_ping_send; }

void chakra::cluster::Peer::setLastPingSend(long lastPingSend) { last_ping_send = lastPingSend; }

long chakra::cluster::Peer::getLastPongRecv() const { return last_pong_recv; }

void chakra::cluster::Peer::setLastPongRecv(long lastPongRecv) { last_pong_recv = lastPongRecv; }

chakra::utils::Error chakra::cluster::Peer::sendMsg(google::protobuf::Message & msg, proto::types::Type type) {
    return link->sendMsg(msg, type);
}

chakra::utils::Error chakra::cluster::Peer::sendSyncMsg(google::protobuf::Message &request, proto::types::Type type,
                                                        google::protobuf::Message &response) {
    return link->sendSyncMsg(request, type, response);
}

void chakra::cluster::Peer::linkFree() {
    if (link) {
        delete link;
        link = nullptr;
    }
}

void chakra::cluster::Peer::addFailReport(const std::shared_ptr<Peer>& sender) {
    auto it = failReports.find(sender->getName());
    if (it != failReports.end()){
        it->second->time = utils::Basic::getNowMillSec();
        LOG(INFO) << "sender " << sender->getName() << " already in fail reports, just update report time.";
    } else{
        std::shared_ptr<FailReport> report = std::make_shared<FailReport>();
        report->peer = sender;
        report->time = utils::Basic::getNowMillSec();
        failReports.emplace(sender->getName(), report);
    }
}

bool chakra::cluster::Peer::delFailReport(const std::shared_ptr<Peer> &sender) {
    auto it = failReports.find(sender->getName());
    if (it != failReports.end()){
        failReports.erase(it);
        return true;
    }
    return false;
}

size_t chakra::cluster::Peer::cleanFailReport(long timeOutMillSec) {
    long maxTime = timeOutMillSec * 2;
    long nowMillSec = utils::Basic::getNowMillSec();
    for(auto it = failReports.begin(); it != failReports.end(); ){
        if (nowMillSec - it->second->time > maxTime)
            failReports.erase(it++);
        else
            it++;
    }
    return failReports.size();
}

bool chakra::cluster::Peer::servedDB(const std::string &name) { return dbs.count(name) > 0; }

long chakra::cluster::Peer::getFailTime() const { return failTime; }

void chakra::cluster::Peer::setFailTime(long failTime) { Peer::failTime = failTime; }

std::string chakra::cluster::Peer::desc() {
    std::string str;
    str.append("ip: ").append(getIp()).append("\n");
    str.append("port: ").append(std::to_string(getPort())).append("\n");
    str.append("name: ").append(getName()).append("\n");
    str.append("last_ping_send: ").append(std::to_string(getLastPingSend())).append("\n");
    str.append("last_pong_recved: ").append(std::to_string(getLastPongRecv())).append("\n");
    str.append("create_time: ").append(std::to_string(createTimeMs())).append("\n");
    str.append("fail_time: ").append(std::to_string(getFailTime())).append("\n");
    str.append("epoch: ").append(std::to_string(getEpoch())).append("\n");
    str.append("has_link: ").append(std::to_string(link != nullptr)).append("\n");
    str.append("is_handshake: ").append(std::to_string(isHandShake())).append("\n");
    str.append("is_meet: ").append(std::to_string(isMeet())).append("\n");
    str.append("is_myself: ").append(std::to_string(isMyself())).append("\n");
    str.append("is_fail: ").append(std::to_string(isFail())).append("\n");
    str.append("is_pfail: ").append(std::to_string(isPfail())).append("\n");
    return str;
}

chakra::cluster::Peer::~Peer() { linkFree(); }

long chakra::cluster::Peer::getRetryLinkTime() const { return retryLinkTime; }

void chakra::cluster::Peer::setRetryLinkTime(long timeMs) { this->retryLinkTime = timeMs; }

chakra::cluster::Peer::Link::Link(int sockfd) : chakra::net::Link(sockfd) {}

chakra::cluster::Peer::Link::Link(const std::string &ip, int port, const std::shared_ptr<Peer>& peer)
: chakra::net::Link(ip,port){
    reletedPeer = peer;
}

void chakra::cluster::Peer::Link::onPeerRead(ev::io &watcher, int event) {
    auto link = static_cast<chakra::cluster::Peer::Link*>(watcher.data);

    auto err = link->conn->receivePack([&link](char *req, size_t reqlen) {

        proto::types::Type msgType = chakra::net::Packet::getType(req, reqlen);
        LOG(INFO) << "-- PEER received message type " << proto::types::Type_Name(msgType) << ":" << msgType;
        auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);
        utils::Error err;
        cmdsptr->execute(req, reqlen, link, [&link, &err](char *reply, size_t replylen) {
            proto::types::Type replyType = chakra::net::Packet::getType(reply, replylen);
            LOG(INFO) << "  PEER reply message type " << proto::types::Type_Name(replyType) << ":" << replyType;
            err = link->conn->send(reply, replylen);
            return err;
        });
        return err;
    });

    if (!err.success()){
        LOG(ERROR) << "I/O error remote addr " << link->conn->remoteAddr() << err.toString();
        if (link->reletedPeer){ // active
            link->reletedPeer->linkFree();
        } else{ // passitive
            delete link;
        }
    }
}

void chakra::cluster::Peer::Link::startEvRead() {
    rio.set<&chakra::cluster::Peer::Link::onPeerRead>(this);
    rio.set(ev::get_default_loop());
    rio.start(conn->fd(), ev::READ);
}

void chakra::cluster::Peer::updateSelf(const proto::peer::GossipSender &sender) {
    setEpoch(sender.config_epoch());
    auto replicaptr = replica::Replica::get();
    for(auto& metadb : sender.meta_dbs()){
        chakra::cluster::Peer::DB db;
        db.name = metadb.name();
        db.cached = metadb.cached();
        db.memory = metadb.memory();
        db.shardSize = metadb.shard_size();
        db.shard = metadb.shard();
        LOG(INFO) << "sender " << getName() << " set db " << db.name;
        setDB(db.name, db);

        // 在多主的集群中，当某个db增加一个副本时
        // 当前副本的所有节点需要立刻复制新增的副本节点，以保证db的最终的一致性
        if (!replicaptr->replicated(metadb.name())){
            replicaptr->setReplicateDB(metadb.name(), sender.ip(), sender.port() + 1);
            LOG(INFO) << "Cluster add new db copy" << metadb.name() << ", start replicate it from("
                      << sender.ip() << ":" << sender.port() + 1
                      << ").";
        }
    }
}

void chakra::cluster::Peer::stateDesc(proto::peer::PeerState &peerState) {
    peerState.set_flag(getFg());
    peerState.set_ip(getIp());
    peerState.set_port(getPort());
    peerState.set_name(getName());
    peerState.set_epoch(getEpoch());
    for(auto& info : dbs){
        auto db = peerState.mutable_dbs()->Add();
        db->set_name(info.second.name);
        db->set_cached(info.second.cached);
        db->set_shard_size(info.second.shardSize);
        db->set_shard(info.second.shard);
        db->set_memory(info.second.memory);
    }
}

chakra::cluster::Peer::Link::~Link() {
    LOG(INFO) << "### chakra::cluster::Peer::Link::~Link";
    close();
    if (reletedPeer) reletedPeer = nullptr;
}
