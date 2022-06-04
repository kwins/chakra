//
// Created by kwins on 2021/5/13.
//

#include "peer.h"
#include "peer.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "cmds/command_pool.h"
#include <cstdint>
#include <error/err.h>
#include <ev++.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

DECLARE_int32(server_port);

const std::string &chakra::cluster::Peer::getName() const { return name; }
const std::string &chakra::cluster::Peer::getIp() const { return ip; }
int chakra::cluster::Peer::getPort() const { return this->port; }
int chakra::cluster::Peer::getReplicatePort() { return this->port + 1; }
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
bool chakra::cluster::Peer::isMyself(uint64_t flag) { return flag & FLAG_MYSELF; }
bool chakra::cluster::Peer::isHandShake(uint64_t flag) { return flag & FLAG_HANDSHAKE; }
bool chakra::cluster::Peer::isMeet(uint64_t flag) { return flag & FLAG_MEET; }
bool chakra::cluster::Peer::isPfail(uint64_t flag) { return flag & FLAG_PFAIL; }
bool chakra::cluster::Peer::isFail(uint64_t flag) { return flag & FLAG_FAIL; }
void chakra::cluster::Peer::updateMetaDB(const std::string &dbname, const proto::peer::MetaDB &st) { dbs[dbname] = st; }
void chakra::cluster::Peer::removeMetaDB(const std::string &dbname) { dbs.erase(dbname); }
long chakra::cluster::Peer::createTimeMs() const { return ctime; }
void chakra::cluster::Peer::setCreatTimeMs(long millsec) { ctime = millsec; }
bool chakra::cluster::Peer::connected() const { return link != nullptr && link->connected(); }

bool chakra::cluster::Peer::connect() {
    try {
        delete link; // 删除之前的 link
        link = new Link(ip, port, shared_from_this());
    } catch (const std::exception& e) {
        link = nullptr; // ~Link
        if (!retryLinkTime)
            retryLinkTime = utils::Basic::getNowMillSec();
        LOG(ERROR) << "[cluster] connect peer " << getName()
                   << "[" << ip + ":" << port << "]"
                   <<" retry time " << retryLinkTime << " error " << e.what();
        return false;
    }
    retryLinkTime = 0;
    link->startEvRead();
    return true;
}

const std::unordered_map<std::string, proto::peer::MetaDB> &chakra::cluster::Peer::getPeerDBs() const { return dbs; }

void chakra::cluster::Peer::dumpPeer(proto::peer::PeerState& peerState) {
    peerState.set_name(name);
    peerState.set_ip(ip);
    peerState.set_port(port);
    peerState.set_epoch(epoch);
    peerState.set_flag(fg);
    for (auto& it : dbs) {
        auto db = peerState.mutable_dbs();
        (*db)[it.first] = it.second;
        // db->CopyFrom(it.second);
    }
}

long chakra::cluster::Peer::getLastPingSend() const { return last_ping_send; }
void chakra::cluster::Peer::setLastPingSend(long lastPingSend) { last_ping_send = lastPingSend; }
long chakra::cluster::Peer::getLastPongRecv() const { return last_pong_recv; }
void chakra::cluster::Peer::setLastPongRecv(long lastPongRecv) { last_pong_recv = lastPongRecv; }

void chakra::cluster::Peer::sendMsg(google::protobuf::Message & msg, proto::types::Type type) {
    link->asyncSendMsg(msg, type);
}

void chakra::cluster::Peer::linkFree() {
    if (link) {
        link->close();
    }
}

void chakra::cluster::Peer::addFailReport(const std::shared_ptr<Peer>& sender) {
    auto it = failReports.find(sender->getName());
    if (it != failReports.end()){
        it->second->time = utils::Basic::getNowMillSec();
        LOG(INFO) << "[cluster] sender " << sender->getName() << " already in fail reports, just update report time.";
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

bool chakra::cluster::Peer::servedDB(const std::string &dbname) {
    auto it = dbs.find(dbname);
    if (it != dbs.end() && it->second.state() == proto::peer::MetaDB_State_ONLINE){
        return true;
    }
    return false;
}

long chakra::cluster::Peer::getFailTime() const { return failTime; }

void chakra::cluster::Peer::setFailTime(long failtime) { Peer::failTime = failtime; }

std::string chakra::cluster::Peer::desc() {
    std::string str;
    str.append("ip: ").append(getIp()).append("\n");
    str.append("cport: ").append(std::to_string(getPort())).append("\n");
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

chakra::cluster::Peer::~Peer() {
    linkFree();
    if (link != nullptr) {
       delete link;
        link = nullptr;     
    }
}

long chakra::cluster::Peer::getRetryLinkTime() const { return retryLinkTime; }

void chakra::cluster::Peer::setRetryLinkTime(long timeMs) { this->retryLinkTime = timeMs; }

chakra::cluster::Peer::Link::Link(int sockfd) : chakra::net::Link(sockfd) {}

chakra::cluster::Peer::Link::Link(const std::string &ip, int port, const std::shared_ptr<Peer>& peer)
: chakra::net::Link(ip,port){
    reletedPeer = peer;
}

void chakra::cluster::Peer::Link::onPeerRead(ev::io &watcher, int event) {
    try {
        conn->receive([this](char *req, size_t rl) {

            proto::types::Type msgType = chakra::net::Packet::type(req, rl);
            DLOG(INFO) << "[cluster] received message type "
                    << proto::types::Type_Name(msgType) << ":" << msgType << " FROM " <<  conn->remoteAddr();

            auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);
            cmdsptr->execute(req, rl, this);
            return error::Error();
        });
    } catch (const error::ConnectClosedError& e1) {
        onReadError();
    } catch (const error::Error& e) {
        onReadError();
        LOG(ERROR) << "[cluster] i/o error " << e.what();
    }
}

void chakra::cluster::Peer::Link::asyncSendMsg(::google::protobuf::Message& msg, proto::types::Type type) {
    DLOG(INFO) << "[cluster] async send message type " << proto::types::Type_Name(type) << ":" << type << " to default loop";
    conn->writeBuffer(msg, type);
    wio.set<chakra::cluster::Peer::Link, &chakra::cluster::Peer::Link::onPeerWrite>(this);
    wio.set(ev::get_default_loop());
    wio.start(conn->fd(), ev::WRITE);
}

void chakra::cluster::Peer::Link::onPeerWrite(ev::io& watcher, int event) {
    DLOG(INFO) << "[cluster] on peer write data len " << conn->sendBufferLength();
    try {
        conn->sendBuffer();
    } catch (const error::ConnectClosedError& err) {
    } catch (const std::exception& err) {
        LOG(ERROR) << err.what();
    }
    wio.stop();
}

void chakra::cluster::Peer::Link::onReadError() {
    close();
    // 这里直接 delete this，因为后面不会再使用到此对象
    if (!reletedPeer) delete this; 
}

void chakra::cluster::Peer::Link::startEvRead() {
    rio.set<chakra::cluster::Peer::Link, &chakra::cluster::Peer::Link::onPeerRead>(this);
    rio.set(ev::get_default_loop());
    rio.start(conn->fd(), ev::READ);
}

void chakra::cluster::Peer::updateSelf(const proto::peer::GossipSender &sender) {
    setEpoch(sender.config_epoch());
    for(auto& metadb : sender.meta_dbs()) {
        updateMetaDB(metadb.name(), metadb);
    }
}

chakra::cluster::Peer::Link::~Link() {
    close();
    if (reletedPeer) reletedPeer = nullptr;
}
