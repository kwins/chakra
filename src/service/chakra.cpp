//
// Created by kwins on 2021/6/23.
//

#include "chakra.h"
#include <cstring>
#include <error/err.h>
#include <ev++.h>
#include <exception>
#include <glog/logging.h>
#include "net/packet.h"
#include "cmds/command_pool.h"
#include <thread>
#include "cluster/cluster.h"
#include <netinet/in.h>
#include "replica/replica.h"
#include "net/network.h"
#include "database/db_family.h"
#include <gflags/gflags.h>
#include <utils/basic.h>

DECLARE_int32(server_tcp_backlog);
DECLARE_double(server_cron_interval_sec);
DECLARE_int32(server_workers);

chakra::serv::Chakra::Chakra() {
    LOG(INFO) << "[chakra] init";
    workNum = FLAGS_server_workers > 1 ? FLAGS_server_workers - 1 : 1;
    workers.reserve(workNum);
    for (int i = 0; i < workNum; ++i) {
        workers[i] = new chakra::serv::Chakra::Worker();
        std::thread([this, i]{
            std::unique_lock<std::mutex> lck(mutex);
            this->successWorkers++;
            lck.unlock();
            workers[i]->workID = i;
            workers[i]->async.set<&chakra::serv::Chakra::Worker::onAsync>(workers[i]);
            workers[i]->async.set(workers[i]->loop);
            workers[i]->async.start();
            workers[i]->stopAsnyc.set<&chakra::serv::Chakra::Worker::onStopAsync>(workers[i]);
            workers[i]->stopAsnyc.set(workers[i]->loop);
            workers[i]->stopAsnyc.start();

            this->cond.notify_one();
            this->workers[i]->startUp(i); // loop in here until server stop

            std::unique_lock<std::mutex> lck1(mutex);
            this->exitSuccessWorkers++;
            lck1.unlock();
            this->cond.notify_one();

        }).detach();
    }

    std::unique_lock<std::mutex> lck2(mutex);
    while (successWorkers < workNum){
        cond.wait(lck2);
    }

    // 初始化
    chakra::cluster::Cluster::get(); /* 集群先启动 */
    chakra::database::FamilyDB::get(); /* db 依赖集群数据 */
    chakra::replica::Replicate::get();
    // sig
    initLibev();
    LOG(INFO) << "[chakra] init end";
}

// Chakra
std::shared_ptr<chakra::serv::Chakra> chakra::serv::Chakra::get() {
    static auto chakraptr = std::make_shared<chakra::serv::Chakra>();
    return chakraptr;
}

chakra::serv::Chakra::Worker* chakra::serv::Chakra::getWorker(int id) { return workers[id]; }
void chakra::serv::Chakra::initLibev() {
    DLOG(INFO) << "[chakra] init lib ev";
    sfd = -1;
    auto err = net::Network::tpcListen(utils::Basic::sport() ,FLAGS_server_tcp_backlog, sfd);
    if (err) {
        LOG(ERROR) << "[chakra] listen on " << utils::Basic::sport() << " " << err.what();
        exit(1);
    }
    acceptIO.set(ev::get_default_loop());
    acceptIO.set<Chakra, &Chakra::onAccept>(this);
    acceptIO.start(sfd, ev::READ);

    // signal
    sigint.set<&chakra::serv::Chakra::onSignal>(this);
    sigint.set(ev::get_default_loop());
    sigint.start(SIGINT);
    sigterm.set<&chakra::serv::Chakra::onSignal>(this);
    sigterm.set(ev::get_default_loop());
    sigterm.start(SIGTERM);

    startServCron();
}

void chakra::serv::Chakra::onSignal(ev::sig & sig, int event) {
    DLOG(INFO) << "[chakra] on signal";
    sig.stop();
    auto serv = static_cast<chakra::serv::Chakra*>(sig.data);
    if (serv)
        serv->stop();
}

void chakra::serv::Chakra::onAccept(ev::io &watcher, int event) {
    sockaddr_in addr{};
    socklen_t slen = sizeof(addr);
    int sockfd = ::accept(watcher.fd, (sockaddr*)&addr, &slen);
    if (sockfd < 0){
        LOG(ERROR) << "[chakra] on accept error" << strerror(errno);
        return;
    }

    long index = (connNums++) % workNum;
    auto link = new chakra::serv::Chakra::Link(sockfd);
    workers[index]->links.push_back(link);
    workers[index]->async.send();
    DLOG(INFO) << "[chakra] accept ok, dispatch connect to work " << index << ", remote addr " << link->conn->remoteAddr();
}

void chakra::serv::Chakra::startServCron() {
    cronIO.set<Chakra, &chakra::serv::Chakra::onServCron>(this);
    cronIO.set(ev::get_default_loop());
    cronIO.start(FLAGS_server_cron_interval_sec);
}

void chakra::serv::Chakra::onServCron(ev::timer &watcher, int event) {
    auto replicaptr = replica::Replicate::get();
    auto clusptr = chakra::cluster::Cluster::get();

    /*  在多主的集群中，当在集群中增加一个副本时
    副本节点需要获取所有其他副本的写数据，并Merge到自己的全局rocksdb中，
    以保证数据和其他副本节点的最终一致性。
    因为要获取多个节点的写数据，所以当有新副本加入集群，
    需要检查是否已经完成历史数据Merge，并且开始增量复制。
    当所有的副本节点历史数据都已Merge完成，历史数据保持一致后，
    更新集群信息，通知集群新的副本上线。
    */
    auto replicatings = replicaptr->dbTransferedLinks();
    for(auto& it : replicatings) {
        auto peers = clusptr->getPeers(it.first);
        int num = peers.size();
        auto dbs = clusptr->getMyself()->getPeerDBs();
        auto db = dbs.find(it.first);
        if (db != dbs.end() && it.second.size() == num 
            && db->second.state() != proto::peer::MetaDB_State_ONLINE){
            /* 复制成功的个数 等于 集群中副本个数 且当前db状态为非ONLINE, 则更新db状态，通知集群 */
            proto::peer::MetaDB info;
            info.CopyFrom(db->second);
            info.set_state(proto::peer::MetaDB_State_ONLINE);
            clusptr->updateMyselfDB(info);
            LOG(INFO) << "[replication] set db " << it.first << " state online to cluster.";
        }
    }

    /* 检查是否有新的副本上线，触发复制流程
    新的副本上线，通知到集群后，其他副本节点同样也需要向新的副本节点发起复制请求
    以获取新副本节点的写数据，保证数据的一致性 */
    for(auto& it : clusptr->getPeers()) { /*  遍历所有节点 */
        for (auto pdb : it.second->getPeerDBs()) { /* 遍历所有节点下的所有DB */
            if (!clusptr->getMyself()->servedDB(pdb.second.name())) continue; /* 过滤自己没有的DB */

            if (pdb.second.state() != proto::peer::MetaDB_State_ONLINE) continue; /* 过滤非 online 状态的DB */

            if (replicaptr->replicatedDB(it.second->getName(), pdb.second.name())) continue; /* 过滤已经复制过的DB */
            
            replicaptr->setReplicateDB(it.second->getName(), 
                pdb.second.name(), it.second->getIp(), it.second->getReplicatePort()); /* 复制新的副本 */
            
            replicaptr->dumpReplicateStates(); /* 更新状态 */
            LOG(INFO) << "[replication] new copy online in this node (" 
                      << pdb.second.name() << "#" << it.second->getIp() << ":" << it.second->getReplicatePort() << ")";
        }
    }
    startServCron();
}

void chakra::serv::Chakra::startUp() const {
    LOG(INFO) << "[chakra] start works " << workNum;
    LOG(INFO) << "[chakra] listen on " << utils::Basic::sport();
    ev::get_default_loop().loop();
}

void chakra::serv::Chakra::stop() {
    LOG(INFO) << "[chakra] stop";
    if (sfd != -1) ::close(sfd);
    acceptIO.stop();
    cronIO.stop();
    exitSuccessWorkers = 0;
    for (int i = 0; i < workNum; ++i)
        workers[i]->stop();

    std::unique_lock<std::mutex> lck(mutex);
    while (exitSuccessWorkers < workNum){
        cond.wait(lck);
    }

    chakra::cluster::Cluster::get()->stop();
    chakra::replica::Replicate::get()->stop();
    chakra::database::FamilyDB::get()->stop();
    ev::get_default_loop().break_loop(ev::ALL);
    LOG(INFO) << "[server] stop";
}

chakra::serv::Chakra::~Chakra() {
    for(auto worker : workers){
        if (worker != nullptr) {
            delete worker;
        }
    }
}


// Link
chakra::serv::Chakra::Link::Link(int sockfd) : chakra::net::Link(sockfd) {}

void chakra::serv::Chakra::Link::onClientRead(ev::io &watcher, int event) {
    try {
        conn->receive([this](char *req, size_t rl) {

            proto::types::Type msgType = chakra::net::Packet::type(req, rl);
            DLOG(INFO) << "[chakra] client received message type "
                       << proto::types::Type_Name(msgType) << ":" << msgType;

            auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);
            cmdsptr->execute(req, rl, this);
            return error::Error();
        });
    } catch (const error::ConnectClosedError& e) {
        delete this;
    } catch (const error::Error& err) {
        LOG(ERROR) << "[chakra] i/o error " << err.what();
        delete this;
    }
}

void chakra::serv::Chakra::Link::onClientWrite(ev::io& watcher, int event) {
    DLOG(INFO) << "[chakra] on client write data len " << conn->sendBufferLength();
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

void chakra::serv::Chakra::Link::asyncSendMsg(const ::google::protobuf::Message& msg, proto::types::Type type) {
    DLOG(INFO) << "[chakra] async send message type " << proto::types::Type_Name(type) << ":" << type << " to work id " << workID;
    conn->writeBuffer(msg, type);
    auto worker = chakra::serv::Chakra::get()->getWorker(workID);
    if (wio.is_active()) return;
    wio.set<chakra::serv::Chakra::Link, &chakra::serv::Chakra::Link::onClientWrite>(this);
    wio.set(worker->loop);
    wio.start(conn->fd(), ev::WRITE);
}

void chakra::serv::Chakra::Link::startEvRead(chakra::serv::Chakra::Worker* worker) {
    rio.set<chakra::serv::Chakra::Link, &chakra::serv::Chakra::Link::onClientRead>(this);
    rio.set(worker->loop);
    rio.start(conn->fd(), ev::READ);
}

chakra::serv::Chakra::Link::~Link() { close(); }
void chakra::serv::Chakra::Worker::startUp(int id) {
    workID = id;
    loop.loop();
}

void chakra::serv::Chakra::Worker::onAsync(ev::async &watcher, int event) {
    auto worker = static_cast<chakra::serv::Chakra::Worker*>(watcher.data);
    while (!worker->links.empty()){
        auto link = worker->links.front();
        worker->links.pop_front();
        link->workID = worker->workID;
        link->startEvRead(worker);
        worker->linkNums++;
    }
}

void chakra::serv::Chakra::Worker::onStopAsync(ev::async &watcher, int events) {
    auto worker = static_cast<chakra::serv::Chakra::Worker*>(watcher.data);
    watcher.stop();
    if (!worker->links.empty()){
        for (auto link : worker->links) {
            if (link != nullptr) {
                delete link;
                link = nullptr;
            }
        }  
    }
    watcher.loop.break_loop(ev::ALL);
}

void chakra::serv::Chakra::Worker::stop() { this->stopAsnyc.send(); }
chakra::serv::Chakra::Worker::~Worker() { stop(); }


