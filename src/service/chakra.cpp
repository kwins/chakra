//
// Created by kwins on 2021/6/23.
//

#include "chakra.h"
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

DECLARE_string(server_ip);
DECLARE_int32(server_port);
DECLARE_int32(server_tcp_backlog);
DECLARE_double(server_cron_interval_sec);

chakra::serv::Chakra::Chakra() {
    workNum = sysconf(_SC_NPROCESSORS_CONF) * 2 * 2 - 1;
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
    chakra::replica::Replica::get();
    // sig
    initLibev();
}

// Chakra
std::shared_ptr<chakra::serv::Chakra> chakra::serv::Chakra::get() {
    auto chakraptr = std::make_shared<chakra::serv::Chakra>();
    return chakraptr;
}

void chakra::serv::Chakra::initLibev() {
    // listen
    sfd = -1;
    auto err = net::Network::tpcListen(FLAGS_server_port ,FLAGS_server_tcp_backlog, sfd);
    if (!err.success()){
        LOG(ERROR) << "Chakra listen on " << FLAGS_server_ip << ":" << FLAGS_server_port << " " << err.what();
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
    sigkill.set<&chakra::serv::Chakra::onSignal>(this);
    sigkill.set(ev::get_default_loop());
    sigkill.start(SIGKILL);

    startServCron();
}

void chakra::serv::Chakra::onSignal(ev::sig & sig, int event) {
    LOG(INFO) << "Chakra on signal";
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
        LOG(ERROR) << "Chakra on accept error" << strerror(errno);
        return;
    }

    long index = (connNums++) % workNum;
    auto link = new chakra::serv::Chakra::Link(sockfd);
    workers[index]->links.push_back(link);
    workers[index]->async.send();
    LOG(INFO) << "Chakra accept OK, dispatch connect to work " << index << " addr:" << link->conn->remoteAddr();
}

void chakra::serv::Chakra::startServCron() {
    cronIO.set<Chakra, &chakra::serv::Chakra::onServCron>(this);
    cronIO.set(ev::get_default_loop());
    cronIO.start(FLAGS_server_cron_interval_sec);
}

void chakra::serv::Chakra::onServCron(ev::timer &watcher, int event) {
    auto replicaptr = replica::Replica::get();
    auto clusptr = chakra::cluster::Cluster::get();

    /*  在多主的集群中，当在集群中增加一个副本时
    副本节点需要获取所有其他副本的写数据，并Merge到自己的全局rocksdb中，以保证数据和其他副本节点的最终一致性。
    因为要获取多个节点的写数据，所以当有新副本加入集群，需要检查是否已经完成历史数据Merge，并且开始增量复制。
    当所有的副本节点历史数据都已Merge完成，历史数据保持一致后，则通知集群新的副本上线。*/
    std::unordered_map<std::string, std::list<std::shared_ptr<replica::Link>>> replicatings;
    for(auto& link : replicaptr->getPrimaryDBLinks()){
        if (link->getState() == replica::Link::State::CONNECTED){
            replicatings[link->getDbName()].push_back(link);
        }
    }

    for(auto& it : replicatings){
        auto peers = clusptr->getPeers(it.first);
        int num = std::count_if(peers.begin(), peers.end(), [](const std::shared_ptr<chakra::cluster::Peer>& peer){
            return !peer->isMyself();
        }); /* 除本节点外 某个db在集群中节点个数 */

        auto dbs = clusptr->getMyself()->getPeerDBs();
        auto db = dbs.find(it.first);
        if (db == dbs.end()){
            LOG(ERROR) << "Not found db " << it.first << " in this node. ";
        } else if(it.second.size() == num
                  && db->second.state() != proto::peer::MetaDB_State_ONLINE){
            /* 复制成功的个数 等于 集群中副本个数(除自己外的) 且 当前db状态为非ONLINE */
            proto::peer::MetaDB info;
            info.CopyFrom(db->second);
            info.set_state(proto::peer::MetaDB_State_ONLINE);
            clusptr->updateMyselfDB(info);
            LOG(INFO) << "Replicate set db " << it.first << " state online to cluster.";
        }
    }

    /* 检查是否有新的副本上线，触发复制流程
    新的副本上线，通知到集群后，其他副本节点同样也需要向新的副本节点发起复制请求
    以获取新副本节点的写数据，保证数据的一致性 */
    for(auto& it : clusptr->getPeers()){
        auto& peer = it.second;
        if (peer->isMyself()) continue;
        for(auto& db : peer->getPeerDBs()){
            auto& metaDB = db.second;
            if (clusptr->getMyself()->servedDB(metaDB.name())  /* 当前节点处理正在处理这个db */
                && metaDB.state() == proto::peer::MetaDB_State_ONLINE /* db 处于 online 状态*/
                && !replicaptr->replicated(metaDB.name(), peer->getIp(), peer->getPort() + 1)){ /* 当前节点没有复制这个db */

                replicaptr->setReplicateDB(metaDB.name(), peer->getIp(), peer->getPort() + 1);
                LOG(INFO) << "Cluster add db " << metaDB.name() << " new copy, starting replicate from("
                          << peer->getIp() << ":" << (peer->getPort() + 1)
                          << ").";
            }
        }
    }

    // TODO: replica self
    /* self replica
     *
     * */

    startServCron();
}

void chakra::serv::Chakra::startUp() const {
    LOG(INFO) << "Chakra start works " << workNum;
    LOG(INFO) << "Chakra start and listen on "
              << FLAGS_server_ip << ":" << FLAGS_server_port << " success.";
    ev::get_default_loop().loop();
}

void chakra::serv::Chakra::stop() {
    LOG(INFO) << "Chakra stop";
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
    chakra::replica::Replica::get()->stop();
    ev::get_default_loop().break_loop(ev::ALL);
}

chakra::serv::Chakra::~Chakra() {
    LOG(INFO) << "~Chakra";
    for(auto worker : workers){
        delete worker;
    }
}


// Link
chakra::serv::Chakra::Link::Link(int sockfd) : chakra::net::Link(sockfd) {}

void chakra::serv::Chakra::Link::onPeerRead(ev::io &watcher, int event) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(watcher.data);

    auto err = link->conn->receivePack([&link](char *req, size_t reqlen) {

        proto::types::Type msgType = chakra::net::Packet::getType(req, reqlen);
        DLOG(INFO) << "-- CLIENT received message type "
                  << proto::types::Type_Name(msgType) << ":" << msgType;

        auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);
        error::Error err;
        cmdsptr->execute(req, reqlen, link, [&link, &err](char *reply, size_t replylen) {
            proto::types::Type replyType = chakra::net::Packet::getType(reply, replylen);
            DLOG(INFO) << "   CLIENT reply message type " << proto::types::Type_Name(replyType) << ":" << replyType;
            err = link->conn->send(reply, replylen);
            return err;
        });
        return err;
    });

    if (!err.success()){
        LOG(ERROR) << "I/O error remote addr " << link->conn->remoteAddr() << err.what();
        delete link;
    }
}

void chakra::serv::Chakra::Link::startEvRead(chakra::serv::Chakra::Worker* worker) {
    LOG(INFO) << "Chakra Link start read in worker " << workID;
    rio.set<&chakra::serv::Chakra::Link::onPeerRead>(this);
    rio.set(worker->loop);
    rio.start(conn->fd(), ev::READ);
}


chakra::serv::Chakra::Link::~Link() {
    LOG(INFO) << "### chakra::serv::Chakra::Link::~Link";
    close();
}


void chakra::serv::Chakra::Worker::startUp(int id) {
    workID = id;
//    LOG(INFO) << "Chakra worker " << id << " start up.";
    loop.loop();
//    LOG(INFO) << "Chakra worker " << id << " break.";
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
        for (auto link : worker->links)
            delete link;
    }
    watcher.loop.break_loop(ev::ALL);
}

void chakra::serv::Chakra::Worker::stop() { this->stopAsnyc.send(); }

chakra::serv::Chakra::Worker::~Worker() {
    stop();
}


