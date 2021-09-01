//
// Created by kwins on 2021/6/23.
//

#include "chakra.h"
#include <glog/logging.h>
#include "net/packet.h"
#include "cmds/command_pool.h"
#include <thread>
#include "cluster/view.h"
#include <netinet/in.h>
#include "replica/replica.h"

// Chakra
std::shared_ptr<chakra::serv::Chakra> chakra::serv::Chakra::get() {
    auto chakraptr = std::make_shared<chakra::serv::Chakra>();
    return chakraptr;
}

void chakra::serv::Chakra::initCharka(const chakra::serv::Chakra::Options &opts) {
    // workers
    this->opts = opts;
    workNum = sysconf(_SC_NPROCESSORS_CONF) * 2 * 2 - 2;
    workNum = 1;
    workers.reserve(workNum);
    LOG(INFO) << "Chakra init word num:" << workNum;

    for (int i = 0; i < workNum; ++i) {
        std::thread([this, i]{

            std::unique_lock<std::mutex> lck(mutex);
            this->successWorkers++;
            this->mutex.unlock();
            workers[i] = new chakra::serv::Chakra::Worker();
            workers[i]->workID = i;
            workers[i]->async.set<&chakra::serv::Chakra::Worker::onAsync>(workers[i]);
            workers[i]->async.set(workers[i]->loop);
            workers[i]->async.start();

            this->cond.notify_one();
            this->workers[i]->startUp(i); // loop in here until server stop

            std::unique_lock<std::mutex> lck1(mutex);
            this->exitSuccessWorkers++;
            this->mutex.unlock();
            this->cond.notify_one();
        }).detach();
    }

    LOG(INFO) << "Chakra init 1";
    std::unique_lock<std::mutex> lck2(mutex);
    while (successWorkers < workNum){
        cond.wait(lck2);
    }
    LOG(INFO) << "Chakra init 4";

    chakra::cluster::View::get()->initView(opts.clusterOpts);
    LOG(INFO) << "Chakra init 5";
    chakra::replica::Replica::get()->initReplica(opts.replicaOpts);
    LOG(INFO) << "Chakra init 6";
    // sig
    initLibev();
    LOG(INFO) << "Chakra init 7";
}

void chakra::serv::Chakra::initLibev() {
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

//    startServCron();
}

void chakra::serv::Chakra::onSignal(ev::sig & sig, int event) {
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
    LOG(INFO) << "Chakra accept OK, dispatch connect to work " << index << " addr:" << link->conn->remoteAddr();
}

void chakra::serv::Chakra::startServCron() {
    cronIO.set<Chakra, &chakra::serv::Chakra::onServCron>(this);
    cronIO.set(ev::get_default_loop());
    cronIO.start(opts.cronInterval);
}

void chakra::serv::Chakra::onServCron(ev::timer &watcher, int event) {

    startServCron();
}

void chakra::serv::Chakra::startUp() {
    ev::get_default_loop().loop();
}

void chakra::serv::Chakra::stop() {
    ioCt.stop();
    cronIO.stop();
    for(auto worker : workers){
        worker->stop();
    }

    chakra::cluster::View::get()->stop();
    chakra::replica::Replica::get()->stop();
}

chakra::serv::Chakra::~Chakra() {
    for(auto worker : workers){
        delete worker;
    }
}


// Link
chakra::serv::Chakra::Link::Link(int sockfd) {
    conn = std::make_shared<net::Connect>(net::Connect::Options{ .fd = sockfd });
}

void chakra::serv::Chakra::Link::onPeerRead(ev::io &watcher, int event) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(watcher.data);

    auto err = link->conn->receivePack([&link](char *req, size_t reqlen) {

        proto::types::Type msgType = chakra::net::Packet::getType(req, reqlen);
        LOG(INFO) << "-- CLIENT received message type " << proto::types::Type_Name(msgType) << ":" << msgType;
        auto cmdsptr = cmds::CommandPool::get()->fetch(msgType);

        cmdsptr->execute(req, reqlen, link, [&link](char *reply, size_t replylen) {
            proto::types::Type replyType = chakra::net::Packet::getType(reply, replylen);
            LOG(INFO) << "  CLIENT reply message type " << proto::types::Type_Name(replyType) << ":" << replyType;
            link->conn->send(reply, replylen);
        });
    });

    if (err){
        LOG(ERROR) << "I/O error remote addr " << link->conn->remoteAddr() << err.toString();
        delete link;
    }
}

void chakra::serv::Chakra::Link::startEvRead(chakra::serv::Chakra::Worker* worker) {
    rio = std::make_shared<ev::io>();
    rio->set<&chakra::serv::Chakra::Link::onPeerRead>(this);
    rio->set(worker->loop);
    rio->start(conn->fd(), ev::READ);
}

void chakra::serv::Chakra::Link::close() const {
    LOG(INFO) << "### chakra::serv::Chakra::Link::close";
    if (rio){
        rio->stop();
    }
    if (conn)
        conn->close();
}

chakra::serv::Chakra::Link::~Link() {
    LOG(INFO) << "### chakra::serv::Chakra::Link::~Link";
    close();
    if (rio) rio = nullptr;
    if (conn) conn = nullptr;
}


void chakra::serv::Chakra::Worker::startUp(int id) {
    workID = id;
    LOG(INFO) << "Chakra worker " << id << " start up.";
    loop.loop();
    LOG(INFO) << "Chakra worker " << id << " break.";
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

void chakra::serv::Chakra::Worker::stop() {
    this->async.stop();
    if (!this->links.empty()) {
        for (auto link : this->links) {
            delete link;
        }
    }
    this->loop.break_loop(ev::ALL);
}

chakra::serv::Chakra::Worker::~Worker() { stop(); }


