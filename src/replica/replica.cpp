//
// Created by kwins on 2021/6/9.
//

#include "replica.h"
#include "utils/basic.h"
#include <glog/logging.h>
#include <thread>
#include <netinet/in.h>
#include "net/network.h"
#include "utils/file_helper.h"

// replica
void chakra::replica::Replica::initReplica(chakra::replica::Replica::Options options) {
    LOG(INFO) << "Replica init";
    opts = std::move(options);
    LOG(INFO) << "Replica init 1";
    auto err = loadLinks();
    if (err) LOG(WARNING) << "REPL load links " << err.toString();
    int sd = -1;
    LOG(INFO) << "Replica init 2";
    err = net::Network::tpcListen(7293, 512, sd);
    if (err){
        LOG(ERROR) << "REPL listen on " << this->opts.ip << ":" << this->opts.port << " " << strerror(errno);
        exit(1);
    }
    LOG(INFO) << "Replica init 3";
    replicaio.set<chakra::replica::Replica, &chakra::replica::Replica::onAccept>(this);
    replicaio.set(ev::get_default_loop());
    replicaio.start(sd, ev::READ);
    LOG(INFO) << "Replica init 4";
    startReplicaCron();
}

void chakra::replica::Replica::startReplicaCron() {
    cronIO.set<chakra::replica::Replica, &chakra::replica::Replica::onReplicaCron>(this);
    cronIO.set(ev::get_default_loop());
    cronIO.start(opts.cronInterval);
}

chakra::utils::Error chakra::replica::Replica::loadLinks() {
    LOG(INFO) << "REPL load";
    nlohmann::json j;
    LOG(INFO) << "REPL load 1";
   std::string filename = this->opts.dir + "/" + REPLICA_FILE_NAME;
    LOG(INFO) << "REPL load 2";
    auto err = utils::FileHelper::loadFile(filename, j);
    if (err){
        // TODO: error code judge
        return err;
    }

    auto primarys = j.at("replicas");
    for (int i = 0; i < primarys.size(); ++i) {
        auto link = std::make_shared<chakra::replica::Link>();
        link->loadLink(j[i]);
        primaryDBLinks.push_back(link);
    }

    return utils::Error();
}

std::shared_ptr<chakra::replica::Replica> chakra::replica::Replica::get() {
    static auto replicaptr = std::make_shared<Replica>();
    return replicaptr;
}

// 多主复制策略
// 因为是多主模式，这里并没有选择主流选举加同步首次全量和后续增量的方式，
// 而是采用同时向所有的在线节点发起复制请求，每个节点只返回自己的分片数据
// 复制者拿到各分片首次全量后，再merge到一起得到一个首次全量数据，再继续复制各分片的增量数据。
// 这样做的方式简化了集群的操作。
// 如果采用选举的方式，则需要在拿到首次全量后，还要进行各节点之间沟通，获取到首次全量之后的增量位置信息，这增加了复杂度。
void chakra::replica::Replica::onReplicaCron(ev::timer &watcher, int event) {
    LOG(INFO) << "REPL cron...";
    cronLoops++;
    for(auto& link : primaryDBLinks){
        link->replicaEventLoop();
    }

    if (cronLoops % 10 == 0)
        dumpLinks();

    // primary side
    replicaLinks.remove_if([](const Link* link){
        return link->isTimeout();
    });

    startReplicaCron();
}

void chakra::replica::Replica::dumpLinks() {
    std::string tofile = this->opts.dir + "/" + REPLICA_FILE_NAME;
    nlohmann::json j;
    for(auto& link : primaryDBLinks){
        j["replicas"].push_back(link->dumpLink());
    }
    auto err = chakra::utils::FileHelper::saveFile(j, tofile);
    if (err){
        LOG(ERROR) << "## REPL dump links error " << err.toString();
    } else {
        LOG(INFO) << "### REPL dump links to filename " << tofile << " success.";
    }
}

void chakra::replica::Replica::setReplicateDB(const std::string &name, const std::string &ip, int port) {
    chakra::replica::Link::Options options;
    options.ip = ip;
    options.port = port;
    options.dbName = name;
    options.cronInterval = opts.cronInterval;
    options.replicaTimeoutMs = opts.replicaTimeoutMs;
    options.dir = opts.dir;
    auto link = std::make_shared<chakra::replica::Link>(options);
    primaryDBLinks.push_back(link);
}

void chakra::replica::Replica::onAccept(ev::io &watcher, int event) {
    sockaddr_in addr{};
    socklen_t slen = sizeof(addr);
    int sockfd = ::accept(watcher.fd, (sockaddr*)&addr, &slen);
    if (sockfd < 0){
        LOG(ERROR) << "REPL on accept error" << strerror(errno);
        return;
    }

    chakra::replica::Link::Options options;
    options.sockfd = sockfd;
    options.dir = opts.dir;
    auto link = new Link(options);
    link->startReplicaRecv();
    replicaLinks.push_back(link);
}

void chakra::replica::Replica::stop() {
    replicaio.stop();
    cronIO.stop();
    replicaLinks.remove_if([](chakra::replica::Link* link){
        link->close();
        delete link;
        return true;
    });

    replicaLinks.remove_if([](chakra::replica::Link* link){
        link->close();
        delete link;
        return true;
    });
}

const std::string chakra::replica::Replica::REPLICA_FILE_NAME = "replicas.json";