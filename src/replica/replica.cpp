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
#include <gflags/gflags.h>
#include "cluster/cluster.h"
#include "replica.pb.h"
#include "error/err_file_not_exist.h"

DECLARE_int32(cluster_port);
DECLARE_string(replica_dir);
DECLARE_int32(replica_tcp_back_log);
DECLARE_int32(replica_timeout_ms);
DECLARE_double(replica_cron_interval_sec);
DECLARE_int32(replica_timeout_retry);
DECLARE_double(replica_delta_pull_interval_sec);
DECLARE_double(replica_bulk_send_interval_sec);

chakra::replica::Replica::Replica() {
    LOG(INFO) << "Replica init";
    loadLinks(); // 加载配置数据
    auto err = net::Network::tpcListen(FLAGS_cluster_port + 1, FLAGS_replica_tcp_back_log, sfd);
    if (!err.success()){
        LOG(ERROR) << "REPL listen on " << FLAGS_cluster_port + 1 << " " << err.what();
        exit(1);
    }
    replicaio.set<chakra::replica::Replica, &chakra::replica::Replica::onAccept>(this);
    replicaio.set(ev::get_default_loop());
    replicaio.start(sfd, ev::READ);
    startReplicaCron();
    LOG(INFO) << "REPL listen on " << FLAGS_cluster_port + 1;
}

void chakra::replica::Replica::startReplicaCron() {
    cronIO.set<chakra::replica::Replica, &chakra::replica::Replica::onReplicaCron>(this);
    cronIO.set(ev::get_default_loop());
    cronIO.start(FLAGS_replica_cron_interval_sec);
}

void chakra::replica::Replica::loadLinks() {
    LOG(INFO) << "REPL load";
    std::string filename = FLAGS_replica_dir + "/" + REPLICA_FILE_NAME;
    try {
        proto::replica::ReplicaStates replicaStates;
        utils::FileHelper::loadFile(filename, replicaStates);
        for(auto& replica : replicaStates.replicas()){
            chakra::replica::Link::PositiveOptions options;
            options.ip = replica.ip();
            options.port = replica.port();
            options.dbName = replica.db_name();
            options.dir = FLAGS_replica_dir;
            auto link = std::make_shared<chakra::replica::Link>(options);
            link->setRocksSeq(replica.delta_seq());
            link->setPeerName(replica.primary());
            primaryDBLinks.push_back(link);
        }

        for(auto& replica : replicaStates.selfs()){
            auto selfLink = std::make_shared<chakra::replica::LinkSelf>(replica.db_name(), replica.delta_seq());
            selfDBLinks.push_back(selfLink);
        }
    } catch (error::FileNotExistError& err) {
        return;
    } catch (std::exception& err) {
        LOG(ERROR) << "REPL load links from " << filename << " error " << err.what();
        exit(-1);
    }
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
    cronLoops++;
    for(auto& link : primaryDBLinks)
        link->replicaEventHandler();

    if (cronLoops % 10 == 0)
        dumpReplicaStates();

    // primary side
    replicaLinks.remove_if([](Link* link){
        if (!link->isTimeout() && link->getState() == Link::State::CONNECTED)
            link->heartBeat();
        return link->isTimeout();
    });

    startReplicaCron();
}

std::list<std::shared_ptr<chakra::replica::Link>> &chakra::replica::Replica::getPrimaryDBLinks() {return primaryDBLinks; }

bool chakra::replica::Replica::hasReplicated(const std::string &dbname, const std::string&ip, int port) {
    return std::any_of(primaryDBLinks.begin(), primaryDBLinks.end(), [dbname, ip, port](const std::shared_ptr<Link>& link){
       return link->getDbName() == dbname && ip == link->getIp() && port == link->getPort();
    });
}

bool chakra::replica::Replica::hasReplicated(const std::string &dbname) {
    return std::any_of(selfDBLinks.begin(), selfDBLinks.end(), [dbname](const std::shared_ptr<LinkSelf>& link){
        return link->getDbName() == dbname;
    });
}


void chakra::replica::Replica::dumpReplicaStates() {
    if (primaryDBLinks.empty() && selfDBLinks.empty()) return;

    std::string tofile = FLAGS_replica_dir + "/" + REPLICA_FILE_NAME;
    proto::replica::ReplicaStates replicaState;
    for(auto& link : primaryDBLinks){
        auto metaReplica = replicaState.mutable_replicas()->Add();
        (*metaReplica) = link->dumpLink();
    }
    for(auto& self : selfDBLinks){
        auto info = replicaState.mutable_selfs()->Add();
        info->set_delta_seq(self->getDeltaSeq());
        info->set_db_name(self->getDbName());
    }
    auto err = chakra::utils::FileHelper::saveFile(replicaState, tofile);
    if (!err.success()){
        LOG(ERROR) << "REPL dump links error " << err.what();
    } else {
        LOG(INFO) << "REPL dump links to filename " << tofile << " success.";
    }
}

void chakra::replica::Replica::setReplicateDB(const std::string &dbname, const std::string &ip, int port) {
    chakra::replica::Link::PositiveOptions options;
    options.ip = ip;
    options.port = port;
    options.dbName = dbname;
    options.dir = FLAGS_replica_dir;
    auto link = std::make_shared<chakra::replica::Link>(options);
    primaryDBLinks.push_back(link);
    dumpReplicaStates();
}

void chakra::replica::Replica::setReplicateDB(const std::string &dbname) {
    selfDBLinks.push_back(std::make_shared<chakra::replica::LinkSelf>(dbname));
    dumpReplicaStates();
}

void chakra::replica::Replica::onAccept(ev::io &watcher, int event) {
    sockaddr_in addr{};
    socklen_t slen = sizeof(addr);
    int sockfd = ::accept(watcher.fd, (sockaddr*)&addr, &slen);
    if (sockfd < 0){
        LOG(ERROR) << "REPL on accept error" << strerror(errno);
        return;
    }

    chakra::replica::Link::NegativeOptions options;
    options.sockfd = sockfd;
    options.dir = FLAGS_replica_dir;
    auto link = new Link(options);
    link->startReplicaRecv();
    replicaLinks.push_back(link);
}

void chakra::replica::Replica::stop() {
    if (sfd != -1) ::close(sfd);

    replicaio.stop();
    cronIO.stop();
    replicaLinks.remove_if([](chakra::replica::Link* link){
        link->close();
        delete link;
        return true;
    });
}

const std::string chakra::replica::Replica::REPLICA_FILE_NAME = "replicas.json";