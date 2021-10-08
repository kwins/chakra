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
#include "database/db_family.h"

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
            selfStates.push_back(replica);
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

    std::unordered_map<std::string, std::list<std::shared_ptr<Link>>> replicatings;
    for(auto& link : primaryDBLinks){
        if (link->getState() == Link::State::CONNECTED){
            replicatings[link->getDbName()].push_back(link);
        }
    }

    auto clusptr = chakra::cluster::Cluster::get();
    for(auto& it : replicatings){
        auto peers = clusptr->getPeers(it.first);
        int num = std::count_if(peers.begin(), peers.end(), [](const std::shared_ptr<chakra::cluster::Peer>& peer){
            return !peer->isMyself();
        }); /* 除本节点外 某个db在集群中节点个数 */

        auto dbs = clusptr->getMyself()->getPeerDBs();
        auto db = dbs.find(it.first);
        if (db == dbs.end()){
            LOG(ERROR) << "not found db " << it.first << " in this node. ";
        } else if(it.second.size() == num
                  && db->second.state() != proto::peer::MetaDB_State_ONLINE){
            proto::peer::MetaDB info;
            info.CopyFrom(db->second);
            info.set_state(proto::peer::MetaDB_State_ONLINE);
            clusptr->updateMyselfDB(info);
            LOG(INFO) << "REPL set db " << it.first << " state online.";
        }
    }

    if (cronLoops % 10 == 0)
        dumpReplicaStates();

    // primary side
    replicaLinks.remove_if([](Link* link){
        if (!link->isTimeout() && link->getState() == Link::State::CONNECTED)
            link->heartBeat();
        return link->isTimeout();
    });

    replicaSelfDBs();

    startReplicaCron();
}

void chakra::replica::Replica::replicaSelfDBs() {
    auto& db = database::FamilyDB::get();
    for(auto& replica : selfStates){
        // TODO: replica self
    }
}

bool chakra::replica::Replica::replicated(const std::string &dbname, const std::string&ip, int port) {
    return std::any_of(primaryDBLinks.begin(), primaryDBLinks.end(), [dbname, ip, port](const std::shared_ptr<Link>& link){
       return link->getDbName() == dbname && ip == link->getIp() && port == link->getPort();
    });
}

void chakra::replica::Replica::dumpReplicaStates() {
    if (primaryDBLinks.empty() && selfStates.empty()) return;

    std::string tofile = FLAGS_replica_dir + "/" + REPLICA_FILE_NAME;
    proto::replica::ReplicaStates replicaState;
    for(auto& link : primaryDBLinks){
        auto metaReplica = replicaState.mutable_replicas()->Add();
        (*metaReplica) = link->dumpLink();
    }
    for(auto& replica : selfStates){
        auto info = replicaState.mutable_replicas()->Add();
        info->CopyFrom(replica);
    }
    auto err = chakra::utils::FileHelper::saveFile(replicaState, tofile);
    if (!err.success()){
        LOG(ERROR) << "REPL dump links error " << err.what();
    } else {
        LOG(INFO) << "REPL dump links to filename " << tofile << " success.";
    }
}

void chakra::replica::Replica::setReplicateDB(const std::string &name, const std::string &ip, int port) {
    chakra::replica::Link::PositiveOptions options;
    options.ip = ip;
    options.port = port;
    options.dbName = name;
    options.dir = FLAGS_replica_dir;
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