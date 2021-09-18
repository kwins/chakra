//
// Created by kwins on 2021/5/17.
//

#include "cluster.h"
#include "utils/basic.h"
#include <nlohmann/json.hpp>
#include <glog/logging.h>
#include "net/network.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <chrono>
#include "peer.pb.h"
#include <random>
#include "utils/file_helper.h"
#include <gflags/gflags.h>

DEFINE_string(cluster_dir, "data", "cluster dir");                                              /* NOLINT */
DEFINE_string(cluster_ip, "127.0.0.1", "cluster ip");                                           /* NOLINT */
DEFINE_int32(cluster_port, 7291, "cluster port");                                               /* NOLINT */
DEFINE_int32(cluster_handshake_timeout_ms, 10000, "cluster handshake timeout ms");              /* NOLINT */
DEFINE_int32(cluster_peer_timeout_ms, 10000, "cluster peer timeout ms");                        /* NOLINT */
DEFINE_int32(cluster_peer_link_retry_timeout_ms, 10000, "cluster peer link retry timeout ms");  /* NOLINT */
DEFINE_double(cluster_cron_interval_sec, 1.0, "cluster cron interval sec");                     /* NOLINT */
DEFINE_int32(cluster_tcp_back_log, 512, "cluster tcp back log");                                /* NOLINT */

chakra::cluster::Cluster::Cluster() {
    LOG(INFO) << "Cluster init dir " << FLAGS_cluster_dir;
    state = STATE_FAIL;
    cronLoops = 0;
    seed = std::make_shared<std::default_random_engine>(time(nullptr));
    auto err = loadConfigFile();
    if (!err.success() && !err.is(utils::Error::ERR_FILE_NOT_EXIST)){
        LOG(ERROR) << "Cluster exit with error " << err.toString();
        exit(-1);
    } else if (err.is(utils::Error::ERR_FILE_NOT_EXIST)){
        err = utils::FileHelper::mkDir(FLAGS_cluster_dir);
        if (!err.success()){
            LOG(ERROR) << "Cluster mkdir dir error " << err.toString();
            exit(-1);
        }
        myself = std::make_shared<Peer>(); // 第一次初始化
        myself->setIp(FLAGS_cluster_ip);
        myself->setPort(FLAGS_cluster_port);
        myself->setName(utils::Basic::genRandomID());
        myself->setFlag(Peer::FLAG_MYSELF);
        peers.insert(std::make_pair(this->myself->getName(), this->myself));
        LOG(INFO) << "Cluster init first, peers size:" << this->peers.size();
    }
    startEv();
    LOG(INFO) << "Cluster listen in " << FLAGS_cluster_ip << ":" << FLAGS_cluster_port << " success, myself is " << myself->getName();
}

chakra::utils::Error chakra::cluster::Cluster::loadConfigFile() {
    std::string filename = FLAGS_cluster_dir + "/" + PEERS_FILE;
    proto::peer::ClusterState clusterState;
    auto err = utils::FileHelper::loadFile(filename, clusterState);
    if (!err.success()) return err;

    LOG(INFO) << "clusterState :" << clusterState.DebugString();

    currentEpoch = clusterState.current_epoch();
    state = clusterState.state();
    for(auto& info : clusterState.peers()){
        auto it = peers.find(info.name());
        if (it != peers.end()){
            LOG(WARNING) << "Cluster init duplicate peer " << info.name();
            continue;
        }
        auto peer = std::make_shared<Peer>();
        peer->setName(info.name());
        peer->setIp(info.ip());
        peer->setPort(info.port());
        peer->setEpoch(info.epoch());
        peer->setFlag(info.flag());
        for(auto& dbinfo : info.dbs()){
            Peer::DB db;
            db.name = dbinfo.name();
            db.memory = dbinfo.memory();
            db.shard = dbinfo.shard();
            db.shardSize = dbinfo.shard_size();
            db.cached = dbinfo.cached();
            peer->setDB(db.name, db);
        }

        peers.emplace(info.name(), peer);
        if (peer->isMyself()) { myself = peer; }
    }
    return err;
}

// 初次调用 必 需要先调用 initView
std::shared_ptr<chakra::cluster::Cluster> chakra::cluster::Cluster::get() {
    static std::shared_ptr<Cluster> viewptr = std::make_shared<Cluster>();
    return viewptr;
}

void chakra::cluster::Cluster::startEv() {
    // ev listen and accept
    auto err = net::Network::tpcListen(FLAGS_cluster_port ,FLAGS_cluster_tcp_back_log, sfd);
    if (!err.success() || sfd == -1){
        LOG(ERROR) << "Cluster listen on " << FLAGS_cluster_ip << ":" << FLAGS_cluster_port << " " << err.toString();
        exit(1);
    }
    acceptIO.set(ev::get_default_loop());
    acceptIO.set<Cluster, &Cluster::onAccept>(this);
    acceptIO.start(sfd, ev::READ);
    startPeersCron();
}

void chakra::cluster::Cluster::startPeersCron() {
    cronIO.set<chakra::cluster::Cluster, &chakra::cluster::Cluster::onPeersCron>(this);
    cronIO.set(ev::get_default_loop());
    cronIO.start(FLAGS_cluster_cron_interval_sec);
}


void chakra::cluster::Cluster::onPeersCron(ev::timer &watcher, int event) {
    iteraion++;
    long nowMillSec = utils::Basic::getNowMillSec();
    // 向集群中的所有断线或者未连接节点发送消息
    for(auto it = peers.begin(); it != peers.end();){
        auto peer = it->second;
        if (peer->isMyself()) {
            it++;
            continue;
        }

        // 如果 handshake 节点已超时，释放它
        auto spends = nowMillSec - peer->createTimeMs();
        if (it->second->isHandShake() && spends > FLAGS_cluster_handshake_timeout_ms){
            peers.erase(it++);
            LOG(WARNING) << "Connect peer " << peer->getIp() + ":" << peer->getPort()
                         << " timeout, delete peer from cluster.";
            continue;
        }

        // 为未创建连接的节点创建连接
        if (!peer->connected() && peer->connect()) {
            // 向新节点发送 Meet
            long oldPingSent = peer->getLastPingSend();
            sendPingOrMeet(peer, proto::types::P_MEET_PEER);
            if (oldPingSent){
                peer->setLastPingSend(oldPingSent);
            }
            peer->delFlag(Peer::FLAG_MEET);
            LOG(INFO) << "Connecting peer " << peer->getName()
                      << " at " << peer->getIp() << ":" << peer->getPort()
                      << " success, send handshake message to it.";
        }
        it++;
    }

    // 向一个随机节点发送 gossip 信息
    if (!(iteraion % 10)){
        std::shared_ptr<Peer> minPingPeer = nullptr;
        std::vector<std::string> times;
        for (int i = 0; i < 5; ++i) {
            auto randPeer = randomPeer();
            if (!randPeer->connected() || randPeer->getLastPingSend() != 0)
                continue;
            if (randPeer->isMyself() || randPeer->isHandShake())
                continue;

            if (!minPingPeer || minPingPeer->getLastPongRecv() > randPeer->getLastPongRecv()){
                minPingPeer = randPeer;
                times.push_back(minPingPeer->getName() + "[" + std::to_string(minPingPeer->getPort()) + "]" + ":" + std::to_string(minPingPeer->getLastPongRecv()));
            }
        }

        if (minPingPeer){
            LOG(INFO) << "Find min pong peer " << minPingPeer->getName() << ":" << minPingPeer->getLastPongRecv() << ", Try to send PING.";
            sendPingOrMeet(minPingPeer, proto::types::P_PING);
        }
    }

    // 遍历所有节点，检查是否需要将某个节点标记为下线
    for(auto & it : peers){
        auto peer = it.second;

        if (peer->isMyself() || peer->isHandShake())
            continue;

        // 如果等到 PONG 到达的时间超过了 node timeout 一半的连接
        // 因为尽管节点依然正常，但连接可能已经出问题了
        // 希望通过重连能够快速感知到对端节点是否有问题
        if (peer->connected() /* 连接正常 */
                && nowMillSec - peer->createTimeMs() > FLAGS_cluster_peer_timeout_ms /* 没有重连 */
                && peer->getLastPingSend() /* 发送过 PING */
                && peer->getLastPongRecv() < peer->getLastPingSend() /* 等待 PONG 中 */
                && nowMillSec - peer->getLastPingSend() > FLAGS_cluster_peer_timeout_ms/2  /* 等待Pong超过超时的一半时间 */
                && !peer->isPfail()
                && !peer->isFail()  /* 节点不能为 FAIL or PFAIL 状态 */
                ){
            // TODO: use other way fix this fail reason
            LOG(WARNING) << "*** NOTE peer "
                         << peer->getName()
                         << " connect ok, but always not received pong since " << peer->getLastPongRecv()
                         << ", try free link and reconnect";
            peer->linkFree(); // 下次重连
        }

        // 如果目前没有在 PING 节点
        // 并且已经有 node timeout 一半的时间没有从节点那里收到 PONG 回复
        // 那么向节点发送一个 PING ，确保节点的信息不会太旧
        // （因为一部分节点可能一直没有被随机中）
        if (peer->connected()
                && peer->getLastPingSend() == 0 /* 没发送PING */
                && (nowMillSec - peer->getLastPongRecv()) > FLAGS_cluster_peer_timeout_ms/2){
            sendPingOrMeet(peer, proto::types::P_PING);
            LOG(WARNING) << "*** NOTE peer "
                         << peer->getName()
                         << " connect ok, but not send PING for a long time " << peer->getLastPongRecv()
                         << ", try to send ping to weak up it.(" << (nowMillSec - peer->getLastPongRecv()) << ")";
            continue;
        }
        // PING 等到 PONG 超时
        // 重连失败超时
        long delay = nowMillSec - peer->getLastPingSend();
        long retryDelay = nowMillSec - peer->getRetryLinkTime();
        if ((peer->getLastPingSend() && delay > FLAGS_cluster_peer_timeout_ms)
                || (peer->getRetryLinkTime() && retryDelay > FLAGS_cluster_peer_link_retry_timeout_ms)) {
            if (!peer->isFail() && !peer->isPfail()) {
                LOG(INFO) << "*** NOTE peer " << peer->getName() << " possibly failing delay=" << delay
                             << "ms, peer time out config is " << FLAGS_cluster_peer_timeout_ms << " ms.";
                peer->setFlag(Peer::FLAG_PFAIL);
                setCronTODO(FLAG_UPDATE_STATE | FLAG_SAVE_CONFIG);
            }
        }
    }

    if (FLAG_UPDATE_STATE & cronTodo){
        updateClusterState();
        cronTodo &= ~FLAG_UPDATE_STATE;
    }

    if (FLAG_SAVE_CONFIG & cronTodo){
        if(dumpPeers())
            cronTodo &= ~FLAG_SAVE_CONFIG;
    }
    startPeersCron();
}

void chakra::cluster::Cluster::onAccept(ev::io &watcher, int event) {
    sockaddr_in addr{};
    socklen_t slen = sizeof(addr);
    int sd = ::accept(watcher.fd, (sockaddr*)&addr, &slen);
    if (sd < 0){
        LOG(ERROR) << "Cluster on read peer error" << strerror(errno);
        return;
    }

    (new chakra::cluster::Peer::Link(sd))->startEvRead();
}

void chakra::cluster::Cluster::stop() {
    if (sfd != -1) ::close(sfd);
    dumpPeers();
    acceptIO.stop();
    cronIO.stop();
    if (!peers.empty())
        peers.clear();
    if (myself)
        myself = nullptr;
}

void chakra::cluster::Cluster::addPeer(const std::string &ip, int port) {
    std::shared_ptr<Peer> peer = std::make_shared<Peer>();
    peer->setName(utils::Basic::genRandomID()); // 先随机一个名字，后续握手时再更新其正在的名字
    peer->setIp(ip);
    peer->setPort(port);
    peer->setEpoch(0);
    peer->setCreatTimeMs(utils::Basic::getNowMillSec());
    peer->setLastPingSend(0);
    peer->setLastPongRecv(0);
    peer->setFlag(Peer::FLAG_MEET | Peer::FLAG_HANDSHAKE);
    peers.emplace(peer->getName(), peer);
}

bool chakra::cluster::Cluster::dumpPeers() {
    std::string filename = FLAGS_cluster_dir + "/" + PEERS_FILE;
    proto::peer::ClusterState clusterState;
    clusterState.set_current_epoch(currentEpoch);
    clusterState.set_state(state);
    for (auto& it : peers){
        auto info = clusterState.mutable_peers()->Add();
        it.second->dumpPeer(*info);
    }

    auto err = utils::FileHelper::saveFile(clusterState, filename);
    if (!err.success()){
        LOG(ERROR) << "Cluster dump dbs error " << strerror(errno);
    }
    else {
        LOG(INFO) << "Cluster dump dbs to filename " << filename << " success.";
    }
    return true;
}

int chakra::cluster::Cluster::getCurrentEpoch() const { return currentEpoch; }

void chakra::cluster::Cluster::setCurrentEpoch(int epoch) { currentEpoch = epoch; }

uint64_t chakra::cluster::Cluster::getMaxEpoch() {
    uint64_t epoch = 0;
    for(auto& peer : peers){
        if (epoch < peer.second->getEpoch()){
            epoch = peer.second->getEpoch();
        }
    }
    return epoch;
}

int chakra::cluster::Cluster::getState() const { return state; }

void chakra::cluster::Cluster::setState(int state) { Cluster::state = state; }

void chakra::cluster::Cluster::buildGossipSeader(proto::peer::GossipSender *sender, const std::string& data) {
    sender->set_ip(myself->getIp());
    sender->set_name(myself->getName());
    sender->set_data(data);
    sender->set_port(myself->getPort());
    sender->set_config_epoch(myself->getEpoch());
    sender->set_current_epoch(getCurrentEpoch());
    sender->set_state(getState());

    for(auto& it : myself->getPeerDBs()){
        auto slot =  sender->mutable_meta_dbs()->Add();
        slot->set_name(it.second.name);
        slot->set_shard(it.second.shard);
        slot->set_shard_size(it.second.shardSize);
        slot->set_cached(it.second.cached);
        slot->set_memory(it.second.memory);
    }
}

void chakra::cluster::Cluster::buildGossipMessage(proto::peer::GossipMessage &gossip, const std::string& data) {
    auto sender = gossip.mutable_sender();
    buildGossipSeader(sender, data);

    int freshnodes = (int)peers.size() - 2;
    int gossipcount = 0;
    while (freshnodes > 0 && gossipcount < 3){
        auto randPeer = randomPeer();
//        LOG(INFO) << "## random name "<< randPeer->getName() << " condition=" << (!randPeer->connected() && randPeer->getPeerDBs().empty() && !randPeer->isPfail() && !randPeer->isFail());
        if (randPeer == myself
            || randPeer->isHandShake()
            || (!randPeer->connected() && randPeer->getPeerDBs().empty() && !randPeer->isPfail() && !randPeer->isFail())){
            freshnodes--;
            continue;
        }

        bool hasin = false;
        for (int i = 0; i < gossip.peers_size(); ++i) {
            if (gossip.peers(i).peer_name() == randPeer->getName()){
                hasin = true;
                break;
            }
        }
        if (hasin) continue;

        freshnodes--;
        auto gossipPeer = gossip.mutable_peers()->Add();
        gossipPeer->set_peer_name(randPeer->getName());
        gossipPeer->set_ip(randPeer->getIp());
        gossipPeer->set_port(randPeer->getPort());
        gossipPeer->set_last_ping_send(randPeer->getLastPingSend());
        gossipPeer->set_last_pong_recved(randPeer->getLastPongRecv());
        gossipPeer->set_config_epoch(randPeer->getEpoch());
        gossipPeer->set_flag(randPeer->getFg());
        for(auto& it : randPeer->getPeerDBs()){
            auto db = gossipPeer->mutable_meta_dbs()->Add();
            db->set_name(it.second.name);
            db->set_shard(it.second.shard);
            db->set_shard_size(it.second.shardSize);
            db->set_cached(it.second.cached);
            db->set_memory(it.second.memory);
        }
        gossipcount++;
    }
}

void chakra::cluster::Cluster::sendPingOrMeet(std::shared_ptr<Peer> peer, proto::types::Type type) {
    proto::peer::GossipMessage gossip;

    if (type == proto::types::P_MEET_PEER){
        buildGossipMessage(gossip, peer->getName());
    } else{
        buildGossipMessage(gossip);
    }

    // 记录下ping的时间点，
    if (type == proto::types::P_PING){
        auto millSec = utils::Basic::getNowMillSec();
        peer->setLastPingSend(millSec);
    }

//    LOG(INFO) << "Send message " << gossip.DebugString();
    // 发送消息
    peer->sendMsg(gossip, type);
}

std::shared_ptr<chakra::cluster::Peer> chakra::cluster::Cluster::randomPeer() {
    std::uniform_int_distribution<int> dist(0, peers.size()-1);
    int step = dist(*this->seed);
    return std::next(peers.begin(), step)->second;
}

void chakra::cluster::Cluster::updateClusterState() {
    int newState = STATE_OK, emptyPeer = 0, unReachablePeer = 0, size = 0;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Peer>>>  dbPeers;
    for (auto& it : peers){
        if (it.second->getPeerDBs().empty()){
            emptyPeer++;
        } else{
            size++;
        }
        if (it.second->isFail() || it.second->isPfail()){
            unReachablePeer++;
        }

        for(auto& db : it.second->getPeerDBs()){
            dbPeers[db.first].push_back(it.second);
        }
    }
    // 1、如果集群中任意节点挂掉，且节点服务的DB中有任意一个DB没有副本，则集群进入fail
    for(auto&it : dbPeers){
        int fails = 0;
        for(auto& peer : it.second){
            if (peer->isFail() || peer->isPfail()){
                fails++;
            }
        }
        if (fails == it.second.size()){
            newState = STATE_FAIL;
        }
    }
    // 2、如果集群中挂掉的节点数量超过半数，则集群进入fail
    if (unReachablePeer >= (size/2) +1){
        newState = STATE_FAIL;
    }
    if (newState != state) {
        state = newState;
        LOG(INFO) << "Change state from " << state << " to " << newState;
    }
}

std::shared_ptr<chakra::cluster::Peer> chakra::cluster::Cluster::getPeer(const std::string& name) {
    auto it =  peers.find(name);
    if (it != peers.end()) return it->second;
    return nullptr;
}

std::shared_ptr<chakra::cluster::Peer> chakra::cluster::Cluster::getPeer(const std::string &ip, int port) {
    for(auto& peer : peers){
        if (peer.second->getIp() == ip && peer.second->getPort() == port){
            return peer.second;
        }
    }
    return nullptr;
}

std::vector<std::shared_ptr<chakra::cluster::Peer>> chakra::cluster::Cluster::getPeers(const std::string &dbName) {
    std::vector<std::shared_ptr<chakra::cluster::Peer>> dbPeers;
    for(auto& it : peers){
        if (it.second->servedDB(dbName)){
            dbPeers.push_back(it.second);
        }
    }
    return std::move(dbPeers);
}

size_t chakra::cluster::Cluster::size() { return peers.size(); }

// 分析并取出消息中的 gossip 节点信息, 非sender
void chakra::cluster::Cluster::processGossip(const proto::peer::GossipMessage &gsp) {
    auto sender = getPeer(gsp.sender().name());

    for (int i = 0; i < gsp.peers_size(); ++i) {
        auto peer = getPeer(gsp.peers(i).peer_name());
        if (peer){ // 已存在
            // 报告节点处于 FAIL 或者 PFAIL 状态
            // 尝试将 node 标记为 FAIL
            if (sender && peer != myself){
                if (Peer::isPfail(gsp.peers(i).flag()) || Peer::isFail(gsp.peers(i).flag())){
                    peer->addFailReport(sender); // sender report peer fail and add to fail report list.
                    tryMarkFailPeer(peer);
                    setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
                    LOG(WARNING) << "*** NOTE sender " << sender->getName() << " report peer " << peer->getName() << " as not reachable.";
                } else{
                    if (peer->delFailReport(sender)){
                        setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
                        LOG(INFO) << "*** NOTE sender " << sender->getName() << " reported peer " << peer->getName() << " is back online.";
                    }
                }
            }

            // 如果节点之前处于 PFAIL 或者 FAIL 状态
            // 并且该节点的 IP 或者端口号已经发生变化
            // 那么可能是节点换了新地址，尝试对它进行握手
            if ((Peer::isPfail(gsp.peers(i).flag()) || Peer::isFail(gsp.peers(i).flag()))
                    && (peer->getIp() != gsp.peers(i).ip() || peer->getPort() != gsp.peers(i).port())){
                // start handshake
                addPeer(gsp.peers(i).ip(), gsp.peers(i).port());
                setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
            }

        } else{

            // 注意，当前节点必须保证 sender 是本集群的节点
            // 否则我们将有加入了另一个集群的风险。
            // start handshake
            if (sender){
                addPeer(gsp.peers(i).ip(), gsp.peers(i).port());
                setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
                LOG(INFO) << "Gossip message ADD peer " << gsp.peers(i).peer_name() << "[" + gsp.peers(i).ip() + ":" << gsp.peers(i).port() << "]";
            }
        }
    }
}

void chakra::cluster::Cluster::tryMarkFailPeer(const std::shared_ptr<Peer> &peer) {
    LOG(INFO) << "Try mark peer " << peer->getName() << " fail(" << peer->isFail() << ").";
    if (peer->isFail()) return;
    size_t needQuorum = peers.size()/2+1;
    size_t failCount = peer->cleanFailReport(FLAGS_cluster_peer_timeout_ms);

    // 当前节点也算在内
    if (failCount + 1 < needQuorum) return;

    LOG(WARNING) << "Marking peer " << peer->getName() << " as failing(quorum " << needQuorum << " reached).";
    peer->delFlag(Peer::FLAG_PFAIL);
    peer->setFlag(Peer::FLAG_FAIL);
    peer->setFailTime(utils::Basic::getNowMillSec());

    // 广播节点FAIL消息，更新其他节点信息
    sendFail(peer->getName());
}

void chakra::cluster::Cluster::sendFail(const std::string &failPeerName) {
    proto::peer::FailMessage failMessage;
    buildGossipSeader(failMessage.mutable_sender());
    failMessage.set_fail_peer_name(failPeerName);
    broadcastMessage(failMessage, proto::types::P_FAIL);
}

void chakra::cluster::Cluster::broadcastMessage(google::protobuf::Message &peerMsg, proto::types::Type type) {
    for (auto& it : peers){
        if (!it.second->connected()) continue;
        if (it.second->isMyself() || it.second->isHandShake()) continue;

        it.second->sendMsg(peerMsg, type);
    }
}

std::shared_ptr<chakra::cluster::Peer> chakra::cluster::Cluster::renamePeer(const std::string &random, const std::string &real) {
    auto it = peers.find(random);
    if (it == peers.end()) return nullptr;

    // 新的name假如集群
    auto peer = it->second;
    if (random != real){
        LOG(INFO) << "Rename peer name from " << peer->getName() << " to " << real;
        peer->setName(real);
        peers.insert(std::make_pair(real, peer));
        // 删除老的name
        peers.erase(it);
    }
    return peer;
}

std::shared_ptr<chakra::cluster::Peer> chakra::cluster::Cluster::getMyself() { return myself; }

void chakra::cluster::Cluster::setCronTODO(uint64_t todo) { cronTodo |= todo; }
