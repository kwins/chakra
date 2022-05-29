//
// Created by kwins on 2021/5/17.
//

#include "cluster.h"
#include "utils/basic.h"
#include <glog/logging.h>
#include "net/network.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <chrono>
#include "peer.pb.h"
#include <random>
#include "utils/file_helper.h"
#include <gflags/gflags.h>
#include "database/db_family.h"
#include "error/err.h"

DECLARE_string(cluster_dir);
DECLARE_int32(cluster_port);
DECLARE_int32(cluster_handshake_timeout_ms);
DECLARE_int32(cluster_peer_timeout_ms);
DECLARE_int32(cluster_peer_link_retry_timeout_ms);
DECLARE_double(cluster_cron_interval_sec);
DECLARE_int32(cluster_tcp_back_log);

chakra::cluster::Cluster::Cluster() {
    LOG(INFO) << "[cluster] init dir " << FLAGS_cluster_dir;
    state = STATE_FAIL;
    cronLoops = 0;
    seed = std::make_shared<std::default_random_engine>(time(nullptr));
    loadPeers();
    startEv();
    updateClusterState();
    dumpMyselfDBs();
    LOG(INFO) << "[cluster] listen in " << FLAGS_cluster_port
              << " success, myself is " << myself->getName();
}

void chakra::cluster::Cluster::loadPeers() {
    try {
        proto::peer::ClusterState clusterState;
        utils::FileHelper::loadFile(FLAGS_cluster_dir + "/" + PEERS_FILE, clusterState);
        LOG(INFO) << "[cluster] state :" << clusterState.DebugString();
        currentEpoch = clusterState.current_epoch();
        state = clusterState.state();
        for(auto& info : clusterState.peers()){
            auto it = peers.find(info.first);
            if (it != peers.end()){
                LOG(WARNING) << "[cluster] init duplicate peer " << info.first;
                continue;
            }
            auto peer = std::make_shared<Peer>();
            peer->setName(info.second.name());
            peer->setIp(info.second.ip());
            peer->setPort(info.second.port());
            peer->setEpoch(info.second.epoch());
            peer->setFlag(info.second.flag());
            peer->setCreatTimeMs(utils::Basic::getNowMillSec());
            for(auto& dbinfo : info.second.dbs()){
                peer->updateMetaDB(dbinfo.first, dbinfo.second);
            }
            peers.emplace(info.first, peer);
            if (peer->isMyself()) { myself = peer; }
        }
    } catch (const error::FileError& err) {
        myself = std::make_shared<Peer>(); // 第一次初始化
        myself->setIp("127.0.0.1");
        myself->setPort(FLAGS_cluster_port);
        myself->setName(utils::Basic::genRandomID());
        myself->setFlag(Peer::FLAG_MYSELF);
        peers.insert(std::make_pair(myself->getName(), myself));
        LOG(INFO) << "[cluster] init first and peers size:" << peers.size();
    } catch (const std::exception& err) {
        LOG(ERROR) << "[cluster] exit with error " << err.what();
        exit(-1);
    }
}

// 初次调用 必 需要先调用 initView
std::shared_ptr<chakra::cluster::Cluster> chakra::cluster::Cluster::get() {
    static std::shared_ptr<Cluster> viewptr = std::make_shared<Cluster>();
    return viewptr;
}

void chakra::cluster::Cluster::startEv() {
    // ev listen and accept
    auto err = net::Network::tpcListen(FLAGS_cluster_port ,FLAGS_cluster_tcp_back_log, sfd);
    if (err || sfd == -1) {
        LOG(ERROR) << "[cluster] listening on " << FLAGS_cluster_port << " " << err.what();
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
            LOG(WARNING) << "[cluster] handshake peer " << peer->getIp() + ":" << peer->getPort()
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
            peer->delFlag(Peer::FLAG_MEET); // whether or not meet message
            LOG(INFO) << "[cluster] connecting peer " << peer->getName()
                      << " at " << peer->getIp() << ":" << peer->getPort()
                      << " success, send "
                      << proto::types::Type_Name(proto::types::P_MEET_PEER)
                      << " create time " << peer->createTimeMs()
                      << " message to it."
                      ;
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
            DLOG(INFO) << "[cluster] find min pong peer " << minPingPeer->getName() << ":" << minPingPeer->getLastPongRecv() << ", Try to send PING.";
            sendPingOrMeet(minPingPeer, proto::types::P_PING);
        }

        /* 定时一段时间保存集群信息 */
        updateClusterState();
        dumpPeers();
        dumpMyselfDBs();
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
                ) {
            // TODO: use other way fix this fail reason
            LOG(INFO) << "[cluster] NOTE peer "
                         << peer->getName()
                         << " connect ok, but always not received pong since " << peer->getLastPongRecv()
                         << ", try free link and reconnect(" << (nowMillSec - peer->getLastPingSend()) << "ms)";
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
            DLOG(INFO) << "[cluster] peer " << peer->getName()
                         << " connect ok, but not send PING for a long time " << peer->getLastPongRecv()
                         << ", try to send ping to weak up it.(" << (nowMillSec - peer->getLastPongRecv()) << "ms)";
            continue;
        }
        // PING 等到 PONG 超时
        // 重连失败超时
        long delay = nowMillSec - peer->getLastPingSend();
        long retryDelay = nowMillSec - peer->getRetryLinkTime();
        if ((peer->getLastPingSend() && delay > FLAGS_cluster_peer_timeout_ms)
                || (peer->getRetryLinkTime() && retryDelay > FLAGS_cluster_peer_link_retry_timeout_ms)) {
            if (!peer->isFail() && !peer->isPfail()) {
                LOG(INFO) << "[cluster] NOTE peer " << peer->getName() << " possibly failing delay=" << delay
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
        dumpPeers();
        dumpMyselfDBs();
        cronTodo &= ~FLAG_SAVE_CONFIG;
    }

    startPeersCron();
}

void chakra::cluster::Cluster::onAccept(ev::io &watcher, int event) {
    sockaddr_in addr{};
    socklen_t slen = sizeof(addr);
    int sd = ::accept(watcher.fd, (sockaddr*)&addr, &slen);
    if (sd < 0){
        LOG(ERROR) << "[cluster] on accept peer error" << strerror(errno);
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
    for (auto& it : peers) {
        auto info = clusterState.mutable_peers();
        proto::peer::PeerState peerState;
        it.second->dumpPeer(peerState);
        (*info)[it.first] = peerState;
    }

    auto err = utils::FileHelper::saveFile(clusterState, filename);
    if (err){
        LOG(ERROR) << "[cluster] dump peers error " << err.what();
    }else {
        DLOG(INFO) << "[cluster] dump peers to filename " << filename << " success.";
    }
    return true;
}

void chakra::cluster::Cluster::dumpMyselfDBs() {
    std::string filename = FLAGS_cluster_dir + "/dbs.json";
    proto::peer::MetaDBs metaDBs;
    for(auto& it : myself->getPeerDBs()){
        auto db = metaDBs.mutable_dbs()->Add();
        db->CopyFrom(it.second);
    }
    auto err = utils::FileHelper::saveFile(metaDBs, filename);
    if (err) {
        LOG(ERROR) << "[cluster] dump myself dbs error " << err.what();
    } else {
        DLOG(INFO) << "[cluster] dump myself dbs to filename " << filename << " success.";
    }
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
        auto info = it.second;
        auto db = sender->mutable_meta_dbs()->Add();
        db->CopyFrom(it.second);
    }
}

void chakra::cluster::Cluster::buildGossipMessage(proto::peer::GossipMessage &gossip, const std::string& data) {
    auto sender = gossip.mutable_sender();
    buildGossipSeader(sender, data);

    int freshnodes = (int)peers.size() - 2;
    int gossipcount = 0;
    while (freshnodes > 0 && gossipcount < 3){
        auto randPeer = randomPeer();
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
            db->CopyFrom(it.second);
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

    if (peers.size() < 3){
        newState = STATE_FAIL;
    }

    if (newState != state) {
        LOG(INFO) << "[cluster] state changed FROM " << state << " TO " << newState;
        state = newState;
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

std::unordered_map<std::string, std::shared_ptr<chakra::cluster::Peer>>&
chakra::cluster::Cluster::getPeers() { return peers; }

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
        if (peer) { // 已存在
            // 报告节点处于 FAIL 或者 PFAIL 状态
            // 尝试将 node 标记为 FAIL
            if (sender && peer != myself) {
                if (Peer::isPfail(gsp.peers(i).flag()) || Peer::isFail(gsp.peers(i).flag())){
                    peer->addFailReport(sender); // sender report peer fail and add to fail report list.
                    tryMarkFailPeer(peer);
                    setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
                    LOG(WARNING) << "[cluster] NOTE sender " << sender->getName() << " report peer " << peer->getName() << " as not reachable.";
                } else {
                    if (peer->delFailReport(sender)) {
                        setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
                        LOG(INFO) << "[cluster] NOTE sender " << sender->getName() << " reported peer " << peer->getName() << " is back online.";
                    }
                }
            }

            // 如果节点之前处于 PFAIL 或者 FAIL 状态
            // 并且该节点的 IP 或者端口号已经发生变化
            // 那么可能是节点换了新地址，尝试对它进行握手
            if ((Peer::isPfail(gsp.peers(i).flag()) || Peer::isFail(gsp.peers(i).flag()))
                    && (peer->getIp() != gsp.peers(i).ip() || peer->getPort() != gsp.peers(i).port())) {
                // start handshake
                addPeer(gsp.peers(i).ip(), gsp.peers(i).port());
                setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
            }

        } else {
            // 注意，当前节点必须保证 sender 是本集群的节点
            // 否则我们将有加入了另一个集群的风险。
            // start handshake
            if (sender){
                addPeer(gsp.peers(i).ip(), gsp.peers(i).port());
                setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
                LOG(INFO) << "[cluster] gossip message add peer " << gsp.peers(i).peer_name() << "[" + gsp.peers(i).ip() + ":" << gsp.peers(i).port() << "]";
            }
        }
    }
}

void chakra::cluster::Cluster::tryMarkFailPeer(const std::shared_ptr<Peer> &peer) {
    LOG(INFO) << "[cluster] try mark peer " << peer->getName() << " fail(" << peer->isFail() << ").";
    if (peer->isFail()) return;
    size_t needQuorum = peers.size()/2+1;
    size_t failCount = peer->cleanFailReport(FLAGS_cluster_peer_timeout_ms);

    // 当前节点也算在内
    if (failCount + 1 < needQuorum) return;

    LOG(WARNING) << "[cluster] marking peer " << peer->getName() << " as failing(quorum " << needQuorum << " reached).";
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
        LOG(INFO) << "[cluster] rename peer name from " << peer->getName() << " to " << real;
        peer->setName(real);
        peers.insert(std::make_pair(real, peer));
        // 删除老的name
        peers.erase(it);
    }
    return peer;
}

void chakra::cluster::Cluster::stateDesc(proto::peer::ClusterState& clusterState){
    clusterState.set_current_epoch(getCurrentEpoch());
    clusterState.set_state(getState());

    for(auto& it : peers){
        auto peer = clusterState.mutable_peers();
        proto::peer::PeerState peerState;
        it.second->dumpPeer(peerState);
        (*peer)[it.first] = peerState;
    }
}

bool chakra::cluster::Cluster::stateOK() const { return state == STATE_OK; }

void chakra::cluster::Cluster::updateMyselfDB(const proto::peer::MetaDB &metaDB) {
    getMyself()->updateMetaDB(metaDB.name(), metaDB);

    increasingMyselfEpoch();
}

void chakra::cluster::Cluster::increasingMyselfEpoch() {
    int maxEpoch = getMaxEpoch();
    if (maxEpoch >= getCurrentEpoch()){
        setCurrentEpoch(maxEpoch + 1);
    } else {
        setCurrentEpoch(getCurrentEpoch() + 1);
    }
    getMyself()->setEpoch(maxEpoch + 1);
    setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
}

std::shared_ptr<chakra::cluster::Peer> chakra::cluster::Cluster::getMyself() { return myself; }

void chakra::cluster::Cluster::setCronTODO(uint64_t todo) { cronTodo |= todo; }
