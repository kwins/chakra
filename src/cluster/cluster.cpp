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
DECLARE_int32(cluster_handshake_timeout_ms);
DECLARE_int32(cluster_peer_timeout_ms);
DECLARE_int32(cluster_peer_link_retry_timeout_ms);
DECLARE_double(cluster_cron_interval_sec);
DECLARE_int32(server_tcp_backlog);

chakra::cluster::Cluster::Cluster() {
    LOG(INFO) << "[cluster] init";
    state = STATE_FAIL;
    cronLoops = 0;
    seed = std::make_shared<std::default_random_engine>(time(nullptr));
    loadPeers();
    startEv();
    updateClusterState();
    dumpMyselfDBs();
    LOG(INFO) << "[cluster] init end";
    LOG(INFO) << "[cluster] listen on " << utils::Basic::cport();
}

void chakra::cluster::Cluster::loadPeers() {
    try {
        proto::peer::ClusterState clusterState;
        utils::FileHelper::loadFile(FLAGS_cluster_dir + "/" + PEERS_FILE, clusterState);
        DLOG(INFO) << "[cluster] state :" << clusterState.DebugString();
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
        myself = std::make_shared<Peer>(); // ??????????????????
        myself->setIp(utils::Basic::getLocalIP());
        myself->setPort(utils::Basic::cport());
        myself->setName(utils::Basic::genRandomID());
        myself->setFlag(Peer::FLAG_MYSELF);
        peers.insert(std::make_pair(myself->getName(), myself));
        DLOG(INFO) << "[cluster] init first and peers size:" << peers.size();
    } catch (const std::exception& err) {
        LOG(ERROR) << "[cluster] exit with error " << err.what();
        exit(-1);
    }
}

// ???????????? ??? ??????????????? initView
std::shared_ptr<chakra::cluster::Cluster> chakra::cluster::Cluster::get() {
    static std::shared_ptr<Cluster> viewptr = std::make_shared<Cluster>();
    return viewptr;
}

void chakra::cluster::Cluster::startEv() {
    // ev listen and accept
    auto err = net::Network::tpcListen(utils::Basic::cport() ,FLAGS_server_tcp_backlog, sfd);
    if (err || sfd == -1) {
        LOG(ERROR) << "[cluster] listening on " << utils::Basic::cport() << " " << err.what();
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
    // ????????????????????????????????????????????????????????????
    for(auto it = peers.begin(); it != peers.end();){
        auto peer = it->second;
        if (peer->isMyself()) {
            it++;
            continue;
        }
        // ?????? handshake ???????????????????????????
        auto spends = nowMillSec - peer->createTimeMs();
        if (it->second->isHandShake() && spends > FLAGS_cluster_handshake_timeout_ms){
            peers.erase(it++);
            LOG(WARNING) << "[cluster] handshake peer " << peer->getIp() + ":" << peer->getPort()
                         << " timeout, delete peer from cluster.";
            continue;
        }
        // ???????????????????????????????????????
        if (!peer->connected() && peer->connect()) {
            // ?????????????????? Meet
            long oldPingSent = peer->getLastPingSend();
            sendPingOrMeet(peer, proto::types::P_MEET_PEER);
            if (oldPingSent){
                peer->setLastPingSend(oldPingSent);
            }
            peer->delFlag(Peer::FLAG_MEET); // whether or not meet message
            LOG(INFO) << "[cluster] connecting peer " << peer->getName()
                      << " at " << peer->getIp() << ":" << peer->getPort()
                      << " success and send "
                      << proto::types::Type_Name(proto::types::P_MEET_PEER)
                      << " message to it."
                      ;
        }
        it++;
    }
    // ??????????????????????????? gossip ??????
    if (!(iteraion % 10)) {
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
    }

    // ?????????????????????????????????????????????????????????????????????
    for(auto & it : peers){
        auto peer = it.second;
        if (peer->isMyself() || peer->isHandShake())
            continue;

        // ???????????? PONG ???????????????????????? node timeout ???????????????
        // ??????????????????????????????????????????????????????????????????
        // ??????????????????????????????????????????????????????????????????
        if (peer->connected() /* ???????????? */
                && nowMillSec - peer->createTimeMs() > FLAGS_cluster_peer_timeout_ms /* ???????????? */
                && peer->getLastPingSend() /* ????????? PING */
                && peer->getLastPongRecv() < peer->getLastPingSend() /* ?????? PONG ??? */
                && nowMillSec - peer->getLastPingSend() > FLAGS_cluster_peer_timeout_ms/2  /* ??????Pong??????????????????????????? */
                && !peer->isPfail()
                && !peer->isFail()  /* ??????????????? FAIL or PFAIL ?????? */
                ) {
            
            if (nowMillSec - peer->getLastPingSend() > FLAGS_cluster_peer_timeout_ms) { // ??????????????????
                peer->linkFree(); 
                LOG(INFO) << "[cluster] NOTE peer " << peer->getName()
                          << " connect ok, but always not received pong since " << peer->getLastPingSend()
                          << ", try free link and reconnect";
            } else {
                LOG(INFO) << "[cluster] NOTE peer " << peer->getName()
                          << " connect ok, but always not received pong since " << peer->getLastPingSend();
            }
        }

        // ????????????????????? PING ??????
        // ??????????????? node timeout ?????????????????????????????????????????? PONG ??????
        // ??????????????????????????? PING ????????????????????????????????????
        // ?????????????????????????????????????????????????????????
        if (peer->connected()
                && peer->getLastPingSend() == 0 /* ?????????PING */
                && (nowMillSec - peer->getLastPongRecv()) > FLAGS_cluster_peer_timeout_ms/2) {
            sendPingOrMeet(peer, proto::types::P_PING);
            DLOG(INFO) << "[cluster] peer " << peer->getName()
                         << " connect ok, but not send PING for a long time " << peer->getLastPongRecv()
                         << ", try to send ping to weak up it.(" << (nowMillSec - peer->getLastPongRecv()) << "ms)";
            continue;
        }
        // PING ?????? PONG ??????
        // ??????????????????
        long delay = nowMillSec - peer->getLastPingSend();
        long retryDelay = nowMillSec - peer->getRetryLinkTime();
        if ((peer->getLastPingSend() && delay > FLAGS_cluster_peer_timeout_ms)
                || (peer->getRetryLinkTime() && retryDelay > FLAGS_cluster_peer_link_retry_timeout_ms)) {
            if (!peer->isFail() && !peer->isPfail()) {
                LOG(WARNING) << "[cluster] NOTE peer " << peer->getName() << " possibly failing when delay " << delay
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
    auto link = new chakra::cluster::Peer::Link(sd);
    link->startEvRead();
    DLOG(INFO) << "[cluster] accept " << link->remoteAddr() << " ok.";
}

void chakra::cluster::Cluster::stop() {
    if (sfd != -1) ::close(sfd);
    dumpPeers();
    dumpMyselfDBs();
    acceptIO.stop();
    cronIO.stop();
    if (!peers.empty())
        peers.clear();
    if (myself)
        myself = nullptr;
    LOG(INFO) << "[cluster] stop";
}

void chakra::cluster::Cluster::addPeer(const std::string &ip, int port) {
    std::shared_ptr<Peer> peer = std::make_shared<Peer>();
    peer->setName(utils::Basic::genRandomID()); // ??????????????????????????????????????????????????????????????????
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
    // ?????????ping???????????????
    if (type == proto::types::P_PING){
        auto millSec = utils::Basic::getNowMillSec();
        peer->setLastPingSend(millSec);
    }

    // ????????????
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
        if (it.second->getPeerDBs().empty()) {
            emptyPeer++;
        } else{
            size++;
        }
        if (it.second->isFail() || it.second->isPfail()) {
            unReachablePeer++;
        }

        for(auto& db : it.second->getPeerDBs()) {
            dbPeers[db.first].push_back(it.second);
        }
    }

    /* cluster is fail state if all nodes of a DB are down */
    for(auto&it : dbPeers) {
        int fails = 0;
        for(auto& peer : it.second) {
            if (peer->isFail() || peer->isPfail()) {
                fails++;
            }
        }
        if (fails == it.second.size()) {
            newState = STATE_FAIL;
        }
    }

    /* cluster is fail state if more than half of nodes in cluster are down */
    if (unReachablePeer >= (size/2) +1) {
        newState = STATE_FAIL;
    }

    if (peers.size() < 3) {
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

// ??????????????????????????? gossip ????????????, ???sender
void chakra::cluster::Cluster::processGossip(const proto::peer::GossipMessage &gsp) {
    auto sender = getPeer(gsp.sender().name());

    for (int i = 0; i < gsp.peers_size(); ++i) {
        auto peer = getPeer(gsp.peers(i).peer_name());
        if (peer) { // ?????????
            // ?????????????????? FAIL ?????? PFAIL ??????
            // ????????? node ????????? FAIL
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

            // ???????????????????????? PFAIL ?????? FAIL ??????
            // ?????????????????? IP ?????????????????????????????????
            // ???????????????????????????????????????????????????????????????
            if ((Peer::isPfail(gsp.peers(i).flag()) || Peer::isFail(gsp.peers(i).flag()))
                    && (peer->getIp() != gsp.peers(i).ip() || peer->getPort() != gsp.peers(i).port())) {
                // start handshake
                addPeer(gsp.peers(i).ip(), gsp.peers(i).port());
                setCronTODO(FLAG_SAVE_CONFIG | FLAG_UPDATE_STATE);
            }

        } else {
            // ????????????????????????????????? sender ?????????????????????
            // ??????????????????????????????????????????????????????
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
    size_t needQuorum = peers.size()/2 + 1;
    size_t failCount = peer->cleanFailReport(FLAGS_cluster_peer_timeout_ms);

    // ????????????????????????
    if (failCount + 1 < needQuorum) return;

    LOG(WARNING) << "[cluster] marking peer " << peer->getName() << " as failing(quorum " << needQuorum << " reached).";
    peer->delFlag(Peer::FLAG_PFAIL);
    peer->setFlag(Peer::FLAG_FAIL);
    peer->setFailTime(utils::Basic::getNowMillSec());

    // ????????????FAIL?????????????????????????????????
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

    // ??????name????????????
    auto peer = it->second;
    if (random != real){
        LOG(INFO) << "[cluster] rename peer name from " << peer->getName() << " to " << real;
        peer->setName(real);
        peers.insert(std::make_pair(real, peer));
        // ????????????name
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
