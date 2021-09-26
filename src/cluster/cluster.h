//
// Created by kwins on 2021/5/17.
//

#ifndef CHAKRA_CLUSTER_VIEW_H
#define CHAKRA_CLUSTER_VIEW_H

#include <chrono>
#include <unordered_map>
#include <ev++.h>
#include "peer.pb.h"
#include "peer.h"
#include <ctime>
#include <random>

namespace chakra::cluster{
using namespace std::chrono;

class Cluster {
public:
    static const int STATE_OK = 0;
    static const int STATE_FAIL = 1;

    static const uint64_t FLAG_SAVE_CONFIG = (1<<1);
    static const uint64_t FLAG_UPDATE_STATE = (1<<2);

public:
    Cluster();
    static std::shared_ptr<Cluster> get();

    void startPeersCron();
    void onPeersCron(ev::timer& watcher, int event);
    void onAccept(ev::io& watcher, int event);
    void stop();

    void addPeer(const std::string& ip, int port);
    std::shared_ptr<Peer> getPeer(const std::string& ip, int port);
    std::shared_ptr<Peer> getPeer(const std::string& name);
    std::vector<std::shared_ptr<Peer>> getPeers(const std::string& dbName);
    size_t size();
    void sendPingOrMeet(std::shared_ptr<Peer> peer, proto::types::Type type);
    int getCurrentEpoch() const;
    void setCurrentEpoch(int epoch);
    int getState() const;
    void setState(int state);
    uint64_t getMaxEpoch();

    bool dumpPeers();
    // 1、如果集群任意节点挂掉，且节点上的某个DB没有副本 或者其他副本也全部挂掉，则集群进入fail状态
    // 2、如果集群超过半数以上节点挂掉，集群进入fail状态
    void updateClusterState();
    void stateDesc(proto::peer::ClusterState& clusterState);
    void processGossip(const proto::peer::GossipMessage& gossip);
    // 根据报道情况，尝试将节点置为fail状态，并广播到集群
    void tryMarkFailPeer(const std::shared_ptr<Peer>& peer);
    void sendFail(const std::string& failPeerName);
    void broadcastMessage(::google::protobuf::Message& peerMsg, proto::types::Type type);
    void buildGossipMessage(proto::peer::GossipMessage& gossip, const std::string& data = "");
    std::shared_ptr<Peer> renamePeer(const std::string& random, const std::string& real);
    std::shared_ptr<Peer> getMyself();
    void setCronTODO(uint64_t todo);
    bool stateOK();
private:
    utils::Error loadConfigFile();
    void startEv();
    std::shared_ptr<Peer> randomPeer();
    void buildGossipSeader(proto::peer::GossipSender* sender, const std::string& data = "");

    ev::io acceptIO;
    int sfd = -1;
    ev::timer cronIO;
    long iteraion = 0;
    const std::string PEERS_FILE = "peers.json";
    int currentEpoch = 0;
    // 集群上线时间
    system_clock::time_point onlineTime{};
    std::shared_ptr<Peer> myself = nullptr;

    // 集群当前的状态：是在线还是下线
    int state = 0;
    // 集群中所有节点对于当前节点的认知是否一致
    bool consensus = false;
    std::shared_ptr<std::default_random_engine> seed;
    // 节点名称索引
    std::unordered_map<std::string, std::shared_ptr<Peer>> peers = {};
    uint64_t cronLoops = 0;
    uint64_t cronTodo = 0;
};


}



#endif //CHAKRA_CLUSTER_VIEW_H
