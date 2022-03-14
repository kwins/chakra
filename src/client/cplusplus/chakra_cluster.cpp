#include "chakra_cluster.h"

#include <glog/logging.h>
#include <thread>

chakra::client::ChakraCluster::ChakraCluster(Options opts) : index(0), options(std::move(opts)) {
    auto err = connectCluster();
    if (err) throw err;
    std::thread([this](){
        while (true) {
            std::this_thread::sleep_for(options.backupUpdateMs);
            if (clusterChanged()) {
                auto err = connectCluster();
                if (err) LOG(ERROR) << "update cluster client when cluster changed error " << err.what();
            }
        }
    }).detach();
}

chakra::error::Error chakra::client::ChakraCluster::connectCluster() {
    int i = next();
    proto::peer::ClusterState clusterState;

    client::Chakra::Options chakraOptions;
    chakraOptions.ip = options.ip;
    chakraOptions.port = options.port;
    chakraOptions.maxConns = options.maxConns;
    chakraOptions.maxIdleConns = options.maxIdleConns;
    chakraOptions.connectTimeOut = options.connectTimeOut;
    chakraOptions.readTimeOut = options.readTimeOut;
    chakraOptions.writeTimeOut = options.writeTimeOut;
    auto c = std::make_shared<client::Chakra>(chakraOptions);
    auto err = c->connect(); if (err) return err;
    err = c->state(clusterState); if (err) return err;
    
    std::unordered_map<std::string, std::shared_ptr<client::Chakra>> clusterConn;
    DBS dbConn;

    LOG(INFO) << "state:" << clusterState.DebugString();
    for (auto peerState : clusterState.peers()) {
        if (peerState.first.empty() || peerState.second.ip().empty() || peerState.second.port() <= 0) {
            return error::Error("load bad cluster state");
        }
        client::Chakra::Options peerOptions;
        peerOptions.name = peerState.first;
        peerOptions.ip = peerState.second.ip();
        peerOptions.port = peerState.second.port();
        peerOptions.connectTimeOut = options.connectTimeOut;
        peerOptions.readTimeOut = options.readTimeOut;
        peerOptions.writeTimeOut = options.writeTimeOut;
        auto conn = std::make_shared<client::Chakra>(peerOptions);
        clusterConn[peerState.first] = conn;
        for (auto db : peerState.second.dbs()) {
            dbConn[db.second.name()].push_back(conn);
        }
        for (auto& it : dbConn) {
            std::sort(it.second.begin(), it.second.end(), [](std::shared_ptr<client::Chakra>& x, std::shared_ptr<client::Chakra>& y){
                return x->peerName() > y->peerName();
            });
        }

    }

    clusterConns[i] = clusterConn;
    dbConns[i] = dbConn;
    clusterStates[i] = clusterState;
    client = c;
    index = i;
    return err;
}

bool chakra::client::ChakraCluster::clusterChanged() {
    proto::peer::ClusterState clusterState;

    client::Chakra::Options chakraOptions;
    chakraOptions.ip = options.ip;
    chakraOptions.port = options.port;
    chakraOptions.maxConns = options.maxConns;
    chakraOptions.maxIdleConns = options.maxIdleConns;
    chakraOptions.connectTimeOut = options.connectTimeOut;
    chakraOptions.readTimeOut = options.readTimeOut;
    chakraOptions.writeTimeOut = options.writeTimeOut;
    auto err = client->state(clusterState); 
    if (err) {
        LOG(ERROR) << err.what();
        return false;
    }

    auto& curState = clusterStates[index.load()];
    return curState.current_epoch() != clusterState.current_epoch();
}

int chakra::client::ChakraCluster::next() { return index.load() == 0 ? 1 : 0; }


chakra::error::Error chakra::client::ChakraCluster::meet(const std::string& ip, int port) {
    return client->meet(ip, port);
}