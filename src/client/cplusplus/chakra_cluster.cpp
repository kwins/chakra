#include "chakra_cluster.h"

#include <error/err.h>
#include <glog/logging.h>
#include <chrono>
#include <zlib.h>

chakra::client::ChakraCluster::ChakraCluster(Options opts) : options(std::move(opts))
                                                        , index(0), exitTH(false), getcount(0) {
    auto err = connectCluster();
    if (err) throw err;
    th = std::make_shared<std::thread>([this] () {
        while (!exitTH.load()) {
            std::unique_lock<std::mutex> lck(mutex);
            auto ok = cond.wait_for(lck, options.backupUpdateMs, [this]() {
                return exitTH.load();
            });
            if (ok && exitTH.load()) {
                break;
            }
            if (clusterChanged()) {
                auto err = connectCluster();
                if (err) LOG(ERROR) << "Update cluster client error when cluster changed:" << err.what();
            }
        }
        // LOG(INFO) << "backup update thread exit.";
    });
}

chakra::client::ChakraCluster::~ChakraCluster() { close(); }
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
        err = conn->connect();
        if (err) return err;

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
    auto lastClient = client;
    client = c;
    if (lastClient) lastClient->close();
    index = i;
    return err;
}

bool chakra::client::ChakraCluster::clusterChanged() {
    proto::peer::ClusterState clusterState;
    auto err = client->state(clusterState); 
    if (err) {
        LOG(ERROR) << "check state error:" << err.what();
        return true;
    }
    auto& curState = clusterStates[index.load()];
    return curState.current_epoch() != clusterState.current_epoch();
}

int chakra::client::ChakraCluster::next() { return index.load() == 0 ? 1 : 0; }

void chakra::client::ChakraCluster::close() {
    exitTH.store(true);
    cond.notify_one();
    if (th) th->join();
}

chakra::error::Error chakra::client::ChakraCluster::meet(const std::string& ip, int port) {
    return client->meet(ip, port);
}

chakra::error::Error chakra::client::ChakraCluster::get(const std::string& dbname, const std::string& key, proto::element::Element& element) {
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(dbname);
    if (it == dbs.end()) {
        return error::Error("db " + dbname + " not found");
    }

    getcount++;
    auto remote = it->second.at(getcount % it->second.size());
    return remote->get(dbname, key, element);
}

chakra::error::Error chakra::client::ChakraCluster::set(const std::string& name, const std::string& key, const std::string& value, int64_t ttl) {
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(name);
    if (it == dbs.end()) {
        return error::Error("db " + name + " not found");
    }
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return it->second.at(hashed % it->second.size())->set(name, key, value);
}

chakra::error::Error chakra::client::ChakraCluster::set(const std::string& name, const std::string& key, int64_t value, int64_t ttl) {
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(name);
    if (it == dbs.end()) {
        return error::Error("db " + name + " not found");
    }

    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return it->second.at(hashed % it->second.size())->set(name, key, value);
}

chakra::error::Error chakra::client::ChakraCluster::set(const std::string& name, const std::string& key, float value, int64_t ttl) {
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(name);
    if (it == dbs.end()) {
        return error::Error("db " + name + " not found");
    }

    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    int i = hashed % it->second.size();
    return it->second.at(i)->set(name, key, value);
}

chakra::error::Error chakra::client::ChakraCluster::setdb(const std::string& nodename, const std::string& dbname, int cached) {
    auto nodes = clusterConns[index.load()];
    auto it = nodes.find(nodename);
    if (it == nodes.end()) {
        return error::Error("node " + nodename + " not found");
    }
    return it->second->setdb(dbname,  cached);
}