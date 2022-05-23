#include "chakra_cluster.h"

#include <client.pb.h>
#include <client/cplusplus/chakra_db.h>
#include <error/err.h>
#include <glog/logging.h>
#include <chrono>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <zlib.h>
#include <future>

chakra::client::ChakraCluster::ChakraCluster(Options opts) : options(std::move(opts))
                                                        , index(0), exitTH(false) {
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

    // LOG(INFO) << "state:" << clusterState.DebugString();
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
            auto cdb = dbConn.find(db.second.name());
            if (cdb == dbConn.end()) {
                dbConn[db.second.name()] = std::make_shared<ChakraDB>();
            }
            dbConn[db.second.name()]->push(conn);
        }
    }

    for(auto& db : dbConn) {
        db.second->sortPartitions();
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
        return dbnf(dbname);
    }
    return it->second->get(dbname, key, element);
}

chakra::error::Error chakra::client::ChakraCluster::mget(const proto::client::MGetMessageRequest& request, int splitN, proto::client::MGetMessageResponse& response) {
    auto splited = splitMGetRequest(request, splitN);
    std::vector<std::future<proto::client::MGetMessageResponse>> futures;
    for(int i = 0; i < splited.size(); i++) {
        futures.emplace_back(std::async(std::launch::async,[&splited, i](){
            proto::client::MGetMessageResponse futureResponse;
            auto err = splited[i].first->mget(splited[i].second, futureResponse);
            if (err) {
                futureResponse.mutable_error()->set_errcode(1);
                futureResponse.mutable_error()->set_errmsg(err.what());
            }
            return futureResponse;
        }));
    }

    bool atLeastOneSuccess = false;
    for(auto& future : futures) {
        future.wait();
        proto::client::MGetMessageResponse subRes = future.get();
        response.MergeFrom(subRes);
        if (subRes.error().errcode() == 0) {
            atLeastOneSuccess = true;
        }
    }

    if (atLeastOneSuccess) {
        response.mutable_error()->set_errcode(0);
    }
    return error::Error();
}

chakra::error::Error chakra::client::ChakraCluster::set(const std::string& name, const std::string& key, const std::string& value, int64_t ttl) {
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(name);
    if (it == dbs.end()) {
        return dbnf(name);
    }
    return it->second->set(name, key, value);
}

chakra::error::Error chakra::client::ChakraCluster::set(const std::string& name, const std::string& key, float value, int64_t ttl) {
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(name);
    if (it == dbs.end()) {
        return dbnf(name);
    }
    return it->second->set(name, key, value);
}

chakra::error::Error chakra::client::ChakraCluster::push(const proto::client::PushMessageRequest& request, proto::client::PushMessageResponse& response){
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(request.db_name());
    if (it == dbs.end()) {
        return dbnf(request.db_name());
    }
    return it->second->push(request, response);
}

chakra::error::Error chakra::client::ChakraCluster::mpush(const proto::client::MPushMessageRequest& request, int splitN, proto::client::MPushMessageResponse& response) {
    auto splited = splitMPushRequest(request, splitN);
    std::vector<std::future<proto::client::MPushMessageResponse>> futures;
    for(auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[&split](){
            proto::client::MPushMessageResponse futureResponse;
            auto err = split.first->mpush(split.second, futureResponse);
            if (err) LOG(ERROR) << err.what();
            return futureResponse;
        }));
    }
    for(int i = 0; i < splited.size(); i++) {

    }

    for(auto& future : futures) {
        future.wait();
        proto::client::MPushMessageResponse subRes = future.get();
        response.MergeFrom(subRes);
    }
    return error::Error();
}

chakra::error::Error chakra::client::ChakraCluster::setdb(const std::string& nodename, const std::string& dbname, int cached) {
    auto nodes = clusterConns[index.load()];
    auto it = nodes.find(nodename);
    if (it == nodes.end()) {
        return error::Error("node " + nodename + " not found");
    }
    return it->second->setdb(dbname,  cached);
}

chakra::error::Error chakra::client::ChakraCluster::dbnf(const std::string& dbname) {
    return error::Error("db:" + dbname + " not found");
}

chakra::client::ChakraCluster::MGetSplitedRequest chakra::client::ChakraCluster::splitMGetRequest(const proto::client::MGetMessageRequest& request, int splitN) {
    auto& dbs = dbConns[index.load()];
    std::unordered_map<std::shared_ptr<client::ChakraDB>, DBKeyPairList> splited;
    for(auto& db : request.keys()) {
        auto it = dbs.find(db.first);
        if (it == dbs.end()) {
            continue;
        }
        auto& pairList = splited[it->second];
        for (auto& key : db.second.value()) {
            pairList.emplace_back(std::make_pair(db.first, key));
        }
    }

    MGetSplitedRequest splitedN;
    for (auto& split : splited) {
        int count = split.second.size();
        auto sn = count / splitN;

        if (count % splitN > 0)
            sn++;
        for(int i = 0; i < sn; i++) {
            proto::client::MGetMessageRequest subReq;
            for(int j = i * splitN; j < (i + 1) * splitN && j < count; j++) {
                auto& dbks = (*subReq.mutable_keys())[split.second[j].first];
                dbks.add_value(split.second[j].second);
            }
            splitedN.emplace_back(std::make_pair(split.first, subReq));
        }
    }
    return splitedN;
}

chakra::client::ChakraCluster::MPushSplitedRequest chakra::client::ChakraCluster::splitMPushRequest(const proto::client::MPushMessageRequest& request, int splitN) {
    auto& dbs = dbConns[index.load()];
    chakra::client::ChakraCluster::MPushSplitedRequest splited;
    for(auto& sub : request.datas()) {
        auto it = dbs.find(sub.db_name());
        if (it == dbs.end()) {
            continue;
        }
        auto& split = splited[it->second];
        auto pushMessage = split.mutable_datas()->Add();
        (*pushMessage) = std::move(sub);
    }
    return splited;
}