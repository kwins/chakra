#include "chakra_cluster.h"

#include <client.pb.h>
#include <client/cplusplus/chakra_db.h>
#include <error/err.h>
#include <exception>
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

    std::shared_ptr<client::Chakra> c;
    try {
        c = std::make_shared<client::Chakra>(chakraOptions);
        auto err = c->state(clusterState); 
        if (err) return err;
    } catch (const error::Error& err) {
        return err;
    } catch (const std::exception& e) {
        return error::Error(e.what());
    }
    
    std::unordered_map<std::string, std::shared_ptr<client::Chakra>> clusterConn;
    DBS dbConn;

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

        std::shared_ptr<client::Chakra> conn;
        try {
            conn = std::make_shared<client::Chakra>(peerOptions);
        } catch (const error::Error& err) {
            return err;
        } catch (const std::exception& e) {
            return error::Error(e.what());
        }

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
    return error::Error();
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
    if (exitTH.load()) return;
    exitTH.store(true);
    cond.notify_one();
    if (th) th->join();
}

chakra::error::Error chakra::client::ChakraCluster::meet(const std::string& ip, int port) {
    return client->meet(ip, port);
}

chakra::error::Error chakra::client::ChakraCluster::get(const std::string& dbname, const std::string& key, proto::element::Element& element, bool hash) {
    auto& dbs = dbConns[index.load()];
    auto it = dbs.find(dbname);
    if (it == dbs.end()) {
        return dbnf(dbname);
    }
    return it->second->get(dbname, key, element, hash);
}

chakra::error::Error chakra::client::ChakraCluster::mget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN, bool hash) {
    auto& dbs = dbConns[index.load()];
    /* 分包 */
    std::unordered_map<std::shared_ptr<client::ChakraDB>, proto::client::MGetMessageRequest> splited;
    for(auto& db : request.keys()) {
        auto it = dbs.find(db.first);
        if (it == dbs.end()) {
            continue;
        }

        auto& getMessage = splited[it->second];
        auto& dbkeys = (*getMessage.mutable_keys())[it->first];
        dbkeys = std::move(db.second);
    }

    if (splited.size() == 1) { /* 单个请求，不用另开一个异步 */
        return splited.begin()->first->mget(splited.begin()->second, response, splitN, hash);
    }

    /*  异步请求 */
    std::vector<std::future<proto::client::MGetMessageResponse>> futures;
    for(auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async, [&split, splitN, hash]() {
            proto::client::MGetMessageResponse futureResponse;
            auto err = split.first->mget(split.second, futureResponse, splitN, hash);
            if (err) {
                LOG(ERROR) << err.what();
            }
            return futureResponse;
        }));
    }

    /* 合并结果 */
    bool atLeastOneSuccess = false;
    for(auto& future : futures) {
        auto sub = future.get();
        for (auto& db : sub.datas()) {
            auto& elements = (*response.mutable_datas())[db.first];
            elements.MergeFrom(db.second);
        }
        if (sub.error().errcode() == 0) {
            atLeastOneSuccess = true;
        }
    }

    if (atLeastOneSuccess) {
        response.mutable_error()->set_errcode(0);
        response.mutable_error()->set_errmsg("");
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

chakra::error::Error chakra::client::ChakraCluster::mset(const proto::client::MSetMessageRequest& request, proto::client::MSetMessageResponse& response) {
    auto& dbs = dbConns[index.load()];
    /* 分包 */
    std::unordered_map<std::shared_ptr<client::ChakraDB>, proto::client::MSetMessageRequest> splited;
    for(auto& sub : request.datas()) {
        auto it = dbs.find(sub.db_name());
        if (it == dbs.end()) {
            continue;
        }
        auto& split = splited[it->second];
        auto pushMessage = split.mutable_datas()->Add();
        (*pushMessage) = std::move(sub);
    }

    if (splited.size() == 1) { /* 单个请求，不用另开一个异步 */
        return splited.begin()->first->mset(splited.begin()->second, response);
    }

    /* 异步请求 */
    std::vector<std::future<proto::client::MSetMessageResponse>> futures;
    for(auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[&split]() {
            proto::client::MSetMessageResponse futureResponse;
            auto err = split.first->mset(split.second, futureResponse);
            if (err) LOG(ERROR) << err.what();
            return futureResponse;
        }));
    }

    /* 合并结果 */
    for(auto& future : futures) {
        auto sub = future.get();
        for (auto& state : sub.states()) {
            auto& dbState = (*response.mutable_states())[state.first];
            dbState.MergeFrom(state.second);
        }
    }

    return error::Error();
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
    auto& dbs = dbConns[index.load()];
    /* 分包 */
    std::unordered_map<std::shared_ptr<client::ChakraDB>, proto::client::MPushMessageRequest> splited;
    for(auto& sub : request.datas()) {
        auto it = dbs.find(sub.db_name());
        if (it == dbs.end()) {
            continue;
        }

        auto& split = splited[it->second];
        auto pushMessage = split.mutable_datas()->Add();
        (*pushMessage) = std::move(sub);
    }

    if (splited.size() == 1) { /* 单个请求，不用另开一个异步 */
        return splited.begin()->first->mpush(splited.begin()->second, response);
    }

    /* 异步请求 */
    std::vector<std::future<proto::client::MPushMessageResponse>> futures;
    for(auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[&split](){
            proto::client::MPushMessageResponse futureResponse;
            auto err = split.first->mpush(split.second, futureResponse);
            if (err) LOG(ERROR) << err.what();
            return futureResponse;
        }));
    }

    /* 合并结果 */
    for(auto& future : futures) {
        auto sub = future.get();
        for (auto& state : sub.states()) {
            auto& dbState = (*response.mutable_states())[state.first];
            dbState.MergeFrom(state.second);
        }
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