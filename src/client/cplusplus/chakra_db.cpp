#include "chakra_db.h"
#include <client.pb.h>
#include <cstddef>
#include <cstdint>
#include <error/err.h>
#include <glog/logging.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <zlib.h>
#include "net/packet.h"
#include <future>

chakra::client::ChakraDB::ChakraDB() : count(0) {}
void chakra::client::ChakraDB::push(std::shared_ptr<client::Chakra> chakra) {
    partitions.push_back(chakra);
}

void chakra::client::ChakraDB::sortPartitions() {
    std::sort(partitions.begin(), partitions.end(), [](std::shared_ptr<client::Chakra>& x, std::shared_ptr<client::Chakra>& y){
        return x->peerName() > y->peerName();
    });
}

chakra::error::Error
chakra::client::ChakraDB::get(const std::string &dbname, const std::string &key, proto::element::Element& element, bool hash) {
    size_t hashed = hash ? ::crc32(0L, (unsigned char*)key.data(), key.size()) : (++count);
    return partitions.at(hashed % partitions.size())->get(dbname, key, element);
}

chakra::error::Error chakra::client::ChakraDB::mget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN, bool hash) {
    return hash ? mhashget(request, response) : mrrget(request, response);
}

chakra::error::Error chakra::client::ChakraDB::mhashget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN) {
    std::unordered_map<int, proto::client::MGetMessageRequest> splited;
    std::vector<std::pair<std::string, std::string>> dbkps;
    for(auto& dbkeys : request.keys()) {
        for (auto& key : dbkeys.second.value()) {
            size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
            auto& split = splited[hashed % partitions.size()];
            auto& subReq = (*split.mutable_keys())[dbkeys.first];
            subReq.add_value(key);
        }
    }
    
    if (splited.size() == 1) {
        return partitions.at(splited.begin()->first)->mget(splited.begin()->second, response);
    }

    std::vector<std::future<proto::client::MGetMessageResponse>> futures;
    for (auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[this, &split](){
            proto::client::MGetMessageResponse futureResponse;
            auto err = partitions.at(split.first)->mget(split.second, futureResponse);
            if (err) LOG(ERROR) << "mget async request error:" << err.what();
            return futureResponse;
        }));
    }

    for(auto& future : futures) {
        auto sub = future.get();
        for (auto& db : sub.datas()) {
            auto& elements = (*response.mutable_datas())[db.first];
            elements.MergeFrom(db.second);
        }
    }
    return error::Error();
}

chakra::error::Error chakra::client::ChakraDB::mrrget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN) {
    std::vector<std::pair<std::string, std::string>> dbkps;
    for(auto& dbkeys : request.keys()) {
        for (auto& key : dbkeys.second.value()) {
            dbkps.emplace_back(std::make_pair(dbkeys.first, key));
        }
    }
    
    if (dbkps.size() <= splitN) { /*  不需要分包 */
        return partitions.at((++count) % partitions.size())->mget(request, response);
    }

    /* 需要分包 */
    int g = dbkps.size() / splitN;
    if (dbkps.size() % splitN > 0) {
        g++;
    }

    std::vector<proto::client::MGetMessageRequest> splited;
    int k = 0;
    for (int i = 0; i < g; i++) {
        proto::client::MGetMessageRequest sub;
        for (int j = i * splitN; j < (i + 1) * splitN && j < dbkps.size(); j++) {
            auto& dbname = dbkps[k].first;
            auto& key = dbkps[k].second;
            auto& subdbkeys = (*sub.mutable_keys())[dbname];
            subdbkeys.add_value(key);
        }
        splited.emplace_back(sub);
    }

    std::vector<std::future<proto::client::MGetMessageResponse>> futures;
    for (auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[this, &split](){
            proto::client::MGetMessageResponse futureResponse;
            auto err = partitions.at((++count) % partitions.size())->mget(split, futureResponse);
            if (err) LOG(ERROR) << err.what();
            return futureResponse;
        }));
    }

    for(auto& future : futures) {
        auto sub = future.get();
        for (auto& db : sub.datas()) {
            auto& elements = (*response.mutable_datas())[db.first];
            elements.MergeFrom(db.second);
        }
    }
    return error::Error();
}

chakra::error::Error chakra::client::ChakraDB::set(const std::string& dbname, const std::string &key, const std::string &value, int64_t ttl) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return partitions.at(hashed % partitions.size())->set(dbname, key, value, ttl);
}

chakra::error::Error chakra::client::ChakraDB::set(const std::string& dbname, const std::string& key, float value, int64_t ttl) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return partitions.at(hashed % partitions.size())->set(dbname, key, value, ttl);
}

chakra::error::Error chakra::client::ChakraDB::mset(const proto::client::MSetMessageRequest& request, proto::client::MSetMessageResponse& response) {
    std::unordered_map<int, proto::client::MSetMessageRequest> splited;
    for(auto& sub : request.datas()) {
        size_t hashed = ::crc32(0L, (unsigned char*)sub.key().data(), sub.key().size());
        auto& split = splited[hashed % partitions.size()];
        auto pushMessage = split.mutable_datas()->Add();
        (*pushMessage) = std::move(sub);
    }

    if (splited.size() == 1) { /* 单个请求，不用另开一个异步 */
        return partitions.at(splited.begin()->first)->mset(splited.begin()->second, response);
    }

    std::vector<std::future<proto::client::MSetMessageResponse>> futures;
    for (auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[this, &split]() {
            proto::client::MSetMessageResponse futureResponse;
            auto err = partitions.at(split.first)->mset(split.second, futureResponse);
            if (err) LOG(ERROR) << err.what();
            return futureResponse;
        }));
    }

    for(auto& future : futures) {
        auto sub = future.get();
        for (auto& state : sub.states()) {
            auto& dbState = (*response.mutable_states())[state.first];
            dbState.MergeFrom(state.second);
        }
    }
    return error::Error();
}

chakra::error::Error chakra::client::ChakraDB::push(const proto::client::PushMessageRequest& request, proto::client::PushMessageResponse& response) {
    size_t hashed = ::crc32(0L, (unsigned char*)request.key().data(), request.key().size());
    return partitions.at(hashed % partitions.size())->push(request, response);
}

chakra::error::Error chakra::client::ChakraDB::mpush(const proto::client::MPushMessageRequest& request, proto::client::MPushMessageResponse& response) {
    std::unordered_map<int, proto::client::MPushMessageRequest> splited;
    for(auto& sub : request.datas()) {
        size_t hashed = ::crc32(0L, (unsigned char*)sub.key().data(), sub.key().size());
        auto& split = splited[hashed % partitions.size()];
        auto pushMessage = split.mutable_datas()->Add();
        (*pushMessage) = std::move(sub);
    }
    
    if (splited.size() == 1) { /* 单个请求，不用另开一个异步 */
        return partitions.at(splited.begin()->first)->mpush(splited.begin()->second, response);
    }

    std::vector<std::future<proto::client::MPushMessageResponse>> futures;
    for (auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[this, &split](){
            proto::client::MPushMessageResponse futureResponse;
            auto err = partitions.at(split.first)->mpush(split.second, futureResponse);
            if (err) LOG(ERROR) << err.what();
            return futureResponse;
        }));
    }

    for(auto& future : futures) {
        auto sub = future.get();
        for (auto& state : sub.states()) {
            auto& dbState = (*response.mutable_states())[state.first];
            dbState.MergeFrom(state.second);
        }
    }
    return error::Error();
}

