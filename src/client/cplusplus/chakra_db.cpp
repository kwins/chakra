#include "chakra_db.h"
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
chakra::client::ChakraDB::get(const std::string &dbname, const std::string &key, proto::element::Element& element) {
    count++;
    return partitions.at(count % partitions.size())->get(dbname, key, element);
}

chakra::error::Error chakra::client::ChakraDB::mget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response) {
    count++;
    return partitions.at(count % partitions.size())->mget(request, response);
}

chakra::error::Error chakra::client::ChakraDB::set(const std::string& dbname, const std::string &key, const std::string &value, int64_t ttl) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return partitions.at(hashed % partitions.size())->set(dbname, key, value, ttl);
}

chakra::error::Error chakra::client::ChakraDB::set(const std::string& dbname, const std::string& key, float value, int64_t ttl) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return partitions.at(hashed % partitions.size())->set(dbname, key, value, ttl);
}

chakra::error::Error chakra::client::ChakraDB::push(const proto::client::PushMessageRequest& request, proto::client::PushMessageResponse& response) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)request.key().data(), request.key().size());
    return partitions.at(hashed % partitions.size())->push(request, response);
}

chakra::error::Error chakra::client::ChakraDB::mpush(const proto::client::MPushMessageRequest& request, proto::client::MPushMessageResponse& response) {
    std::unordered_map<int, proto::client::MPushMessageRequest> splited;
    for(auto& sub : request.datas()) {
        unsigned long hashed = ::crc32(0L, (unsigned char*)sub.key().data(), sub.key().size());
        auto& split = splited[hashed % partitions.size()];
        auto pushMessage = split.mutable_datas()->Add();
        (*pushMessage) = std::move(sub);
    }

    std::vector<std::future<proto::client::MPushMessageResponse>> futures;
    for (auto& split : splited) {
        futures.emplace_back(std::async(std::launch::async,[this, &split](){
            proto::client::MPushMessageResponse splitResponse;
            auto err = partitions.at(split.first)->mpush(split.second, splitResponse);
            if (err) LOG(ERROR) << err.what();
            return splitResponse;
        }));
    }

    for(auto& future : futures) {
        future.wait();
        response.MergeFrom(future.get());
    }
    return error::Error();
}

