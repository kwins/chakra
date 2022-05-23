//
// Created by kwins on 2021/6/4.
//

#ifndef CLIENT_CHAKRA_DB_H
#define CLIENT_CHAKRA_DB_H

#include "chakra.h"
#include <atomic>
#include "error/err.h"

namespace chakra::client {

class ChakraDB {
public:
    ChakraDB();
    void push(std::shared_ptr<client::Chakra> chakra);
    void sortPartitions();

    error::Error get(const std::string& dbname, const std::string& key, proto::element::Element& element);
    error::Error mget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response);

    error::Error set(const std::string& dbname, const std::string& key, const std::string& value, int64_t ttl = 0);
    error::Error set(const std::string& dbname, const std::string& key, float value, int64_t ttl = 0);
    
    error::Error push(const proto::client::PushMessageRequest& request, proto::client::PushMessageResponse& response);
    error::Error mpush(const proto::client::MPushMessageRequest& request, proto::client::MPushMessageResponse& response);
private:
    std::atomic_int64_t count;
    std::vector<std::shared_ptr<client::Chakra>> partitions;
};

}




#endif // CLIENT_CHAKRA_DB_H
