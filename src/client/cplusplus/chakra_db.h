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

    error::Error get(const proto::client::GetMessageRequest& request, proto::client::GetMessageResponse& response, bool hash = false);
    error::Error mget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN = 20, bool hash = false);

    error::Error set(const proto::client::SetMessageRequest& request, proto::client::SetMessageResponse& response);
    error::Error mset(const proto::client::MSetMessageRequest& request, proto::client::MSetMessageResponse& response);

    error::Error push(const proto::client::PushMessageRequest& request, proto::client::PushMessageResponse& response);
    error::Error mpush(const proto::client::MPushMessageRequest& request, proto::client::MPushMessageResponse& response);

    error::Error scan(const proto::client::ScanMessageRequest& request, proto::client::ScanMessageResponse& response);

    error::Error incr(const proto::client::IncrMessageRequest& request, proto::client::IncrMessageResponse& response);
    error::Error mincr(const proto::client::MIncrMessageRequest& request, proto::client::MIncrMessageResponse& response);
private:
    error::Error mhashget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN = 20);
    error::Error mrrget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response, int splitN = 20);

    std::atomic_int64_t count;
    std::vector<std::shared_ptr<client::Chakra>> partitions;
};

}




#endif // CLIENT_CHAKRA_DB_H
