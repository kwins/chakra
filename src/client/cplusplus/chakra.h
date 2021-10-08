//
// Created by kwins on 2021/6/4.
//

#ifndef CHAKRA_CHAKRA_H
#define CHAKRA_CHAKRA_H
#include "net/connect.h"
#include "peer.pb.h"
#include "types.pb.h"
#include "error/err.h"

namespace chakra::client{
class Chakra {
public:
    explicit Chakra(net::Connect::Options options);
    error::Error meet(const std::string& ip, int port);
    error::Error set(const std::string& dbname, const std::string& key, const std::string& value);
    error::Error get(const std::string& dbname, const std::string& key, std::string& value);
    error::Error setdb(const std::string& dbname, int cached);
    error::Error state(proto::peer::ClusterState& clusterState);
    error::Error setEpoch(int64_t epoch, bool increasing);
    void close();
private:
    error::Error executeCmd(::google::protobuf::Message& msg, proto::types::Type type, ::google::protobuf::Message& reply);
    std::shared_ptr<net::Connect> conn;
};

}




#endif //CHAKRA_CHAKRA_H
