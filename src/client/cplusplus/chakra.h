//
// Created by kwins on 2021/6/4.
//

#ifndef CHAKRA_CHAKRA_H
#define CHAKRA_CHAKRA_H
#include "net/connect.h"
#include "peer.pb.h"
#include "types.pb.h"
#include "utils/error.h"

namespace chakra::client{
class Chakra {
public:
    explicit Chakra(net::Connect::Options options);
    utils::Error meet(const std::string& ip, int port);
    utils::Error set(const std::string& dbname, const std::string& key, const std::string& value);
    utils::Error get(const std::string& dbname, const std::string& key, std::string& value);
    utils::Error setdb(const std::string& dbname, int cached);
    utils::Error replicaof(const std::string& dbname);
    utils::Error state(proto::peer::ClusterState& clusterState);
    void close();
private:
    utils::Error executeCmd(::google::protobuf::Message& msg, proto::types::Type type, ::google::protobuf::Message& reply);
    std::shared_ptr<net::Connect> conn;
};

}




#endif //CHAKRA_CHAKRA_H
