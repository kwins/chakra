//
// Created by kwins on 2021/6/4.
//

#ifndef CHAKRA_CHAKRA_H
#define CHAKRA_CHAKRA_H
#include "net/connect.h"
#include "peer.pb.h"
#include "types.pb.h"

namespace chakra::client{
class Chakra {
public:
    explicit Chakra(net::Connect::Options options);
    proto::peer::MeetMessageReply meet(const std::string& ip, int port);

private:
    void executeCmd(::google::protobuf::Message& msg, proto::types::Type type, ::google::protobuf::Message& reply);
    std::shared_ptr<net::Connect> conn;
};

}




#endif //CHAKRA_CHAKRA_H
