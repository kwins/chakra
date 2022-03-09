//
// Created by kwins on 2021/9/22.
//

#ifndef CHAKRA_NET_LINK_H
#define CHAKRA_NET_LINK_H
#include <ev++.h>
#include "net/connect.h"
#include <google/protobuf/message.h>
#include "types.pb.h"
#include "error/err.h"

namespace chakra::net {

class Link {
public:
    explicit Link(const std::string& ip, int port, bool connect = true);
    explicit Link(int sockfd);
    error::Error sendMsg(::google::protobuf::Message& msg, proto::types::Type type);
    bool connected() const;
    void close();
    ~Link();

    std::shared_ptr<net::Connect> conn = nullptr;
    ev::io rio;
    ev::io wio;
};

}

#endif //CHAKRA_BASE_LINK_H
