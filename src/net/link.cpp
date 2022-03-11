//
// Created by kwins on 2021/9/22.
//

#include "link.h"
#include <net/packet.h>
#include <glog/logging.h>

chakra::net::Link::Link(int sockfd) {
    conn = std::make_shared<net::Connect>(net::Connect::Options{ .fd = sockfd });
}

chakra::net::Link::Link(const std::string &ip, int port, bool connect) {
    conn = std::make_shared<net::Connect>(net::Connect::Options{ .host = ip, .port = port });
    if (connect) {
        auto err = conn->connect();
        if (err) {
            throw err;
        }
    }
}

chakra::error::Error chakra::net::Link::sendMsg(google::protobuf::Message &msg, proto::types::Type type) {
    return chakra::net::Packet::serialize(msg, type, [this](char* data, size_t len) -> error::Error{
        return conn->send(data, len);
    });
}

bool chakra::net::Link::connected() const {
    return conn != nullptr && conn->connState() == chakra::net::Connect::State::CONNECTED;
}

void chakra::net::Link::close() {
    rio.stop();
    if (conn)
        conn->close();
}

chakra::net::Link::~Link() {
    if (conn) conn = nullptr;
}
