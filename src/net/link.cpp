//
// Created by kwins on 2021/9/22.
//

#include "link.h"
#include <gflags/gflags_declare.h>
#include <net/buffer.h>
#include <net/packet.h>
#include <glog/logging.h>

DECLARE_int64(connect_buff_size);

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

bool chakra::net::Link::connected() const {
    return conn != nullptr && conn->connState() == chakra::net::Connect::State::CONNECTED;
}

std::string chakra::net::Link::remoteAddr() { return conn->remoteAddr(); }
void chakra::net::Link::close() {
    rio.stop();
    if (conn)
        conn->close();
}

chakra::net::Link::~Link() {
    if (conn) conn = nullptr;
}
