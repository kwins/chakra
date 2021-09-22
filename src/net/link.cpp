//
// Created by kwins on 2021/9/22.
//

#include "link.h"
#include <net/packet.h>
#include <glog/logging.h>

chakra::net::Link::Link(int sockfd) {
    conn = std::make_shared<net::Connect>(net::Connect::Options{ .fd = sockfd });
}

chakra::net::Link::Link(const std::string &ip, int port) {
    conn = std::make_shared<net::Connect>(net::Connect::Options{ .host = ip, .port = port });
    auto err = conn->connect();
    if (!err.success()){
        throw std::logic_error(err.toString());
    }
}

chakra::utils::Error chakra::net::Link::sendMsg(google::protobuf::Message &msg, proto::types::Type type) {
    return chakra::net::Packet::serialize(msg, type, [this](char* data, size_t len){
        auto err = conn->send(data, len);
        if (!err.success()){
            LOG(ERROR) << "Send message to " << "[" << conn->remoteAddr() << "] error " << err.toString();
        }
        return err;
    });
}

chakra::utils::Error chakra::net::Link::sendSyncMsg(google::protobuf::Message &request, proto::types::Type type,
                                                    google::protobuf::Message &response) {
    return chakra::net::Packet::serialize(request, type, [this, &response, type](char* req, size_t reqLen){
        auto err = conn->send(req, reqLen);
        if (err.success()){
            return conn->receivePack([&response, type](char *resp, size_t respLen) {
                return chakra::net::Packet::deSerialize(resp, respLen, response, type);
            });
        }
        LOG(ERROR) << "REPL send message to [" << conn->remoteAddr() << "] error " << strerror(errno);
        return err;
    });
}

bool chakra::net::Link::connected() {
    return conn != nullptr && conn->connState() == chakra::net::Connect::State::CONNECTED;
}

void chakra::net::Link::close() {
    rio.stop();
    if (conn)
        conn->close();
}

chakra::net::Link::~Link() {
    LOG(INFO) << "chakra::net::Link::~Link";
    if (conn) conn = nullptr;
}
