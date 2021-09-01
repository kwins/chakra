//
// Created by kwins on 2021/6/4.
//

#include "chakra.h"
#include "net/packet.h"

chakra::client::Chakra::Chakra(chakra::net::Connect::Options options) {
    conn = std::make_shared<net::Connect>(options);
    auto err = conn->connect();
    if (err){
        throw std::logic_error(err.toString());
    }
}

proto::peer::MeetMessageReply chakra::client::Chakra::meet(const std::string &ip, int port) {
    proto::peer::MeetMessage meet;
    meet.set_port(port);
    meet.set_ip(ip);

    proto::peer::MeetMessageReply reply;
    executeCmd(meet, proto::types::P_MEET, reply);
    return reply;
}


void chakra::client::Chakra::executeCmd(google::protobuf::Message &msg, proto::types::Type type, google::protobuf::Message &reply) {
    chakra::net::Packet::serialize(msg, type,[this, &reply](char* req, size_t reqlen){
        this->conn->send(req, reqlen);
        this->conn->receivePack([this, &reply](char *resp, size_t resplen) {
            chakra::net::Packet::deSerialize(resp, resplen, reply);
        });
    });
}
