//
// Created by kwins on 2021/6/4.
//

#include "chakra.h"
#include "net/packet.h"
#include "client.pb.h"

chakra::client::Chakra::Chakra(chakra::net::Connect::Options options) {
    conn = std::make_shared<net::Connect>(options);
    auto err = conn->connect();
    if (err){
        throw std::logic_error(err.toString());
    }
}

chakra::utils::Error chakra::client::Chakra::meet(const std::string &ip, int port, proto::peer::MeetMessageResponse& response) {
    proto::peer::MeetMessageRequest meet;
    meet.set_port(port);
    meet.set_ip(ip);
    return executeCmd(meet, proto::types::P_MEET, response);
}

chakra::utils::Error chakra::client::Chakra::set(const std::string& dbname, const std::string &key, const std::string &value) {
    proto::client::SetMessageRequest setMessageRequest;
    setMessageRequest.set_db_name(dbname);
    setMessageRequest.set_key(key);
    setMessageRequest.set_value(value);
    setMessageRequest.set_type(proto::types::D_STRING);

    proto::client::SetMessageResponse setMessageResponse;
    auto err = executeCmd(setMessageRequest, proto::types::C_SET, setMessageResponse);
    if (err) return err;

    if (setMessageResponse.error().errcode() != 0){
        return utils::Error(setMessageResponse.error().errcode(), setMessageResponse.error().errmsg());
    }
    return err;
}

chakra::utils::Error chakra::client::Chakra::setdb(const std::string &dbname, int cached) {
    proto::peer::DBSetMessageRequest dbSetMessageRequest;
    auto db = dbSetMessageRequest.mutable_dbs()->Add();
    db->set_name(dbname);
    db->set_cached(cached);
    proto::peer::DBSetMessageResponse dbSetMessageResponse;
    auto err = executeCmd(dbSetMessageRequest, proto::types::P_SET_DB, dbSetMessageResponse);
    if (err) return err;
    if (dbSetMessageResponse.error().errcode() != 0){
        return utils::Error(dbSetMessageResponse.error().errcode(), dbSetMessageResponse.error().errmsg());
    }
    return err;
}

chakra::utils::Error
chakra::client::Chakra::get(const std::string &dbname, const std::string &key, std::string &value) {
    proto::client::GetMessageRequest getMessageRequest;
    getMessageRequest.set_db_name(dbname);
    getMessageRequest.set_key(key);
    getMessageRequest.set_type(proto::types::D_STRING);

    proto::client::GetMessageResponse getMessageResponse;

    auto err = executeCmd(getMessageResponse, proto::types::P_SET_DB, getMessageResponse);
    if (err) return err;
    if (getMessageResponse.error().errcode() != 0){
        return utils::Error(getMessageResponse.error().errcode(), getMessageResponse.error().errmsg());
    }
    value = getMessageResponse.value();
    return err;
}

chakra::utils::Error chakra::client::Chakra::executeCmd(google::protobuf::Message &msg, proto::types::Type type, google::protobuf::Message &reply) {
    utils::Error err;
    chakra::net::Packet::serialize(msg, type,[this, &reply, &err](char* req, size_t reqlen){
        err = this->conn->send(req, reqlen);
        if (!err){
            err = this->conn->receivePack([this, &reply](char *resp, size_t resplen) {
                chakra::net::Packet::deSerialize(resp, resplen, reply);
            });
        }
    });
    return err;
}

void chakra::client::Chakra::close() {
    if (conn)
        conn->close();
}
