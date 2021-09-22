//
// Created by kwins on 2021/6/4.
//

#include "chakra.h"
#include "net/packet.h"
#include "client.pb.h"
#include <glog/logging.h>
#include "database/db_object.h"
#include "replica.pb.h"

chakra::client::Chakra::Chakra(chakra::net::Connect::Options options) {
    conn = std::make_shared<net::Connect>(options);
    auto err = conn->connect();
    if (!err.success()){
        throw std::logic_error(err.toString());
    }
}

chakra::utils::Error chakra::client::Chakra::meet(const std::string &ip, int port) {
    proto::peer::MeetMessageRequest meet;
    meet.set_port(port);
    meet.set_ip(ip);

    proto::peer::MeetMessageResponse meetMessageResponse;
    auto err = executeCmd(meet, proto::types::P_MEET, meetMessageResponse);
    if (!err.success()) return err;
    if (meetMessageResponse.error().errcode() != 0){
        return utils::Error(meetMessageResponse.error().errcode(), meetMessageResponse.error().errmsg());
    }
    return err;
}

chakra::utils::Error chakra::client::Chakra::set(const std::string& dbname, const std::string &key, const std::string &value) {
    proto::client::SetMessageRequest setMessageRequest;
    setMessageRequest.set_db_name(dbname);
    setMessageRequest.set_key(key);
    setMessageRequest.set_value(value);
    setMessageRequest.set_type(database::Object::Type::STRING);

    proto::client::SetMessageResponse setMessageResponse;
    auto err = executeCmd(setMessageRequest, proto::types::C_SET, setMessageResponse);
    if (!err.success()) return err;

    if (setMessageResponse.error().errcode() != 0){
        return utils::Error(setMessageResponse.error().errcode(), setMessageResponse.error().errmsg());
    }
    return err;
}

chakra::utils::Error
chakra::client::Chakra::get(const std::string &dbname, const std::string &key, std::string &value) {
    proto::client::GetMessageRequest getMessageRequest;
    getMessageRequest.set_db_name(dbname);
    getMessageRequest.set_key(key);

    proto::client::GetMessageResponse getMessageResponse;

    auto err = executeCmd(getMessageRequest, proto::types::C_GET, getMessageResponse);
    if (!err.success()) return err;
    if (getMessageResponse.error().errcode() != 0){
        return utils::Error(getMessageResponse.error().errcode(), getMessageResponse.error().errmsg());
    }

    if(getMessageResponse.value_case() == proto::client::GetMessageResponse::ValueCase::kS){
        value = getMessageResponse.s();
    } else if (getMessageResponse.value_case() == proto::client::GetMessageResponse::ValueCase::kI){
        value = std::to_string(getMessageResponse.i());
    } else if (getMessageResponse.value_case() == proto::client::GetMessageResponse::ValueCase::kF){
        value = std::to_string(getMessageResponse.f());
    } else {
        return utils::Error(utils::Error::ERR_DATA_TYPE, "data type error");
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
    if (!err.success()) return err;
    if (dbSetMessageResponse.error().errcode() != 0){
        return utils::Error(dbSetMessageResponse.error().errcode(), dbSetMessageResponse.error().errmsg());
    }
    return err;
}

chakra::utils::Error chakra::client::Chakra::executeCmd(google::protobuf::Message &msg, proto::types::Type type, google::protobuf::Message &reply) {
    return chakra::net::Packet::serialize(msg, type,[this, &reply, type](char* req, size_t reqlen){
        auto err = this->conn->send(req, reqlen);
        if (err.success()){
            return this->conn->receivePack([this, &reply, type](char *resp, size_t resplen) {
                auto reqtype = chakra::net::Packet::getType(resp, resplen);
                if (reqtype == 0){
                    return utils::Error(utils::Error::ERR_COMMAND_NOT_FOUND, "command not found");
                }
                return chakra::net::Packet::deSerialize(resp, resplen, reply, type);
            });
        }
        return err;
    });
}

chakra::utils::Error chakra::client::Chakra::replicaof(const std::string &dbname) {
    proto::replica::ReplicaOfRequest replicaOfRequest;
    replicaOfRequest.set_db_name(dbname);

    proto::replica::ReplicaOfResponse replicaOfResponse;
    auto err = executeCmd(replicaOfRequest, proto::types::R_REPLICA_OF, replicaOfResponse);
    if (!err.success()) return err;
    if (replicaOfResponse.error().errcode() != 0){
        return utils::Error(replicaOfResponse.error().errcode(), replicaOfResponse.error().errmsg());
    }
    return err;
}

chakra::utils::Error chakra::client::Chakra::state(proto::peer::ClusterState &clusterState) {
    proto::peer::StateMessageRequest stateMessageRequest;
    stateMessageRequest.set_from(" ");
    proto::peer::StateMessageResponse stateMessageResponse;
    auto err = executeCmd(stateMessageRequest, proto::types::P_STATE, stateMessageResponse);
    if (!err.success()) return err;
    if (stateMessageResponse.error().errcode() != 0){
        return utils::Error(stateMessageResponse.error().errcode(), stateMessageResponse.error().errmsg());
    }
    clusterState = stateMessageResponse.state();
    return err;
}

void chakra::client::Chakra::close() {
    if (conn)
        conn->close();
}
