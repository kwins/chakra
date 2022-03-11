//
// Created by kwins on 2021/6/4.
//

#include "chakra.h"
#include "net/packet.h"
#include "client.pb.h"
#include <glog/logging.h>
#include "database/db_object.h"

chakra::client::Chakra::Chakra(chakra::net::Connect::Options options) {
    conn = std::make_shared<net::Connect>(options);
    auto err = conn->connect();
    if (err){
        throw err;
    }
}

chakra::error::Error chakra::client::Chakra::meet(const std::string &ip, int port) {
    proto::peer::MeetMessageRequest meet;
    meet.set_port(port);
    meet.set_ip(ip);

    proto::peer::MeetMessageResponse meetMessageResponse;
    auto err = executeCmd(meet, proto::types::P_MEET, meetMessageResponse);
    if (err) return err;
    if (meetMessageResponse.error().errcode() != 0){
        return error::Error(meetMessageResponse.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::set(const std::string& dbname, const std::string &key, const std::string &value) {
    proto::client::SetMessageRequest setMessageRequest;
    setMessageRequest.set_db_name(dbname);
    setMessageRequest.set_key(key);
    setMessageRequest.set_value(value);
    setMessageRequest.set_type(database::Object::Type::STRING);

    proto::client::SetMessageResponse setMessageResponse;
    auto err = executeCmd(setMessageRequest, proto::types::C_SET, setMessageResponse);
    if (err) return err;

    if (setMessageResponse.error().errcode() != 0){
        return error::Error(setMessageResponse.error().errmsg());
    }
    return err;
}

chakra::error::Error
chakra::client::Chakra::get(const std::string &dbname, const std::string &key, std::string &value) {
    proto::client::GetMessageRequest getMessageRequest;
    getMessageRequest.set_db_name(dbname);
    getMessageRequest.set_key(key);

    proto::client::GetMessageResponse getMessageResponse;

    auto err = executeCmd(getMessageRequest, proto::types::C_GET, getMessageResponse);
    if (err) return err;
    if (getMessageResponse.error().errcode() != 0){
        return error::Error(getMessageResponse.error().errmsg());
    }

    if(getMessageResponse.value_case() == proto::client::GetMessageResponse::ValueCase::kS){
        value = getMessageResponse.s();
    } else if (getMessageResponse.value_case() == proto::client::GetMessageResponse::ValueCase::kI){
        value = std::to_string(getMessageResponse.i());
    } else if (getMessageResponse.value_case() == proto::client::GetMessageResponse::ValueCase::kF){
        value = std::to_string(getMessageResponse.f());
    } else {
        return error::Error("bad data type");
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::setdb(const std::string &dbname, int cached) {
    proto::peer::DBSetMessageRequest dbSetMessageRequest;
    auto db = dbSetMessageRequest.mutable_db();
    db->set_name(dbname);
    db->set_cached(cached);
    proto::peer::DBSetMessageResponse dbSetMessageResponse;
    auto err = executeCmd(dbSetMessageRequest, proto::types::P_SET_DB, dbSetMessageResponse);
    if (err) return err;
    if (dbSetMessageResponse.error().errcode() != 0){
        return error::Error(dbSetMessageResponse.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::executeCmd(google::protobuf::Message &msg, proto::types::Type type, google::protobuf::Message &reply) {
    return chakra::net::Packet::serialize(msg, type,[this, &reply, type](char* req, size_t reqlen){
        auto err = this->conn->send(req, reqlen);
        if (!err){
            return this->conn->receivePack([this, &reply, type](char *resp, size_t resplen) -> error::Error {
                auto reqtype = chakra::net::Packet::getType(resp, resplen);
                if (reqtype == 0){
                    return error::Error("command not found");
                }
                return chakra::net::Packet::deSerialize(resp, resplen, reply, type);
            });
        }
        return err;
    });
}

chakra::error::Error chakra::client::Chakra::state(proto::peer::ClusterState &clusterState) {
    proto::peer::StateMessageRequest stateMessageRequest;
    stateMessageRequest.set_from(" ");
    proto::peer::StateMessageResponse stateMessageResponse;
    auto err = executeCmd(stateMessageRequest, proto::types::P_STATE, stateMessageResponse);
    if (err) return err;
    if (stateMessageResponse.error().errcode() != 0){
        return error::Error(stateMessageResponse.error().errmsg());
    }
    clusterState = stateMessageResponse.state();
    return err;
}

chakra::error::Error chakra::client::Chakra::setEpoch(int64_t epoch, bool increasing) {
    proto::peer::EpochSetMessageRequest epochSetMessageRequest;
    epochSetMessageRequest.set_epoch(epoch);
    epochSetMessageRequest.set_increasing(increasing);
    proto::peer::EpochSetMessageResponse epochSetMessageResponse;
    auto err = executeCmd(epochSetMessageRequest, proto::types::P_SET_EPOCH, epochSetMessageResponse);
    if (err) return err;
    if (epochSetMessageResponse.error().errcode() != 0){
        return error::Error(epochSetMessageResponse.error().errmsg());
    }
    return err;
}

void chakra::client::Chakra::close() {
    if (conn)
        conn->close();
}
