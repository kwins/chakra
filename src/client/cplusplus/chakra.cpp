//
// Created by kwins on 2021/6/4.
//

#include "chakra.h"
#include "net/packet.h"
#include "client.pb.h"
#include <algorithm>
#include <element.pb.h>
#include <glog/logging.h>
#include <google/protobuf/map.h>
#include <memory>
#include <types.pb.h>
#include "database/db_object.h"

chakra::client::Chakra::Chakra(Options opts) : options(opts) {
    chakra::net::Connect::Options connOptions;
    connOptions.host = options.ip;
    connOptions.port = options.port;
    connOptions.connectTimeOut = options.connectTimeOut;
    connOptions.readTimeOut = options.readTimeOut;
    connOptions.writeTimeOut = options.writeTimeOut;
    for (int i = 0; i < options.maxIdleConns; i++) {
        auto conn = std::make_shared<net::Connect>(connOptions);
        conns.push_back(conn);
    }
}

chakra::error::Error chakra::client::Chakra::connect() {
    error::Error err;
    for (auto& conn : conns) {
        err = conn->connect();
        if (err) return err;
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::meet(const std::string &ip, int port) {
    proto::peer::MeetMessageRequest meet;
    meet.set_port(port);
    meet.set_ip(ip);

    proto::peer::MeetMessageResponse meetMessageResponse;
    auto err = executeCmd(meet, proto::types::P_MEET, meetMessageResponse);
    if (err) return err;
    if (meetMessageResponse.error().errcode() != 0) {
        return error::Error(meetMessageResponse.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::set(const std::string& dbname, const std::string &key, const std::string &value, int64_t ttl) {
    proto::client::SetMessageRequest setMessageRequest;
    setMessageRequest.set_db_name(dbname);
    setMessageRequest.set_key(key);
    setMessageRequest.set_type(::proto::element::ElementType::STRING);
    setMessageRequest.set_ttl(ttl);
    setMessageRequest.set_s(value);

    proto::client::SetMessageResponse setMessageResponse;
    auto err = executeCmd(setMessageRequest, proto::types::C_SET, setMessageResponse);
    if (err) return err;

    if (setMessageResponse.error().errcode() != 0) {
        return error::Error(setMessageResponse.error().errmsg());
    }
    return err;
}
    
chakra::error::Error chakra::client::Chakra::set(const std::string& dbname, const std::string& key, float value, int64_t ttl) {
    proto::client::SetMessageRequest setMessageRequest;
    setMessageRequest.set_db_name(dbname);
    setMessageRequest.set_key(key);
    setMessageRequest.set_type(::proto::element::ElementType::FLOAT);
    setMessageRequest.set_ttl(ttl);
    setMessageRequest.set_f(value);

    proto::client::SetMessageResponse setMessageResponse;
    auto err = executeCmd(setMessageRequest, proto::types::C_SET, setMessageResponse);
    if (err) return err;

    if (setMessageResponse.error().errcode() != 0) {
        return error::Error(setMessageResponse.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::push(const proto::client::PushMessageRequest& request, proto::client::PushMessageResponse& response) {
    auto err = executeCmd(request, proto::types::C_PUSH, response);
    if (err) return err;
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::mpush(const proto::client::MPushMessageRequest& request, proto::client::MPushMessageResponse& response) {
    return executeCmd(request, proto::types::C_MPUSH, response);
}

chakra::error::Error
chakra::client::Chakra::get(const std::string &dbname, const std::string &key, proto::element::Element& element) {
    proto::client::GetMessageRequest getMessageRequest;
    getMessageRequest.set_db_name(dbname);
    getMessageRequest.set_key(key);

    proto::client::GetMessageResponse getMessageResponse;

    auto err = executeCmd(getMessageRequest, proto::types::C_GET, getMessageResponse);
    if (err) return err;
    if (getMessageResponse.error().errcode() != 0) {
        return error::Error(getMessageResponse.error().errmsg());
    }
    element = std::move(getMessageResponse.data());
    return err;
}

chakra::error::Error chakra::client::Chakra::mget(const proto::client::MGetMessageRequest& request, proto::client::MGetMessageResponse& response) {
    auto err = executeCmd(request, proto::types::C_MGET, response);
    if (err) {
        return err;
    }
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
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
    if (err) {
        return err;
    }
    if (dbSetMessageResponse.error().errcode() != 0) {
        return error::Error(dbSetMessageResponse.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::executeCmd(const google::protobuf::Message &msg, proto::types::Type type, google::protobuf::Message &reply) {
    return chakra::net::Packet::serialize(msg, type,[this, &reply, type](char* req, size_t reqlen) {
        try {
            auto conn = connnectGet();
            auto err = conn->send(req, reqlen);
            if (err) {
                connectBack(conn);
                return err;
            }
            err = conn->receivePack([this, &reply, type](char *resp, size_t resplen) -> error::Error {
                auto reqtype = chakra::net::Packet::getType(resp, resplen);
                if (reqtype == 0) {
                    return error::Error("command not found");
                }
                return chakra::net::Packet::deSerialize(resp, resplen, reply, type);
            });
            connectBack(conn);
            return err;
        } catch (const error::Error& err) {
            return err;
        }
    });
}

chakra::error::Error chakra::client::Chakra::state(proto::peer::ClusterState &clusterState) {
    proto::peer::StateMessageRequest stateMessageRequest;
    stateMessageRequest.set_from(" ");
    proto::peer::StateMessageResponse stateMessageResponse;
    auto err = executeCmd(stateMessageRequest, proto::types::P_STATE, stateMessageResponse);
    if (err) return err;
    if (stateMessageResponse.error().errcode() != 0) {
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
    if (epochSetMessageResponse.error().errcode() != 0) {
        return error::Error(epochSetMessageResponse.error().errmsg());
    }
    return err;
}

std::shared_ptr<chakra::net::Connect> chakra::client::Chakra::connnectGet() {
    std::lock_guard lock(mutex);
    if (conns.size() == 0) {
        throw error::Error("too many client");
    }
    auto conn = conns.front();
    conns.pop_front();
    return conn;
}

void chakra::client::Chakra::connectBack(std::shared_ptr<net::Connect> conn) {
    std::lock_guard lock(mutex);
    conns.push_back(conn);
}

std::string chakra::client::Chakra::peerName() const { return options.name; }
void chakra::client::Chakra::close() {
    std::lock_guard lock(mutex);
    for (auto& conn : conns) {
        if (conn) conn->close();
    }
}
