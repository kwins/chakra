//
// Created by kwins on 2021/6/4.
//

#include "chakra.h"
#include "net/packet.h"
#include "client.pb.h"
#include <algorithm>
#include <element.pb.h>
#include <error/err.h>
#include <glog/logging.h>
#include <google/protobuf/map.h>
#include <memory>
#include <string>
#include <types.pb.h>

chakra::client::Chakra::Chakra(Options opts) : options(opts), connUsingNumber(0) {
    chakra::net::Connect::Options connOptions;
    connOptions.host = options.ip;
    connOptions.port = options.port;
    connOptions.connectTimeOut = options.connectTimeOut;
    connOptions.readTimeOut = options.readTimeOut;
    connOptions.writeTimeOut = options.writeTimeOut;
    for (int i = 0; i < options.maxIdleConns; i++) {
        auto conn = std::make_shared<net::Connect>(connOptions);
        auto err = conn->connect();
        if (err) {
            throw error::Error("connect host (" + connOptions.host + ":" + std::to_string(connOptions.port) + ") error " + err.what());
        }
        conns.push_back(conn);
    }
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

chakra::error::Error chakra::client::Chakra::set(const proto::client::SetMessageRequest& request, proto::client::SetMessageResponse& response) {
    auto err = executeCmd(request, proto::types::C_SET, response);
    if (err) return err;
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::mset(const proto::client::MSetMessageRequest& request, proto::client::MSetMessageResponse& response){
    auto err = executeCmd(request, proto::types::C_MSET, response);
    if (err) return err;
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
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
    auto err = executeCmd(request, proto::types::C_MPUSH, response);
    if (err) return err;
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::get(const proto::client::GetMessageRequest& request, proto::client::GetMessageResponse& response) {
    auto err = executeCmd(request, proto::types::C_GET, response);
    if (err) return err;
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
    }
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

chakra::error::Error chakra::client::Chakra::scan(const proto::client::ScanMessageRequest& request, proto::client::ScanMessageResponse& response) {
    auto err = executeCmd(request, proto::types::C_SCAN, response);
    if (err) {
        return err;
    }
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::incr(const proto::client::IncrMessageRequest& request, proto::client::IncrMessageResponse& response) {
    auto err = executeCmd(request, proto::types::C_INCR, response);
    if (err) {
        return err;
    }
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::mincr(const proto::client::MIncrMessageRequest& request, proto::client::MIncrMessageResponse& response) {
    auto err = executeCmd(request, proto::types::C_MINCR, response);
    if (err) {
        return err;
    }
    if (response.error().errcode() != 0) {
        return error::Error(response.error().errmsg());
    }
    return err;
}

chakra::error::Error chakra::client::Chakra::state(proto::peer::ClusterState &clusterState) {
    proto::peer::StateMessageRequest stateMessageRequest;
    stateMessageRequest.set_from(" ");
    proto::peer::StateMessageResponse stateMessageResponse;
    auto err = executeCmd(stateMessageRequest, proto::types::C_STATE, stateMessageResponse);
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

chakra::error::Error chakra::client::Chakra::executeCmd(const google::protobuf::Message &msg, proto::types::Type type, google::protobuf::Message &reply) {
    std::shared_ptr<chakra::net::Connect> conn;
    try {
        conn = connnectGet();
        conn->writeBuffer(msg, type);
        conn->sendBuffer();
        
        conn->receive([this, &reply, type](char *resp, size_t rsl) -> error::Error {
            auto reqtype = chakra::net::Packet::type(resp, rsl);
            if (reqtype == 0) {
                return error::Error("bad type received");
            }
            return chakra::net::Packet::deSerialize(resp, rsl, reply, type);
        });
        connectBack(conn);
        return error::Error();
    } catch (const error::ClientNotEnogthError& err) {
        return err; /* drop connect */
    } catch (const error::ConnectClosedError& err) {
        return err; /* drop connect */
    } catch (const error::Error& err) {
        LOG(INFO) << "execute cmd error: " << err.what();
        // connectBack(conn);
        return err;
    }
}

std::shared_ptr<chakra::net::Connect> chakra::client::Chakra::connnectGet() {
    std::lock_guard lock(mutex);
    if (conns.size() == 0 && connUsingNumber >= options.maxConns) {
        throw error::ClientNotEnogthError("too many clients(" + std::to_string(connUsingNumber) + ":" + std::to_string(options.maxConns) + ")");
    }

    if (conns.size() == 0) { //  新建一个链接
        chakra::net::Connect::Options connOptions;
        connOptions.host = options.ip;
        connOptions.port = options.port;
        connOptions.connectTimeOut = options.connectTimeOut;
        connOptions.readTimeOut = options.readTimeOut;
        connOptions.writeTimeOut = options.writeTimeOut;
        auto newc = std::make_shared<net::Connect>(connOptions);
        auto err = newc->connect();
        if (err) throw err;
        conns.push_back(newc);
    }

    auto conn = conns.front();
    conns.pop_front();
    connUsingNumber++;
    return conn;
}

void chakra::client::Chakra::connectBack(std::shared_ptr<net::Connect> conn) {
    std::lock_guard lock(mutex);
    conns.push_back(conn);
    connUsingNumber--;
}

std::string chakra::client::Chakra::peerName() const { return options.name; }
void chakra::client::Chakra::close() {
    std::lock_guard lock(mutex);
    for (int i = 0; i < conns.size(); i++){
        auto& conn = conns.front();
        conns.pop_front();
        connUsingNumber++;
        conn->close();
    }
}