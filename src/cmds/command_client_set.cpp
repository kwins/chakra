//
// Created by kwins on 2021/8/27.
//

#include "command_client_set.h"
#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"
#include "utils/basic.h"
#include "database/type_string.h"
#include <element.pb.h>
#include <error/err.h>
#include <service/chakra.h>

void chakra::cmds::CommandClientSet::execute(char *req, size_t len, void *data) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(data);
    proto::client::SetMessageResponse setMessageResponse;
    proto::client::SetMessageRequest setMessageRequest;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, setMessageRequest, proto::types::C_SET);
    if (err) {
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, err.what());
    } else if (!dbptr->servedDB(setMessageRequest.db_name())) {
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, "DB " + setMessageRequest.db_name() + " not exist.");
    } else if (setMessageRequest.key().empty()) {
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, "bad arguments");
    } else {
        DLOG(INFO) << "[chakra] set request: " << setMessageRequest.DebugString();
        switch (setMessageRequest.type()) {
        case proto::element::ElementType::STRING:
            err = dbptr->set(setMessageRequest.db_name(), setMessageRequest.key(), setMessageRequest.s(), setMessageRequest.ttl());
            break;
        case proto::element::ElementType::FLOAT:
            err = dbptr->set(setMessageRequest.db_name(), setMessageRequest.key(), setMessageRequest.f(), setMessageRequest.ttl());
            break;
        default:
            err = error::Error("set command only supprt string and float type");
            break;
        }
        if (err) {
            chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, err.what());
        }
    }
    link->asyncSendMsg(setMessageResponse, proto::types::C_SET);
}
