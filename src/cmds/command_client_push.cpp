//
// Created by kwins on 2021/8/27.
//

#include "command_client_push.h"
#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"
#include "utils/basic.h"
#include <cstdint>
#include <element.pb.h>
#include <error/err.h>
#include <types.pb.h>
#include <vector>
#include <service/chakra.h>

void chakra::cmds::CommandClientPush::execute(char *req, size_t len, void *data) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(data);
    proto::client::PushMessageRequest pushMessageRequest;
    proto::client::PushMessageResponse pushMessageResponse;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, pushMessageRequest, proto::types::C_PUSH);
    if (err) {
        fillError(pushMessageResponse.mutable_error(), 1, err.what());
    } else if (!dbptr->servedDB(pushMessageRequest.db_name())) {
        fillError(pushMessageResponse.mutable_error(), 1, "DB " + pushMessageRequest.db_name() + " not exist.");
    } else if (pushMessageRequest.key().empty()) {
        fillError(pushMessageResponse.mutable_error(), 1, "bad arguments");
    } else {

        switch (pushMessageRequest.type()) {
        case proto::element::ElementType::STRING:
        {
            err = dbptr->push(pushMessageRequest.db_name(), pushMessageRequest.key(), std::vector<std::string>{ pushMessageRequest.s() }, pushMessageRequest.ttl());
            break;
        }
        case proto::element::ElementType::FLOAT:
        {
            err = dbptr->push(pushMessageRequest.db_name(), pushMessageRequest.key(), std::vector<float>{ pushMessageRequest.f() }, pushMessageRequest.ttl());
            break;
        }
        case proto::element::ElementType::STRING_ARRAY:
        {
            std::vector<std::string> values(pushMessageRequest.ss().value().begin(), pushMessageRequest.ss().value().end());
            err = dbptr->push(pushMessageRequest.db_name(), pushMessageRequest.key(), values , pushMessageRequest.ttl());
            break;
        }
        case proto::element::ElementType::FLOAT_ARRAY:
        {
            std::vector<float> values(pushMessageRequest.ff().value().begin(), pushMessageRequest.ff().value().end());
            err = dbptr->push(pushMessageRequest.db_name(), pushMessageRequest.key(), values , pushMessageRequest.ttl());
            break;   
        }
        default:
            err = error::Error("push command only supprt string array and float array type");
            break;
        }
        
        if (err) {
            fillError(pushMessageResponse.mutable_error(), 1, err.what());
        }
    }
    link->asyncSendMsg(pushMessageResponse, proto::types::C_PUSH);
}
