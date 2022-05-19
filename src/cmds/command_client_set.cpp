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

void
chakra::cmds::CommandClientSet::execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) {
    proto::client::SetMessageResponse setMessageResponse;
    proto::client::SetMessageRequest setMessageRequest;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, setMessageRequest, proto::types::C_SET);
    if (err) {
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, err.what());
    }if (!dbptr->servedDB(setMessageRequest.db_name())) {
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, "DB " + setMessageRequest.db_name() + " not exist.");
    } else if (setMessageRequest.key().empty()) {
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, "bad arguments");
    } else {
        switch (setMessageRequest.type()) {
        case proto::element::ElementType::STRING:
            dbptr->set(setMessageRequest.db_name(), setMessageRequest.key(), setMessageRequest.s(), setMessageRequest.ttl());
            break;
        case proto::element::ElementType::INTEGER:
            dbptr->set(setMessageRequest.db_name(), setMessageRequest.key(), setMessageRequest.i(), setMessageRequest.ttl());
            break;
        case proto::element::ElementType::FLOAT:
            dbptr->set(setMessageRequest.db_name(), setMessageRequest.key(), setMessageRequest.f(), setMessageRequest.ttl());
        default:
            break;
        }
    }
    chakra::net::Packet::serialize(setMessageResponse, chakra::net::Packet::getType(req,len), cbf);
}
