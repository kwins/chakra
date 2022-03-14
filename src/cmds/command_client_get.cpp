//
// Created by kwins on 2021/9/2.
//

#include "command_client_get.h"
#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"

void
chakra::cmds::CommandClientGet::execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) {
    proto::client::GetMessageResponse getMessageResponse;
    proto::client::GetMessageRequest getMessageRequest;
    auto err = chakra::net::Packet::deSerialize(req, len, getMessageRequest, proto::types::C_GET);
    if (err) {
        chakra::net::Packet::fillError(getMessageResponse.mutable_error(), 1, err.what());
    } else {
        auto dbptr = chakra::database::FamilyDB::get();
        auto value = dbptr->get(getMessageRequest.db_name(), getMessageRequest.key());
        if (!value) {
            chakra::net::Packet::fillError(getMessageResponse.mutable_error(), 1, "data not found");
        } else {
            if (value->getObject()->type() == database::Object::Type::STRING) {
                value->getObject()->serialize([&getMessageResponse](char* data, size_t len) {
                    getMessageResponse.set_type(database::Object::Type::STRING);
                    getMessageResponse.set_s(data, len);
                });
            } else {
                chakra::net::Packet::fillError(getMessageResponse.mutable_error(), 1, "data type not support");
            }
        }
    }
    chakra::net::Packet::serialize(getMessageResponse, proto::types::C_GET, cbf);
}
