//
// Created by kwins on 2021/9/2.
//

#include "command_client_get.h"
#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"

void
chakra::cmds::CommandClientGet::execute(char *req, size_t len, void *data, std::function<utils::Error(char *, size_t)> cbf) {
    proto::client::GetMessageResponse getMessageResponse;
    proto::client::GetMessageRequest getMessageRequest;
    auto err = chakra::net::Packet::deSerialize(req, len, getMessageRequest, proto::types::C_GET);
    if (!err.success()){
        chakra::net::Packet::fillError(getMessageResponse.mutable_error(), err.getCode(), err.getMsg());
    } else {
        LOG(INFO) << "1";
        auto& dbptr = chakra::database::FamilyDB::get();
        LOG(INFO) << "1-1";
        auto value = dbptr.get(getMessageRequest.db_name(), getMessageRequest.key());
        if (!value){
            LOG(INFO) << "2";
            chakra::net::Packet::fillError(getMessageResponse.mutable_error(), 1, "data not found");
        } else {
            LOG(INFO) << "3";
            if (value->getObject()->type() == database::Object::Type::STRING){
                LOG(INFO) << "4";
                value->getObject()->serialize([&getMessageResponse](char* data, size_t len){
                    getMessageResponse.set_type(database::Object::Type::STRING);
                    getMessageResponse.set_s(data, len);
                });
                LOG(INFO) << "5";
            } else {
                LOG(INFO) << "6";
                chakra::net::Packet::fillError(getMessageResponse.mutable_error(), 1, "data type not support");
            }
        }
    }
    LOG(INFO) << "7";
    chakra::net::Packet::serialize(getMessageResponse, proto::types::C_GET, cbf);
}
