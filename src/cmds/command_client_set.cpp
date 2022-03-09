//
// Created by kwins on 2021/8/27.
//

#include "command_client_set.h"
#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"
#include "utils/basic.h"
#include "database/type_string.h"

void
chakra::cmds::CommandClientSet::execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) {
    proto::client::SetMessageResponse setMessageResponse;
    proto::client::SetMessageRequest setMessageRequest;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, setMessageRequest, proto::types::C_SET);
    if (!err.success()){
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), err.getCode(), err.getMsg());
    }if (!dbptr->servedDB(setMessageRequest.db_name())){
        chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, "DB " + setMessageRequest.db_name() + " not exist.");
    }else {
        auto element = std::make_shared<chakra::database::Element>();
        element->setUpdated(true);
        element->setCreate(utils::Basic::getNowMillSec());
        element->setUpdated(element->getCreate());
        element->setLastVisit(-1);

        if (setMessageRequest.type() == database::Object::Type::STRING){
            auto obj = std::make_shared<chakra::database::String>();
            obj->deSeralize(setMessageRequest.value().data(), setMessageRequest.value().size());
            element->setObject(obj);
            dbptr->put(setMessageRequest.db_name(), setMessageRequest.key(), element);
        } else {
            chakra::net::Packet::fillError(setMessageResponse.mutable_error(), 1, "Data type Not support");
        }
    }
    chakra::net::Packet::serialize(setMessageResponse, chakra::net::Packet::getType(req,len), cbf);
}
