//
// Created by kwins on 2021/8/27.
//

#include "command_client_mset.h"
#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"
#include "utils/basic.h"
#include "database/type_string.h"
#include <element.pb.h>
#include <error/err.h>

void chakra::cmds::CommandClientMSet::execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) {
    proto::client::MSetMessageRequest msetMessageRequest;
    proto::client::MSetMessageResponse msetMessageResponse;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, msetMessageRequest, proto::types::C_MSET);
    if (err) {
        chakra::net::Packet::fillError(msetMessageResponse.mutable_error(), 1, err.what());
    } else if (msetMessageRequest.datas().size() == 0) {
        chakra::net::Packet::fillError(msetMessageResponse.mutable_error(), 1, "empty arguments");
    } else {
        for(auto& sub : msetMessageRequest.datas()) {
            auto& state = (*msetMessageResponse.mutable_states())[sub.db_name()];
            auto& info = (*state.mutable_value())[sub.key()];
            info.set_succ(true);

            switch (sub.type()) {
            case proto::element::ElementType::STRING:
                err = dbptr->set(sub.db_name(), sub.key(), sub.s(), sub.ttl());
                break;
            case proto::element::ElementType::FLOAT:
                err = dbptr->set(sub.db_name(), sub.key(), sub.f(), sub.ttl());
            default:
                err = error::Error("set command only supprt string and float type");
                break;
            }
            if (err) {
                info.set_succ(false);
                info.set_errmsg(err.what());
            }
        }
    }
    chakra::net::Packet::serialize(msetMessageResponse, proto::types::C_MSET, cbf);
}
