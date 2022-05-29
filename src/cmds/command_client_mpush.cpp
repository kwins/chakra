#include "command_client_mpush.h"

#include <cstdint>
#include <element.pb.h>
#include <error/err.h>
#include <types.pb.h>
#include <vector>

#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"
#include "utils/basic.h"
#include "database/type_string.h"


void chakra::cmds::CommandClientMPush::execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) {
    proto::client::MPushMessageRequest mpushMessageRequest;
    proto::client::MPushMessageResponse mpushMessageResponse;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, mpushMessageRequest, proto::types::C_MPUSH);
    if (err) {
        chakra::net::Packet::fillError(mpushMessageResponse.mutable_error(), 1, err.what());
    } else {
        int fails = 0;
        for(auto& sub : mpushMessageRequest.datas()) {
            auto& state = (*mpushMessageResponse.mutable_states())[sub.db_name()];
            auto& info = (*state.mutable_value())[sub.key()];
            info.set_succ(true);

            switch (sub.type()) {
            case proto::element::ElementType::STRING:
            {
                err = dbptr->push(sub.db_name(), sub.key(), std::vector<std::string>{ sub.s() }, sub.ttl());
                break;
            }
            case proto::element::ElementType::FLOAT:
            {
                err = dbptr->push(sub.db_name(), sub.key(), std::vector<float>{ sub.f() }, sub.ttl());
                break;
            }
            case proto::element::ElementType::STRING_ARRAY:
            {
                std::vector<std::string> values(sub.ss().value().begin(), sub.ss().value().end());
                err = dbptr->push(sub.db_name(), sub.key(), values , sub.ttl());
                break;
            }
            case proto::element::ElementType::FLOAT_ARRAY:
            {
                std::vector<float> values(sub.ff().value().begin(), sub.ff().value().end());
                err = dbptr->push(sub.db_name(), sub.key(), values , sub.ttl());
                break;   
            }
            default:
                err = error::Error("element type error");
                break;
            }
            
            if (err) {
                fails++;
                info.set_succ(false);
                info.set_errmsg(err.what());
            }
        }
        if (fails > 0 && fails == mpushMessageRequest.datas_size()) {
            chakra::net::Packet::fillError(mpushMessageResponse.mutable_error(), 1, "all fail");
        }
        chakra::net::Packet::serialize(mpushMessageResponse, proto::types::C_MPUSH, cbf);
    }
}
