#include "command_client_mpush.h"

#include <cstdint>
#include <element.pb.h>
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
        for(auto& sub : mpushMessageRequest.datas()) {
            if (!dbptr->servedDB(sub.db_name())) {
                chakra::net::Packet::fillError(mpushMessageResponse.mutable_error(), 1, "DB " + sub.db_name() + " not exist.");
                return;
            }
            if (sub.key().empty()) {
                chakra::net::Packet::fillError(mpushMessageResponse.mutable_error(), 1, "Request key empty");
                return;
            }
        }

        for(auto& sub : mpushMessageRequest.datas()) {
            switch (sub.type()) {
            case proto::element::ElementType::STRING:
            {
                dbptr->push(sub.db_name(), sub.key(), std::vector<std::string>{ sub.s() }, sub.ttl());
                break;
            }
            case proto::element::ElementType::FLOAT:
            {
                dbptr->push(sub.db_name(), sub.key(), std::vector<float>{ sub.f() }, sub.ttl());
                break;
            }
            case proto::element::ElementType::STRING_ARRAY:
            {
                std::vector<std::string> values(sub.ss().value().begin(), sub.ss().value().end());
                dbptr->push(sub.db_name(), sub.key(), values , sub.ttl());
                break;
            }
            case proto::element::ElementType::FLOAT_ARRAY:
            {
                std::vector<float> values(sub.ff().value().begin(), sub.ff().value().end());
                dbptr->push(sub.db_name(), sub.key(), values , sub.ttl());
                break;   
            }
            default:
                break;
            }
        }
        chakra::net::Packet::serialize(mpushMessageResponse, proto::types::C_MPUSH, cbf);
    }
}
