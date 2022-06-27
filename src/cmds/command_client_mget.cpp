#include "command_client_mget.h"

#include <cstddef>
#include <element.pb.h>
#include <google/protobuf/map.h>
#include <types.pb.h>
#include <utility>

#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"
#include <service/chakra.h>

void chakra::cmds::CommandClientMGet::execute(char *req, size_t len, void *data) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(data);
    proto::client::MGetMessageRequest mGetMessageRequest;
    proto::client::MGetMessageResponse mGetMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, len, mGetMessageRequest, proto::types::C_MGET);
    if (err) {
        fillError(mGetMessageResponse.mutable_error(), 1, err.what());
    } else {
        auto dbptr = chakra::database::FamilyDB::get();
        std::unordered_map<std::string, std::vector<std::string>> dbkeys;
        for(auto& db : mGetMessageRequest.keys()) {
            for (auto& key : db.second.value()) {
                dbkeys[db.first].push_back(key);
            }
        }

        auto result = dbptr->mget(dbkeys);
        for(auto& db : result) {
            auto& elements = (*mGetMessageResponse.mutable_datas())[db.first];
            for (int i = 0; i < db.second.size(); i++) {
                auto& element = (*elements.mutable_value())[dbkeys.at(db.first).at(i)];
                if (db.second[i]) {
                    element.CopyFrom(*db.second[i]);
                } else { // 结果为空，只设置key
                    element.set_key(dbkeys.at(db.first).at(i));
                    element.set_type(::proto::element::ElementType::NF);
                }
            }
        }
    }
    link->asyncSendMsg(mGetMessageResponse, proto::types::C_MGET);
}
