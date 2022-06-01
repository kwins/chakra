#include "command_client_mincr.h"
#include "client.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include <chrono>
#include <cluster/cluster.h>
#include <cstddef>
#include <element.pb.h>
#include <error/err.h>
#include <exception>
#include <rocksdb/options.h>
#include <service/chakra.h>
#include <types.pb.h>

void chakra::cmds::CommandClientMIncr::execute(char *req, size_t len, void *data) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(data);
    proto::client::MIncrMessageRequest mincrMessageRequest;
    proto::client::MIncrMessageResponse mincrMessageResponse;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, mincrMessageRequest, proto::types::C_MINCR);
    if (err) {
        fillError(mincrMessageResponse.mutable_error(), 1, err.what());
    } else if (mincrMessageRequest.datas().size() == 0) {
        fillError(mincrMessageResponse.mutable_error(), 1, "empty data");
    } else {
        int fails = 0;
        for (auto& sub : mincrMessageRequest.datas()) {
            auto& state = (*mincrMessageResponse.mutable_states())[sub.db_name()];
            auto& info = (*state.mutable_value())[sub.key()];
            info.set_succ(true);
            switch (sub.type()) {
            case proto::element::ElementType::FLOAT:
                err = err = dbptr->incr(sub.db_name(), sub.key(), sub.f());
                break;
            default:
                err = error::Error("type must be float");
                break;
            }
            if (err) {
                fails++;
                info.set_succ(false);
                info.set_errmsg(err.what());
            }            
        }
        if (fails > 0 && fails == mincrMessageRequest.datas_size()) {
            fillError(mincrMessageResponse.mutable_error(), 1, "all fail");
        }
    }
    link->asyncSendMsg(mincrMessageResponse, proto::types::C_MINCR);
}