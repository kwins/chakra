//
// Created by kwins on 2021/9/26.
//

#include "command_replica_sync_response.h"

#include "replica/replica.h"
#include "replica.pb.h"
#include "net/packet.h"
#include "utils/basic.h"

void chakra::cmds::CommandReplicaSyncResponse::execute(char *req, size_t len, void *data,
                                                       std::function<error::Error(char *, size_t)> cbf) {
    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    proto::replica::SyncMessageResponse syncMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, len, syncMessageResponse, proto::types::R_SYNC_RESPONSE);
    if (!err.success()) {
        LOG(ERROR) << "Replica sync response deserialize error " << err.what();
    } else if (syncMessageResponse.error().errcode() != 0) {
        LOG(ERROR) << "Unexpected errcode to PSYNC from primary " << syncMessageResponse.error().errmsg();
    } else {
        link->setLastInteractionMs(utils::Basic::getNowMillSec());
        switch (syncMessageResponse.psync_type()){
        case proto::types::R_FULLSYNC:
            {
                LOG(INFO) << "PRIMARY <-> REPLICATE accepted a FULL sync.";
                link->setState(chakra::replica::Replicate::Link::State::REPLICA_TRANSFORING);
                link->setLastTransferMs(syncMessageResponse.db_name(), utils::Basic::getNowMillSec());
                break;
            }
        case proto::types::R_PARTSYNC:
            {
                LOG(INFO) << "PRIMARY <-> REPLICATE accepted a PART sync.";
                link->setState(chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED);
                link->setRocksSeq(syncMessageResponse.db_name(), syncMessageResponse.seq());
                link->startPullDelta(syncMessageResponse.db_name()); // 触发一次 pull delta
                break;
            }
        default:
            {
                LOG(INFO) << "PRIMARY <-> REPLICATE accepted a BAD sync message " <<  syncMessageResponse.DebugString();
                break;
            }
        }
    }
}
