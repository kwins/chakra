//
// Created by kwins on 2021/9/26.
//

#include "command_replica_sync_response.h"
#include "replica/replica_link.h"
#include "replica.pb.h"
#include "net/packet.h"
#include "utils/basic.h"

void chakra::cmds::CommandReplicaSyncResponse::execute(char *req, size_t len, void *data,
                                                       std::function<utils::Error(char *, size_t)> cbf) {
    auto link = static_cast<chakra::replica::Link*>(data);
    proto::replica::SyncMessageResponse syncMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, len, syncMessageResponse, proto::types::R_SYNC_RESPONSE);
    if (!err.success()){
        LOG(ERROR) << "replica sync response deserialize error " << err.toString();
    } else if (syncMessageResponse.error().errcode() != 0){
        LOG(ERROR) << "unexpected errcode to PSYNC from primary " << syncMessageResponse.error().errmsg();
    } else {
        link->setLastInteractionMs(utils::Basic::getNowMillSec());
        if (syncMessageResponse.psync_type() == proto::types::R_FULLSYNC){
            LOG(INFO) << "PRIMARY <-> REPLICATE accepted a FULL sync.";
            link->setState(chakra::replica::Link::State::TRANSFOR);
            link->setLastTransferMs(utils::Basic::getNowMillSec());

        } else if (syncMessageResponse.psync_type() == proto::types::R_PARTSYNC){
            LOG(INFO) << "PRIMARY <-> REPLICATE accepted a PART sync.";
            link->setState(chakra::replica::Link::State::CONNECTED);
            link->setRocksSeq(syncMessageResponse.seq());
            link->startPullDelta(); // 触发一次 pull delta
        } else {
            LOG(INFO) << "PRIMARY <-> REPLICATE accepted a BAD sync type (" << syncMessageResponse.psync_type() << ")";
        }
    }
}
