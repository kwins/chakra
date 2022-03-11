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
    if (err) {
        LOG(ERROR) << "Replica sync response deserialize error " << err.what();
    } else if (syncMessageResponse.error().errcode() != 0 || syncMessageResponse.db_name().empty()) {
        LOG(ERROR) << "Unexpected PSYNC response errcode from primary " << syncMessageResponse.DebugString();
    } else {
        link->setLastInteractionMs(utils::Basic::getNowMillSec());
        auto replicateDB = link->getReplicateDB(syncMessageResponse.db_name());
        if (!replicateDB) {
            LOG(ERROR) << "Replicate sync response db " << syncMessageResponse.db_name() << " not found in server.";
            return;
        }

        switch (syncMessageResponse.psync_type()) {
        case proto::types::R_FULLSYNC:
            {
                LOG(INFO) << "PRIMARY <-> REPLICATE accepted a FULL sync.";
                replicateDB->state = chakra::replica::Replicate::Link::State::REPLICA_TRANSFORING;
                replicateDB->lastTransferMs = utils::Basic::getNowMillSec();
                /* 因为需要全量同步，在还没有同步完成前，不更新deltaSeq，因为可能在同步过程中服务挂了，再次重启时使用deltaSeq的值为错误的 */
                /* 这里不更新的目标是，当服务同步过程中挂了，则下次重启再次全量同步 */
                break;
            }
        case proto::types::R_PARTSYNC:
            {
                LOG(INFO) << "PRIMARY <-> REPLICATE accepted a PART sync.";
                replicateDB->state = chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED;
                replicateDB->deltaSeq = syncMessageResponse.seq();
                replica::Replicate::get()->dumpReplicateStates();
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
