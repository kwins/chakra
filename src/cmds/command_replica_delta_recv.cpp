//
// Created by kwins on 2021/9/27.
//

#include "command_replica_delta_recv.h"
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "replica.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include "replica/replica.h"
#include "utils/basic.h"
#include "database/db_replica_batch.h"

DECLARE_int32(replica_timeout_ms);

void chakra::cmds::CommandReplicaDeltaRecv::execute(char *req, size_t reqLen, void *data) {
    auto st = utils::Basic::getNowMillSec();
    proto::replica::DeltaMessageResponse deltaMessageResponse;
    auto link = static_cast<replica::Replicate::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());

    auto err = chakra::net::Packet::deSerialize(req, reqLen, deltaMessageResponse, proto::types::R_DELTA_RESPONSE);
    if (err) {
        LOG(ERROR) << "[replication] delta message deserialize error " << err.what();
    } else if (deltaMessageResponse.error().errcode() != 0){
        LOG(ERROR) << "[replication] delta message response error " << deltaMessageResponse.error().errmsg();
    } else {
        DLOG(INFO) << "[replication] receive delta response " << deltaMessageResponse.DebugString();
        if (deltaMessageResponse.seqs_size() > 0) {
            auto dbptr = database::FamilyDB::get();
            database::ReplicaBatchHandler batchHandler;
            for (int i = 0; i < deltaMessageResponse.seqs_size(); ++i) {
                auto& seq = deltaMessageResponse.seqs(i);
                batchHandler.Put(seq.data());
            }

            err = dbptr->writeBatch(deltaMessageResponse.db_name(), batchHandler.GetBatch(), batchHandler.GetKeys());
            if (err) {
                LOG(ERROR) << "[replication]" << err.what();
            } else {
                link->setRocksSeq(deltaMessageResponse.db_name(), deltaMessageResponse.seqs(deltaMessageResponse.seqs_size()-1).seq());
            }
        }
    }

    LOG_IF(WARNING, (utils::Basic::getNowMillSec() - st) > FLAGS_replica_timeout_ms/2) 
                << "[replication] receive delta from " << link->getPeerName()
                << " spend " << (utils::Basic::getNowMillSec() - st) << "ms "
                << deltaMessageResponse.seqs_size() << " batch messages.";
    link->startPullDelta(deltaMessageResponse.db_name()); // next pull delta
}
