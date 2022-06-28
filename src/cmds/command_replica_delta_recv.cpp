//
// Created by kwins on 2021/9/27.
//

#include "command_replica_delta_recv.h"
#include "replica.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include "replica/replica.h"
#include "utils/basic.h"

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
        auto dbptr = database::FamilyDB::get();
        for (int i = 0; i < deltaMessageResponse.seqs_size(); ++i) {
            auto& seq = deltaMessageResponse.seqs(i);
            rocksdb::WriteBatch batch(seq.data());
            err = dbptr->rocksWriteBulk(deltaMessageResponse.db_name(), batch);
            if (!err) {
                link->setRocksSeq(deltaMessageResponse.db_name(), seq.seq());
            } else {
                LOG(ERROR) << "[replication] receive delta error " << err.what();
            }
        }
    }

    LOG(INFO) << "[replication] receive delta from " << link->getPeerName() 
               << " spend " << (utils::Basic::getNowMillSec() - st) << "ms "
               << deltaMessageResponse.seqs_size() << " batch messages.";
    link->startPullDelta(deltaMessageResponse.db_name()); // next pull delta
}
