//
// Created by kwins on 2021/9/27.
//

#include "command_replica_recv_bulk.h"

#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "replica/replica.h"
#include "database/db_family.h"

void chakra::cmds::CommandReplicaRecvBulk::execute(char *req, size_t reqLen, void *data) {
    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    proto::replica::BulkMessage bulkMessage;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, bulkMessage, proto::types::R_BULK);
    if (err) {
        LOG(ERROR) << "[replication] bulk message deserialize error " << err.what();
    } else if (link->getState() != chakra::replica::Replicate::Link::State::CONNECTED) {
        LOG(WARNING) << "Replicate link state not connected when recv bulk(" << (int)link->getState() << ")";
    } else {
        auto replicateDB = link->getReplicateDB(bulkMessage.db_name());
        if (!replicateDB) {
            LOG(ERROR) << "[replication] bulk message db " << bulkMessage.db_name() << " not found in server.";
        } else if (replicateDB->state != chakra::replica::Replicate::Link::State::REPLICA_TRANSFORING) {
            LOG(ERROR) << "[replication] state not REPLICA_TRANSFORING";
        } else {
            auto dbptr = chakra::database::FamilyDB::get();
            link->setLastInteractionMs(utils::Basic::getNowMillSec());
            replicateDB->lastTransferMs = link->getLastInteractionMs();
            if (bulkMessage.kvs_size() > 0) {
                rocksdb::WriteBatch batch;
                for(auto& it : bulkMessage.kvs()) {
                    batch.Put(it.key(), it.value());
                }
                err = dbptr->writeBatch(bulkMessage.db_name(), batch, {});
                if (err) {
                    LOG(ERROR) << err.what();
                    replicateDB->reset();
                    return;
                }
            }

            replicateDB->itsize += bulkMessage.kvs_size();
            replicateDB->transferTimes++;
            if (!bulkMessage.end()) return;

            // ??????????????????
            dbptr->cacheClear(bulkMessage.db_name()); /* ???????????? */
            replicateDB->state = chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED;
            replicateDB->deltaSeq = bulkMessage.seq();
            replica::Replicate::get()->dumpReplicateStates();
            replicateDB->startPullDelta(); // ?????? pull delta

            LOG(INFO) << "[replication] receive full db " << bulkMessage.db_name() << " from " 
                      << link->getPeerName() << " success and spend " << (utils::Basic::getNowMillSec() - replicateDB->startTransferMs) << "ms " 
                      << replicateDB->transferTimes << " times to receive "
                      << replicateDB->itsize << " kv and now start pull delta.";
        }
    }
}
