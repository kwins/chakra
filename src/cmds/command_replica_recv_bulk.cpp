//
// Created by kwins on 2021/9/27.
//

#include "command_replica_recv_bulk.h"

#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "replica/replica.h"
#include "database/db_family.h"

void chakra::cmds::CommandReplicaRecvBulk::execute(char *req, size_t reqLen, void *data,
                                                   std::function<error::Error(char *, size_t)> cbf) {

    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    proto::replica::BulkMessage bulkMessage;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, bulkMessage, proto::types::R_BULK);
    if (err) {
        LOG(ERROR) << "Replicate recv bulk deserialize error " << err.what();
    } else if (link->getState() != chakra::replica::Replicate::Link::State::CONNECTED) {
        LOG(WARNING) << "Replicate link state not connected when recv bulk(" << (int)link->getState() << ")";
    } else {
        LOG(INFO) << "Replicate receive bulk message" << bulkMessage.DebugString();
        auto replicateDB = link->getReplicateDB(bulkMessage.db_name());
        if (!replicateDB) {
            LOG(ERROR) << "Replicate receive bulk message db " << bulkMessage.db_name() << " not found in server.";
        } else {
            link->setLastInteractionMs(utils::Basic::getNowMillSec());
            replicateDB->lastTransferMs = link->getLastInteractionMs();
            if (bulkMessage.kvs_size() > 0) {
                auto dbptr = chakra::database::FamilyDB::get();
                rocksdb::WriteBatch batch;
                for(auto& it : bulkMessage.kvs()) {
                    batch.Put(it.key(), it.value());
                }
                err = dbptr->putAll(bulkMessage.db_name(), batch);
                if (err) {
                    LOG(ERROR) << err.what();
                    link->close();
                    return;
                }
            }
            if (bulkMessage.end()) {
                replicateDB->state = chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED;
                replicateDB->deltaSeq = bulkMessage.seq();
                replica::Replicate::get()->dumpReplicateStates();
                replicateDB->startPullDelta(); // 触发 pull delta
                LOG(INFO) << "Full replicate db " << bulkMessage.db_name() 
                            << " from " << link->getPeerName() << " finished and start pull delta.";
            }
        }
    }
}
