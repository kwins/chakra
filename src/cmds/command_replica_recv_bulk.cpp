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
    if (!err.success()) {
        LOG(ERROR) << "Replicate recv bulk deserialize error " << err.what();
    } else if (link->getState() != chakra::replica::Replicate::Link::State::CONNECTED) {
        LOG(WARNING) << "Replicate link state not connected when recv bulk.";
    } else {
        LOG(INFO) << "Replicate receive bulk message" << bulkMessage.DebugString();

        auto nowms = utils::Basic::getNowMillSec();
        link->setLastInteractionMs(nowms);
        link->setLastTransferMs(bulkMessage.db_name(), nowms);
        if (bulkMessage.kvs_size() > 0){
            auto dbptr = chakra::database::FamilyDB::get();
            rocksdb::WriteBatch batch;
            for(auto& it : bulkMessage.kvs()){
                batch.Put(it.key(), it.value());
            }
            err = dbptr->putAll(bulkMessage.db_name(), batch);
            if (!err.success()) {
                LOG(ERROR) << "REPL set db " << bulkMessage.db_name() << " error " << err.what();
            }
        }
        
        if (bulkMessage.end()) {
            link->setState(chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED);
            link->setRocksSeq(bulkMessage.db_name(), bulkMessage.seq());
            link->startPullDelta(bulkMessage.db_name()); // 触发 pull delta
            LOG(INFO) << "Full replicate db " << bulkMessage.db_name() 
                        << " from " << link->getPeerName() << " finished and start pull delta.";
        }
    }
}
