//
// Created by kwins on 2021/9/27.
//

#include "command_replica_recv_bulk.h"
#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "replica/replica_link.h"
#include "database/db_family.h"

void chakra::cmds::CommandReplicaRecvBulk::execute(char *req, size_t reqLen, void *data,
                                                   std::function<error::Error(char *, size_t)> cbf) {
    auto link = static_cast<chakra::replica::Link*>(data);
    if (link->getState() != chakra::replica::Link::State::TRANSFOR){
        LOG(WARNING) << "replica link state not transfor when recv bulk.";
        return;
    }

    link->setLastTransferMs(utils::Basic::getNowMillSec());
    link->setLastInteractionMs(link->getLastTransferMs());

    proto::replica::BulkMessage bulkMessage;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, bulkMessage, proto::types::R_BULK);
    if (!err.success()){
        LOG(ERROR) << "replica recv bulk deserialize error " << err.what();
    } else {
        if (bulkMessage.kvs_size() > 0){
            auto& dbptr = chakra::database::FamilyDB::get();
            rocksdb::WriteBatch batch;
            for(auto& it : bulkMessage.kvs()){
                batch.Put(it.key(), it.value());
            }
            err = dbptr.putAll(bulkMessage.db_name(), batch);
            if (!err.success()){
                LOG(ERROR) << "REPL set db " << bulkMessage.db_name() << " error " << err.what();
            }
        }

        if (bulkMessage.end()){
            link->setState(chakra::replica::Link::State::CONNECTED);
            link->setRocksSeq(bulkMessage.seq());
            link->startPullDelta(); // 触发一次 pull delta
        }
    }
}
