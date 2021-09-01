//
// Created by 王振奎 on 2021/7/11.
//

#include <replica/replica_link.h>
#include "command_replica_sync.h"
#include "net/packet.h"
#include "database/db_family.h"


void chakra::cmds::CommandReplicaSync::execute(char *req, size_t reqLen, void *data,
                                               std::function<void(char *resp, size_t respLen)> cbf) {
    proto::replica::SyncMessageRequest syncMessageRequest;
    if (!chakra::net::Packet::deSerialize(req, reqLen, syncMessageRequest)){
        return;
    }

    auto link = static_cast<chakra::replica::Link*>(data);
    auto dbptr = chakra::database::FamilyDB::get();

    proto::replica::SyncMessageResponse syncMessageResponse;
    if (!syncMessageRequest.db_name().empty() && syncMessageRequest.seq() > 0){ // 增量同步
        // 如果请求的 seq 已经过期，则需要全量同步
        std::unique_ptr<rocksdb::TransactionLogIterator> iter;
        auto err = dbptr->fetch(syncMessageRequest.db_name(), syncMessageRequest.seq(), &iter);
        if (err){
            chakra::net::Packet::fillError(*syncMessageResponse.mutable_error(), 1, err.toString());
        } else {
            if (iter->Valid() && iter->GetBatch().sequence > syncMessageRequest.seq()){ // 执行全量同步
                err = startBulk(link, syncMessageRequest);
                if (err){
                    chakra::net::Packet::fillError(*syncMessageResponse.mutable_error(), 1, err.toString());
                }
            }else{ // 执行增量同步
                link->setState(replica::Link::State::CONNECTED);
            }
        }
    } else if (!syncMessageRequest.db_name().empty() && syncMessageRequest.seq() == -1){ // 全量同步
        auto err = startBulk(link, syncMessageRequest);
        if (err){
            chakra::net::Packet::fillError(*syncMessageResponse.mutable_error(), 1, err.toString());
        }
    } else {
        chakra::net::Packet::fillError(*syncMessageResponse.mutable_error(), 1, "Parameter illegal");
    }

    chakra::net::Packet::serialize(syncMessageResponse, proto::types::R_PSYNC, cbf);
}

chakra::utils::Error
chakra::cmds::CommandReplicaSync::startBulk(chakra::replica::Link *link, proto::replica::SyncMessageRequest& request) {
    auto dbptr = chakra::database::FamilyDB::get();
    rocksdb::Iterator* iterator;
    rocksdb::SequenceNumber lastSeq;
    auto err = dbptr->snapshot(request.db_name(), &iterator, lastSeq);
    if (!err) link->startSendBulk(iterator, lastSeq);
    return err;
}
