//
// Created by 王振奎 on 2021/7/11.
//

#include "command_replica_sync_request.h"

#include "replica/replica.h"
#include "net/packet.h"
#include "database/db_family.h"


void chakra::cmds::CommandReplicaSyncRequest::execute(char *req, size_t reqLen, void *data) {
    proto::replica::SyncMessageResponse syncMessageResponse;
    proto::replica::SyncMessageRequest syncMessageRequest;
    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    auto dbptr = chakra::database::FamilyDB::get();

    auto err = chakra::net::Packet::deSerialize(req, reqLen, syncMessageRequest, proto::types::R_SYNC_REQUEST);
    if (err) {
        fillError(syncMessageResponse.mutable_error(), 1, err.what());
    } else if (syncMessageRequest.db_name().empty()) {
        fillError(syncMessageResponse.mutable_error(), 1, "db name empty");
    } else {
        LOG(INFO) << "[replication] sync request: " << syncMessageRequest.DebugString();
        syncMessageResponse.set_db_name(syncMessageRequest.db_name());
        syncMessageResponse.set_seq(syncMessageRequest.seq());
        if (syncMessageRequest.seq() > 0) { /* 增量同步, 如果请求的 seq 已经过期，则需要全量同步 */
            std::unique_ptr<rocksdb::TransactionLogIterator> iter;
            err = dbptr->getUpdateSince(syncMessageRequest.db_name(), syncMessageRequest.seq(), &iter);
            if (err) {
                fillError(syncMessageResponse.mutable_error(), 1, err.what());
            } else if (iter->Valid() && iter->GetBatch().sequence == syncMessageRequest.seq()) {
                auto replicateDB = std::make_shared<chakra::replica::Replicate::Link::ReplicateDB>();
                replicateDB->name = syncMessageRequest.db_name();
                replicateDB->state = chakra::replica::Replicate::Link::State::REPLICA_TRANSFORED;
                replicateDB->deltaSeq = syncMessageRequest.seq();
                link->setReplicateDB(replicateDB);
                syncMessageResponse.set_psync_type(proto::types::R_PARTSYNC);
            } else {
                fullSync(link, syncMessageRequest, syncMessageResponse);
            }
        } else { 
            fullSync(link, syncMessageRequest, syncMessageResponse);
        }
    }
    
    LOG(INFO) << "[replication] sync response: " << syncMessageResponse.DebugString();
    link->asyncSendMsg(syncMessageResponse, proto::types::R_SYNC_RESPONSE);
}

void chakra::cmds::CommandReplicaSyncRequest::fullSync(chakra::replica::Replicate::Link* link, 
            proto::replica::SyncMessageRequest& request, proto::replica::SyncMessageResponse& response) {
    /* 1、delta 有效，但是获取的 第一个增量的delta seq 非请求的 seq 则说明历史delta已经被删除了 */
    /* 2、delta 无效 */
    rocksdb::SequenceNumber lastSeq;
    auto err = link->snapshotDB(request.db_name(), lastSeq);
    if (err) {
        fillError(response.mutable_error(), 1, err.what());
    } else {
        response.set_psync_type(proto::types::R_FULLSYNC);
        response.set_seq(lastSeq);
    }
}