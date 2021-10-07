//
// Created by 王振奎 on 2021/7/11.
//

#include <replica/replica_link.h>
#include "command_replica_sync_request.h"
#include "net/packet.h"
#include "database/db_family.h"


void chakra::cmds::CommandReplicaSyncRequest::execute(char *req, size_t reqLen, void *data,
                                                      std::function<utils::Error(char *resp, size_t respLen)> cbf) {
    LOG(INFO) << "************ CommandReplicaSyncRequest ************ start";
    proto::replica::SyncMessageResponse syncMessageResponse;
    proto::replica::SyncMessageRequest syncMessageRequest;
    auto link = static_cast<chakra::replica::Link*>(data);
    auto& dbptr = chakra::database::FamilyDB::get();

    auto err = chakra::net::Packet::deSerialize(req, reqLen, syncMessageRequest, proto::types::R_SYNC_REQUEST);
    if (!err.success()) {
        chakra::net::Packet::fillError(syncMessageResponse.mutable_error(), err.getCode(), err.getMsg());
    } else{
        LOG(INFO) << "CommandReplicaSync message " << syncMessageRequest.DebugString();

        syncMessageResponse.set_db_name(syncMessageRequest.db_name());
        syncMessageResponse.set_seq(syncMessageRequest.seq());
        if (!syncMessageRequest.db_name().empty() && syncMessageRequest.seq() >= 0){ // 增量同步
            LOG(INFO) << "CommandReplicaSync execute part sync...";
            syncMessageResponse.set_db_name(syncMessageRequest.db_name());
            syncMessageResponse.set_seq(syncMessageRequest.seq());

            // 如果请求的 seq 已经过期，则需要全量同步
            std::unique_ptr<rocksdb::TransactionLogIterator> iter;
            err = dbptr.getUpdateSince(syncMessageRequest.db_name(), syncMessageRequest.seq(), &iter);
            if (!err.success()){
                chakra::net::Packet::fillError(syncMessageResponse.mutable_error(), err.getCode(), err.getMsg());
            } else {
                if (iter->Valid() && iter->GetBatch().sequence > syncMessageRequest.seq()){ // 执行全量同步
                    err = link->snapshotBulk(syncMessageRequest.db_name());
                    if (!err.success()){
                        chakra::net::Packet::fillError(*syncMessageResponse.mutable_error(), err.getCode(), err.getMsg());
                    } else {
                        syncMessageResponse.set_psync_type(proto::types::R_FULLSYNC);
                    }
                }else{ // 执行增量同步
                    link->setState(replica::Link::State::CONNECTED);
                    syncMessageResponse.set_psync_type(proto::types::R_PARTSYNC);
                }
            }
        } else if (!syncMessageRequest.db_name().empty() && syncMessageRequest.seq() < 0){ // 全量同步
            LOG(INFO) << "CommandReplicaSync execute full sync...";
            err = link->snapshotBulk(syncMessageRequest.db_name());
            if (!err.success()){
                chakra::net::Packet::fillError(*syncMessageResponse.mutable_error(), err.getCode(), err.getMsg());
            } else {
                syncMessageResponse.set_psync_type(proto::types::R_FULLSYNC);
            }
        } else {
            chakra::net::Packet::fillError(*syncMessageResponse.mutable_error(), 1, "Parameter illegal");
        }
    }

    LOG(INFO) << "CommandReplicaSync send " << syncMessageResponse.DebugString();
    chakra::net::Packet::serialize(syncMessageResponse, proto::types::R_SYNC_RESPONSE, cbf);
    LOG(INFO) << "************ CommandReplicaSyncRequest ************ end";
}
