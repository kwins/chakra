//
// Created by 王振奎 on 2021/7/10.
//

#include "command_replica_delta_pull.h"
#include "replica.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include "replica/replica.h"
#include "utils/basic.h"
#include <rocksdb/types.h>

void chakra::cmds::CommandReplicaDeltaPull::execute(char *req, size_t reqLen, void *data) {
    auto st = utils::Basic::getNowMillSec();
    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());

    proto::replica::DeltaMessageRequest deltaMessageRequest;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, deltaMessageRequest, proto::types::R_DELTA_REQUEST);
    if (err) {
        LOG(ERROR) << "[replication] delta pull message deserialize error " << err.what();
        return;
    }

    proto::replica::DeltaMessageResponse deltaMessageResponse;
    deltaMessageResponse.set_db_name(deltaMessageRequest.db_name());

    auto& dbptr = database::FamilyDB::get();
    rocksdb::SequenceNumber lastSeq;
    err = dbptr->getLastSeqNumber(deltaMessageRequest.db_name(), lastSeq);
    if (err) { /* 当没有更新的时候，这里直接使用 GetUpdateSince 会相当耗费 CPU(rocksdb) */
        fillError(deltaMessageResponse.mutable_error(), 1, err.what());
    } else if (lastSeq > deltaMessageRequest.seq()) {
        std::unique_ptr<rocksdb::TransactionLogIterator> iter;
        err = dbptr->getUpdateSince(deltaMessageRequest.db_name(), deltaMessageRequest.seq(), &iter);
        if (err) {
            fillError(deltaMessageResponse.mutable_error(), 1, err.what());
        } else {
            DLOG(INFO) << "[replication] delta pull request: " << deltaMessageRequest.DebugString();
            int bytes = 0;
            uint64_t seq = 0;
            while (iter->Valid()) {
                auto batch = iter->GetBatch();
                if (bytes >= deltaMessageRequest.size())
                    break;
                if (batch.sequence > deltaMessageRequest.seq()) {
                    bytes += batch.writeBatchPtr->Data().size();
                    auto batchMsg = deltaMessageResponse.mutable_seqs()->Add();
                    batchMsg->set_data(batch.writeBatchPtr->Data());
                    batchMsg->set_seq(batch.sequence);
                    seq = batch.sequence;
                }
                iter->Next();
            }
        }
    }
    DLOG(INFO) << "[replication] delta pull response: " << deltaMessageResponse.DebugString();
    link->asyncSendMsg(deltaMessageResponse, proto::types::R_DELTA_RESPONSE);
}
