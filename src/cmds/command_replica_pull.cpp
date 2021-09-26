//
// Created by 王振奎 on 2021/7/10.
//

#include "command_replica_pull.h"
#include "replica.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include "replica/replica_link.h"
#include "utils/basic.h"

void chakra::cmds::CommandReplicaPull::execute(char *req, size_t reqLen, void *data,
                                               std::function<utils::Error(char *resp, size_t respLen)> cbf) {
    LOG(INFO) << "**********CommandReplicaPull**********";
    auto link = static_cast<replica::Link*>(data);
    proto::replica::DeltaMessageRequest deltaMessageRequest;
    if (!chakra::net::Packet::deSerialize(req, reqLen, deltaMessageRequest, proto::types::R_PULL).success()) return;

    LOG(INFO) << deltaMessageRequest.DebugString();

    proto::replica::DeltaMessageResponse deltaMessageResponse;
    auto& dbptr = database::FamilyDB::get();
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto err = dbptr.getUpdateSince(deltaMessageRequest.db_name(), deltaMessageRequest.seq(), &iter);
    if (!err.success()){
        chakra::net::Packet::fillError(*deltaMessageResponse.mutable_error(), 1, err.toString());
    } else {
        int bytes = 0;
        int64_t seq = -1;
        while (iter->Valid()){
            auto batch = iter->GetBatch();
            if (bytes >= deltaMessageRequest.size())
                break;

            bytes += batch.writeBatchPtr->Data().size();
            auto batchMsg = deltaMessageResponse.mutable_seqs()->Add();
            batchMsg->set_data(batch.writeBatchPtr->Data());
            batchMsg->set_seq(batch.sequence);
            seq = batch.sequence;
            iter->Next();
        }
        LOG(INFO) << "REPL pull bytes " << bytes << " start seq=" << deltaMessageRequest.seq() << " end seq=" << seq << " size=" << deltaMessageResponse.seqs_size();
    }
    link->setLastInteractionMs(utils::Basic::getNowMillSec());
    chakra::net::Packet::serialize(deltaMessageResponse, proto::types::R_PULL, cbf);
}
