//
// Created by 王振奎 on 2021/7/10.
//

#include "command_replica_delta_pull.h"
#include "replica.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include "replica/replica.h"
#include "utils/basic.h"

void chakra::cmds::CommandReplicaDeltaPull::execute(char *req, size_t reqLen, void *data,
                                                    std::function<error::Error(char *resp, size_t respLen)> cbf) {
    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());

    proto::replica::DeltaMessageRequest deltaMessageRequest;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, deltaMessageRequest, proto::types::R_DELTA_REQUEST);
    if (err) {
        LOG(ERROR) << "replica delta pull deserialize error " << err.what();
        return;
    }

    proto::replica::DeltaMessageResponse deltaMessageResponse;
    deltaMessageResponse.set_db_name(deltaMessageRequest.db_name());

    auto& dbptr = database::FamilyDB::get();
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    err = dbptr->getUpdateSince(deltaMessageRequest.db_name(), deltaMessageRequest.seq(), &iter);
    if (err) {
        chakra::net::Packet::fillError(*deltaMessageResponse.mutable_error(), 1, err.what());
    } else {
        int bytes = 0;
        uint64_t seq = 0;
        while (iter->Valid()){
            auto batch = iter->GetBatch();
            if (bytes >= deltaMessageRequest.size())
                break;
            if (batch.sequence > deltaMessageRequest.seq()){
                bytes += batch.writeBatchPtr->Data().size();
                auto batchMsg = deltaMessageResponse.mutable_seqs()->Add();
                batchMsg->set_data(batch.writeBatchPtr->Data());
                batchMsg->set_seq(batch.sequence);
                seq = batch.sequence;
            }
            iter->Next();
        }
    }
    chakra::net::Packet::serialize(deltaMessageResponse, proto::types::R_DELTA_RESPONSE, cbf);
}
