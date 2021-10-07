//
// Created by kwins on 2021/9/27.
//

#include "command_replica_delta_recv.h"
#include "replica.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include "replica/replica_link.h"
#include "utils/basic.h"

void chakra::cmds::CommandReplicaDeltaRecv::execute(char *req, size_t reqLen, void *data,
                                                    std::function<utils::Error(char *, size_t)> cbf) {

    LOG(INFO) << "******** CommandReplicaDeltaRecv *********** start";
    proto::replica::DeltaMessageResponse deltaMessageResponse;
    auto link = static_cast<replica::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());
    auto err = chakra::net::Packet::deSerialize(req, reqLen, deltaMessageResponse, proto::types::R_DELTA_RESPONSE);
    if (!err.success()){
        LOG(ERROR) << "REPL delta recv deserialize error " << err.toString();
    } else if (deltaMessageResponse.error().errcode() != 0){
        LOG(ERROR) << "REPL delta recv response error " << deltaMessageResponse.error().errmsg();
    } else {
        auto& dbptr = database::FamilyDB::get();
        for (int i = 0; i < deltaMessageResponse.seqs_size(); ++i) {
            auto& seq = deltaMessageResponse.seqs(i);
            rocksdb::WriteBatch batch(seq.data());
            err = dbptr.put(deltaMessageResponse.db_name(), batch);
            if (err.success()){
                link->setRocksSeq(seq.seq());
            } else {
                LOG(ERROR) << "Delta recv error " << err.toString();
            }
        }
    }
    link->startPullDelta(); // 轮询触发 pull delta
    LOG(INFO) << "******** CommandReplicaDeltaRecv *********** end seqs size:" << deltaMessageResponse.seqs_size();
}
