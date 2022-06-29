//
// Created by kwins on 2021/9/24.
//

#include "command_replica_heartbeat.h"
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <types.pb.h>

#include "replica/replica.h"
#include "utils/basic.h"
#include "replica.pb.h"
#include "net/packet.h"

DECLARE_int64(replica_delta_delay_num);

void chakra::cmds::CommandReplicaHeartbeat::execute(char *req, size_t len, void *data) {
    auto link = static_cast<replica::Replicate::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());

    proto::replica::HeartBeatMessage heart;
    auto err = chakra::net::Packet::deSerialize(req, len, heart, proto::types::R_HEARTBEAT);
    if (err) {
        LOG(ERROR) << err.what();
    } else if (heart.positive()) {
        proto::replica::LinkReplicaState rs;
        link->replicateState(rs);
        for (auto& seq : heart.seqs()) { /* 主节点 */
            for (auto& state : rs.replicas()) { /* 当前节点 */
                LOG_IF(WARNING, seq.db_name() == state.db_name() && seq.seq() - state.delta_seq() > FLAGS_replica_delta_delay_num)
                                << "[replication] replicating db " << seq.db_name()  << " from " << link->getPeerName()
                                << " delay " << (seq.seq() - state.delta_seq()) << " sequence number,"
                                << " default is " << FLAGS_replica_delta_delay_num;
            }
        }
    }
}
