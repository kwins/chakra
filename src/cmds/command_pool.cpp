//
// Created by kwins on 2021/5/27.
//

#include "command_pool.h"

#include "command_cluster_ping.h"
#include "command_cluster_meet.h"
#include "command_cluster_pong.h"
#include "command_cluster_meet_peer.h"
#include "command_cluster_fail.h"
#include "command_cluster_set_db.h"
#include "command_cluster_set_epoch.h"
#include "command_cluster_state.h"

#include "command_client_set.h"
#include "command_client_get.h"
#include "command_client_mget.h"
#include "command_replica_heartbeat.h"
#include "command_replica_delta_pull.h"
#include "command_replica_delta_recv.h"
#include "command_replica_ping.h"
#include "command_replica_pong.h"
#include "command_replica_sync_request.h"
#include "command_replica_sync_response.h"
#include "command_replica_recv_bulk.h"
#include <memory>
#include <types.pb.h>

chakra::cmds::CommandPool::CommandPool() {
    cmdnf = std::make_shared<CommandNF>();
    // cluster
    regCmd(proto::types::P_PING, std::make_shared<CommandClusterPing>());
    regCmd(proto::types::P_MEET, std::make_shared<CommandClusterMeet>());
    regCmd(proto::types::P_MEET_PEER, std::make_shared<CommandClusterMeetPeer>());
    regCmd(proto::types::P_PONG, std::make_shared<CommandClusterPong>());
    regCmd(proto::types::P_FAIL, std::make_shared<CommandClusterFail>());
    regCmd(proto::types::P_SET_EPOCH, std::make_shared<CommandClusterSetEpoch>());
    regCmd(proto::types::P_SET_DB, std::make_shared<CommandClusterSetDB>());
    regCmd(proto::types::P_STATE, std::make_shared<CommandClusterState>());

    // client
    regCmd(proto::types::C_SET, std::make_shared<CommandClientSet>());
    regCmd(proto::types::C_GET, std::make_shared<CommandClientGet>());
    regCmd(proto::types::C_MGET, std::make_shared<CommandClientMGet>());
    // replica
    regCmd(proto::types::R_PING, std::make_shared<CommandReplicaPing>());
    regCmd(proto::types::R_PONG, std::make_shared<CommandReplicaPong>());
    regCmd(proto::types::R_HEARTBEAT, std::make_shared<CommandReplicaHeartbeat>());
    regCmd(proto::types::R_DELTA_REQUEST, std::make_shared<CommandReplicaDeltaPull>());
    regCmd(proto::types::R_DELTA_RESPONSE, std::make_shared<CommandReplicaDeltaRecv>());
    regCmd(proto::types::R_BULK, std::make_shared<CommandReplicaRecvBulk>());
    regCmd(proto::types::R_SYNC_REQUEST, std::make_shared<CommandReplicaSyncRequest>());
    regCmd(proto::types::R_SYNC_RESPONSE, std::make_shared<CommandReplicaSyncResponse>());
}

void chakra::cmds::CommandPool::regCmd(proto::types::Type type, std::shared_ptr<Command> cmd) { cmds.emplace(type, cmd); }

std::shared_ptr<chakra::cmds::Command> chakra::cmds::CommandPool::fetch(proto::types::Type type) {
    auto it = cmds.find(type);
    return it != cmds.end() ? it->second : cmdnf;
}

std::shared_ptr<chakra::cmds::CommandPool> chakra::cmds::CommandPool::get() {
    static std::shared_ptr<CommandPool> cmdsptr = std::make_shared<CommandPool>();
    return cmdsptr;
}