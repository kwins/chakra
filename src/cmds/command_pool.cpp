//
// Created by kwins on 2021/5/27.
//

#include "command_pool.h"
#include <memory>
#include <types.pb.h>

#include "command_cluster_ping.h"
#include "command_cluster_meet.h"
#include "command_cluster_pong.h"
#include "command_cluster_meet_peer.h"
#include "command_cluster_fail.h"
#include "command_cluster_set_db.h"
#include "command_cluster_set_epoch.h"
#include "command_cluster_state.h"
#include "command_replica_heartbeat.h"
#include "command_replica_delta_pull.h"
#include "command_replica_delta_recv.h"
#include "command_replica_ping.h"
#include "command_replica_pong.h"
#include "command_replica_sync_request.h"
#include "command_replica_sync_response.h"
#include "command_replica_recv_bulk.h"

#include "command_client_set.h"
#include "command_client_mset.h"
#include "command_client_get.h"
#include "command_client_mget.h"
#include "command_client_push.h"
#include "command_client_mpush.h"
#include "command_client_scan.h"
#include "command_client_incr.h"
#include "command_client_mincr.h"

chakra::cmds::CommandPool::CommandPool() {
    cmdnf = std::make_shared<CommandNF>();
    cmds = {
        // cluster
        { proto::types::P_PING,       std::make_shared<CommandClusterPing>() },
        { proto::types::P_MEET,       std::make_shared<CommandClusterMeet>() },
        { proto::types::P_MEET_PEER,  std::make_shared<CommandClusterMeetPeer>() },
        { proto::types::P_PONG,       std::make_shared<CommandClusterPong>() },
        { proto::types::P_FAIL,       std::make_shared<CommandClusterFail>() },
        { proto::types::P_SET_EPOCH,  std::make_shared<CommandClusterSetEpoch>() },
        { proto::types::P_SET_DB,     std::make_shared<CommandClusterSetDB>() },
        { proto::types::P_STATE,      std::make_shared<CommandClusterState>() },

        // client
        { proto::types::C_SET,          std::make_shared<CommandClientSet>() },
        { proto::types::C_MSET,         std::make_shared<CommandClientMSet>() },
        { proto::types::C_GET,          std::make_shared<CommandClientGet>() },
        { proto::types::C_MGET,         std::make_shared<CommandClientMGet>() },
        { proto::types::C_PUSH,         std::make_shared<CommandClientPush>() },
        { proto::types::C_MPUSH,        std::make_shared<CommandClientMPush>() },
        { proto::types::C_SCAN,         std::make_shared<CommandClientScan>() },
        { proto::types::C_INCR,         std::make_shared<CommandClientIncr>() },
        { proto::types::C_MINCR,         std::make_shared<CommandClientMIncr>() },
        
        // replica
        { proto::types::R_PING,           std::make_shared<CommandReplicaPing>() },
        { proto::types::R_PONG,           std::make_shared<CommandReplicaPong>() },
        { proto::types::R_HEARTBEAT,      std::make_shared<CommandReplicaHeartbeat>() },
        { proto::types::R_DELTA_REQUEST,  std::make_shared<CommandReplicaDeltaPull>() },
        { proto::types::R_DELTA_RESPONSE, std::make_shared<CommandReplicaDeltaRecv>() },
        { proto::types::R_BULK,           std::make_shared<CommandReplicaRecvBulk>() },
        { proto::types::R_SYNC_REQUEST,   std::make_shared<CommandReplicaSyncRequest>() },
        { proto::types::R_SYNC_RESPONSE,  std::make_shared<CommandReplicaSyncResponse>() },
    };
}

std::shared_ptr<chakra::cmds::Command> chakra::cmds::CommandPool::fetch(proto::types::Type type) {
    auto it = cmds.find(type);
    return it != cmds.end() ? it->second : cmdnf;
}

std::shared_ptr<chakra::cmds::CommandPool> chakra::cmds::CommandPool::get() {
    static std::shared_ptr<CommandPool> cmdsptr = std::make_shared<CommandPool>();
    return cmdsptr;
}