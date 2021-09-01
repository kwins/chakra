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
#include "command_cluster_update_db.h"

#include "command_client_set.h"

#include "command_replica_of.h"
#include "command_replica_ping.h"
#include "command_replica_pull.h"
#include "command_replica_sync.h"

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

    // client
    regCmd(proto::types::C_SET, std::make_shared<CommandClientSet>());

    // replica
    regCmd(proto::types::R_REPLICA_OF, std::make_shared<CommandReplicaOf>());
    regCmd(proto::types::R_PING, std::make_shared<CommandReplicaPing>());
    regCmd(proto::types::R_PULL, std::make_shared<CommandReplicaPull>());
    regCmd(proto::types::R_PSYNC, std::make_shared<CommandReplicaSync>());
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