//
// Created by kwins on 2021/9/24.
//

#include "command_replica_heartbeat.h"
#include "replica/replica.h"
#include "utils/basic.h"

void chakra::cmds::CommandReplicaHeartbeat::execute(char *req, size_t len, void *data) {
    auto link = static_cast<replica::Replicate::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());
}
