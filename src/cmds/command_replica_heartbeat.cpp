//
// Created by kwins on 2021/9/24.
//

#include "command_replica_heartbeat.h"
#include "replica/replica_link.h"
#include "utils/basic.h"

void chakra::cmds::CommandReplicaHeartbeat::execute(char *req, size_t len, void *data,
                                                    std::function<utils::Error(char *, size_t)> cbf) {
    auto link = static_cast<replica::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());
}
