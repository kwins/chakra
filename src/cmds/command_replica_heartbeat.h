//
// Created by kwins on 2021/9/24.
//

#ifndef CHAKRA_COMMAND_REPLICA_HEARTBEAT_H
#define CHAKRA_COMMAND_REPLICA_HEARTBEAT_H



#include "command.h"

namespace chakra::cmds {
class CommandReplicaHeartbeat : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandReplicaHeartbeat() override = default;
};
}


#endif //CHAKRA_COMMAND_REPLICA_HEARTBEAT_H
