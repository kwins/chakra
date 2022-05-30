//
// Created by kwins on 2021/7/2.
//

#ifndef CHAKRA_COMMAND_REPLICA_PING_H
#define CHAKRA_COMMAND_REPLICA_PING_H

#include "command.h"

namespace chakra::cmds {
class CommandReplicaPing : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandReplicaPing() override = default;
};
}



#endif //CHAKRA_COMMAND_REPLICA_PING_H
