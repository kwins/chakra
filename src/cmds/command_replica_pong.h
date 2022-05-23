//
// Created by kwins on 2021/9/26.
//

#ifndef CHAKRA_COMMAND_REPLICA_PONG_H
#define CHAKRA_COMMAND_REPLICA_PONG_H


#include "command.h"

namespace chakra::cmds {
class CommandReplicaPong : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandReplicaPong() override = default;
};
}

#endif //CHAKRA_COMMAND_REPLICA_PONG_H
