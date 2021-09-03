//
// Created by 王振奎 on 2021/7/10.
//

#ifndef CHAKRA_COMMAND_REPLICA_PULL_H
#define CHAKRA_COMMAND_REPLICA_PULL_H

#include "command.h"

namespace chakra::cmds{
class CommandReplicaPull : public Command {
public:
    void execute(char *req, size_t reqLen, void *data, std::function<utils::Error(char *resp, size_t respLen)> reply) override;
    ~CommandReplicaPull() override = default;
};
}



#endif //CHAKRA_COMMAND_REPLICA_PULL_H
