//
// Created by kwins on 2021/6/30.
//

#ifndef CHAKRA_COMMAND_REPLICA_OF_H
#define CHAKRA_COMMAND_REPLICA_OF_H

#include "command.h"

namespace chakra::cmds{
class CommandReplicaOf : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<utils::Error(char *, size_t)> reply) override;
    ~CommandReplicaOf() override = default;
};
}



#endif //CHAKRA_COMMAND_REPLICA_OF_H
