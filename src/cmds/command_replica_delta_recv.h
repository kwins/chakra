//
// Created by kwins on 2021/9/27.
//

#ifndef CHAKRA_COMMAND_REPLICA_DELTA_RECV_H
#define CHAKRA_COMMAND_REPLICA_DELTA_RECV_H

#include "command.h"

namespace chakra::cmds{
class CommandReplicaDeltaRecv : public Command {
public:
    void execute(char *req, size_t reqLen, void *data, std::function<error::Error(char *resp, size_t respLen)> cbf) override;
    ~CommandReplicaDeltaRecv() override = default;
};
}


#endif //CHAKRA_COMMAND_REPLICA_DELTA_RECV_H
