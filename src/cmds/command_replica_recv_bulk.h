//
// Created by kwins on 2021/9/27.
//

#ifndef CHAKRA_COMMAND_REPLICA_RECV_BULK_H
#define CHAKRA_COMMAND_REPLICA_RECV_BULK_H

#include "command.h"

namespace chakra::cmds {
class CommandReplicaRecvBulk : public Command {
public:
    void execute(char *req, size_t reqLen, void *data) override;
    ~CommandReplicaRecvBulk() override = default;
};
}



#endif //CHAKRA_COMMAND_REPLICA_RECV_BULK_H
