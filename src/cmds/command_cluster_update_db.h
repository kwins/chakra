//
// Created by kwins on 2021/7/14.
//

#ifndef CHAKRA_COMMAND_CLUSTER_UPDATE_DB_H
#define CHAKRA_COMMAND_CLUSTER_UPDATE_DB_H

#include "command.h"

namespace chakra::cmds{
class CommandPeerUpdateDB : public Command {
public:
    void execute(char *req, size_t reqLen, void *data, std::function<void(char *, size_t)> cbf) override;
    ~CommandPeerUpdateDB() override = default;
};
}



#endif //CHAKRA_COMMAND_CLUSTER_UPDATE_DB_H
