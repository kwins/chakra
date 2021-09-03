//
// Created by kwins on 2021/7/16.
//

#ifndef CHAKRA_COMMAND_CLUSTER_SET_DB_H
#define CHAKRA_COMMAND_CLUSTER_SET_DB_H

#include "command.h"

namespace chakra::cmds{
class CommandClusterSetDB : public Command {
public:
    void execute(char *req, size_t reqLen, void *data, std::function<utils::Error(char *, size_t)> cbf) override;

    ~CommandClusterSetDB() override = default;
};
}



#endif //CHAKRA_COMMAND_CLUSTER_SET_DB_H
