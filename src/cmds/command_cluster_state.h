//
// Created by kwins on 2021/9/18.
//

#ifndef CHAKRA_COMMAND_CLUSTER_STATE_H
#define CHAKRA_COMMAND_CLUSTER_STATE_H


#include "command.h"
namespace chakra::cmds{
class CommandClusterState : public Command{
public:
    void execute(char *req, size_t reqLen, void *data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandClusterState() override = default;
};
}


#endif //CHAKRA_COMMAND_CLUSTER_STATE_H
