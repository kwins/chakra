//
// Created by kwins on 2021/7/16.
//

#ifndef CHAKRA_COMMAND_CLUSTER_SET_EPOCH_H
#define CHAKRA_COMMAND_CLUSTER_SET_EPOCH_H
#include "command.h"
namespace chakra::cmds{
class CommandClusterSetEpoch : public Command{
public:
    void execute(char *req, size_t reqLen, void *data, std::function<error::Error(char *, size_t)> cbf) override;

    ~CommandClusterSetEpoch() override = default;
};
}



#endif //CHAKRA_COMMAND_CLUSTER_SET_EPOCH_H
