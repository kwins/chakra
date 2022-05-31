//
// Created by kwins on 2021/6/3.
//

#ifndef CHAKRA_COMMAND_CLUSTER_FAIL_H
#define CHAKRA_COMMAND_CLUSTER_FAIL_H
#include "command.h"
namespace chakra::cmds {
class CommandClusterFail : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClusterFail() override = default;
};

}


#endif //CHAKRA_COMMAND_CLUSTER_FAIL_H
