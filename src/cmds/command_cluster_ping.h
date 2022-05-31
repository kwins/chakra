//
// Created by kwins on 2021/6/2.
//

#ifndef CHAKRA_COMMAND_CLUSTER_PING_H
#define CHAKRA_COMMAND_CLUSTER_PING_H
#include "command.h"

namespace chakra::cmds {

class CommandClusterPing : public Command {
public:
    void execute(char *req, size_t len, void* data) override;
    ~CommandClusterPing() override = default;
};

}



#endif //CHAKRA_COMMAND_CLUSTER_PING_H
