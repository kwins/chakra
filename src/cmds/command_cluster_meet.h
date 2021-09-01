//
// Created by kwins on 2021/5/28.
//

#ifndef CHAKRA_COMMAND_CLUSTER_MEET_H
#define CHAKRA_COMMAND_CLUSTER_MEET_H
#include "command.h"

namespace chakra::cmds{

class CommandClusterMeet : public Command{
public:
    void execute(char *req, size_t len,void* data, std::function<void(char *, size_t)> reply) override;
    ~CommandClusterMeet() override = default;
};

}



#endif //CHAKRA_COMMAND_CLUSTER_MEET_H
