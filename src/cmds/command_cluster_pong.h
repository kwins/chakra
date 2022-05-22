//
// Created by kwins on 2021/6/2.
//

#ifndef CHAKRA_COMMANDPONG_H
#define CHAKRA_COMMANDPONG_H
#include "command.h"

namespace chakra::cmds {
class CommandClusterPong : public Command {
public:
    void execute(char *req, size_t len, void* data, std::function<error::Error(char *, size_t)> reply) override;
    ~CommandClusterPong() override = default;
};

}



#endif //CHAKRA_COMMANDPONG_H
