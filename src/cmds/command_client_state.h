//
// Created by kwins on 2021/9/18.
//

#ifndef CHAKRA_COMMAND_CLIENT_STATE_H
#define CHAKRA_COMMAND_CLIENT_STATE_H


#include "command.h"
namespace chakra::cmds {
class CommandClientState : public Command {
public:
    void execute(char *req, size_t reqLen, void *data) override;
    ~CommandClientState() override = default;
};
}


#endif //CHAKRA_COMMAND_CLIENT_STATE_H
