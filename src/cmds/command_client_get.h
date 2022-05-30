//
// Created by kwins on 2021/9/2.
//

#ifndef CHAKRA_COMMAND_CLIENT_GET_H
#define CHAKRA_COMMAND_CLIENT_GET_H


#include "command.h"
namespace chakra::cmds {
class CommandClientGet : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClientGet() override = default;
};

}

#endif //CHAKRA_COMMAND_CLIENT_GET_H
