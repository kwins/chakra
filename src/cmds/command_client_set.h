//
// Created by kwins on 2021/8/27.
//

#ifndef CHAKRA_COMMAND_CLIENT_SET_H
#define CHAKRA_COMMAND_CLIENT_SET_H

#include "command.h"
namespace chakra::cmds{
class CommandClientSet : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<void(char *, size_t)> reply) override;
    ~CommandClientSet() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_SET_H
