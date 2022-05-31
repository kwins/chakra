#ifndef CHAKRA_COMMAND_CLIENT_PUSH_H
#define CHAKRA_COMMAND_CLIENT_PUSH_H

#include "command.h"
namespace chakra::cmds {
class CommandClientPush : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClientPush() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_PUSH_H
