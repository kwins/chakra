#ifndef CHAKRA_COMMAND_CLIENT_PUSH_H
#define CHAKRA_COMMAND_CLIENT_PUSH_H

#include "command.h"
namespace chakra::cmds {
class CommandClientPush : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandClientPush() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_PUSH_H
