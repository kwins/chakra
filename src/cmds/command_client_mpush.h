#ifndef CHAKRA_COMMAND_CLIENT_MPUSH_H
#define CHAKRA_COMMAND_CLIENT_MPUSH_H

#include "command.h"
namespace chakra::cmds {
class CommandClientMPush : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandClientMPush() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_MPUSH_H
