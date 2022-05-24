#ifndef CHAKRA_COMMAND_CLIENT_MSET_H
#define CHAKRA_COMMAND_CLIENT_MSET_H

#include "command.h"
namespace chakra::cmds {
class CommandClientMSet : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandClientMSet() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_MSET_H
