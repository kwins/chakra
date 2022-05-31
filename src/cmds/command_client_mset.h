#ifndef CHAKRA_COMMAND_CLIENT_MSET_H
#define CHAKRA_COMMAND_CLIENT_MSET_H

#include "command.h"
namespace chakra::cmds {
class CommandClientMSet : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClientMSet() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_MSET_H
