#ifndef CHAKRA_COMMAND_CLIENT_MGET_H
#define CHAKRA_COMMAND_CLIENT_MGET_H


#include "command.h"
namespace chakra::cmds {
class CommandClientMGet : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClientMGet() override = default;
};

}

#endif //CHAKRA_COMMAND_CLIENT_MGET_H
