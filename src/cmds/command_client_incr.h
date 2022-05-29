#ifndef CHAKRA_COMMAND_CLIENT_INCR_H
#define CHAKRA_COMMAND_CLIENT_INCR_H

#include "command.h"
namespace chakra::cmds {
class CommandClientIncr : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandClientIncr() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_INCR_H