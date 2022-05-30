#ifndef CHAKRA_COMMAND_CLIENT_INCR_H
#define CHAKRA_COMMAND_CLIENT_INCR_H

#include "command.h"
namespace chakra::cmds {
class CommandClientIncr : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClientIncr() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_INCR_H
