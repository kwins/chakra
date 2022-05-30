#ifndef CHAKRA_COMMAND_CLIENT_MINCR_H
#define CHAKRA_COMMAND_CLIENT_MINCR_H

#include "command.h"
namespace chakra::cmds {
class CommandClientMIncr : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClientMIncr() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_MINCR_H
