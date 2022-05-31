#ifndef CHAKRA_COMMAND_CLIENT_SCAN_H
#define CHAKRA_COMMAND_CLIENT_SCAN_H

#include "command.h"
namespace chakra::cmds {
class CommandClientScan : public Command {
public:
    void execute(char *req, size_t len, void *data) override;
    ~CommandClientScan() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_SCAN_H
