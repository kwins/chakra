#ifndef CHAKRA_COMMAND_CLIENT_SCAN_H
#define CHAKRA_COMMAND_CLIENT_SCAN_H

#include "command.h"
namespace chakra::cmds {
class CommandClientScan : public Command {
public:
    void execute(char *req, size_t len, void *data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandClientScan() override = default;
};

}


#endif //CHAKRA_COMMAND_CLIENT_SCAN_H
