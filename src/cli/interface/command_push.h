#ifndef CHAKRA_CLI_COMMAND_PUSH_H
#define CHAKRA_CLI_COMMAND_PUSH_H

#include "command.h"

namespace chakra::cli {

class CommandPush : public Command {
public:
    void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) override;
    ~CommandPush() override = default;
};

}
#endif // CHAKRA_CLI_COMMAND_PUSH_H