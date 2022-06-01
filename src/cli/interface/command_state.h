#ifndef CHAKRA_CLI_COMMAND_STATE_H
#define CHAKRA_CLI_COMMAND_STATE_H

#include "command.h"

namespace chakra::cli {

class CommandState : public Command {
public:
    void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) override;
    ~CommandState() override = default;
};

}
#endif // CHAKRA_CLI_COMMAND_STATE_H