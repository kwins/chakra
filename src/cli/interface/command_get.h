#ifndef CHAKRA_CLI_COMMAND_GET_H
#define CHAKRA_CLI_COMMAND_GET_H

#include "command.h"

namespace chakra::cli {

class CommandGet : public Command {
public:
    void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) override;
    ~CommandGet() override = default;
};

}
#endif // CHAKRA_CLI_COMMAND_GET_H