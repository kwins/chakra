#ifndef CHAKRA_CLI_COMMAND_SET_H
#define CHAKRA_CLI_COMMAND_SET_H

#include "command.h"

namespace chakra::cli {

class CommandSet : public Command {
public:
    void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) override;
    ~CommandSet() override = default;
};

}
#endif // CHAKRA_CLI_COMMAND_SET_H