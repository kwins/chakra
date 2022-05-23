#ifndef CHAKRA_CLI_COMMAND_MGET_H
#define CHAKRA_CLI_COMMAND_MGET_H

#include "command.h"

namespace chakra::cli {

class CommandMGet : public Command {
public:
    void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) override;
    ~CommandMGet() override = default;
};

}
#endif // CHAKRA_CLI_COMMAND_MGET_H