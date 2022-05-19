#ifndef CHAKRA_CLI_COMMAND_MEET_H
#define CHAKRA_CLI_COMMAND_MEET_H

#include "command.h"

namespace chakra::cli {

class CommandMeet : public Command {
public:
    void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) override;
    ~CommandMeet() override = default;
};

}
#endif // CHAKRA_CLI_COMMAND_MEET_H