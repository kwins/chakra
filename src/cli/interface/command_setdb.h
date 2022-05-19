#ifndef CHAKRA_CLI_COMMAND_SETDB_H
#define CHAKRA_CLI_COMMAND_SETDB_H

#include "command.h"

namespace chakra::cli {

class CommandSetDB : public Command {
public:
    void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) override;
    ~CommandSetDB() override = default;
};

}
#endif // CHAKRA_CLI_COMMAND_SETDB_H