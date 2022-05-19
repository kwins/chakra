#ifndef CHAKRA_CLI_H
#define CHAKRA_CLI_H

#include <command_setdb.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "client/cplusplus/chakra_cluster.h"
#include "command.h"

namespace chakra::cli {

class Cli {
public:
    Cli();
    void execute();
private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
    static std::unordered_map<std::string, std::shared_ptr<Command>> cmds;
};

}

#endif // CHAKRA_CLI_H