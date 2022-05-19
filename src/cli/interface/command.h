#ifndef CHAKRA_CLI_COMMAND_H
#define CHAKRA_CLI_COMMAND_H

#include <memory>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "client/cplusplus/chakra_cluster.h"

namespace chakra::cli {

class Command {
public:
    virtual void execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) = 0;
    virtual ~Command() = default;
};

}

#endif // CHAKRA_CLI_COMMAND_H