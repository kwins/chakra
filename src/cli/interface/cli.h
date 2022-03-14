#ifndef CHAKRA_CLI_H
#define CHAKRA_CLI_H

#include <string>
#include <glog/logging.h>
#include "client/cplusplus/chakra_cluster.h"


namespace chakra::cli {

class Cli {
public:
    Cli();
    void execute();
private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};

}

#endif // CHAKRA_CLI_H