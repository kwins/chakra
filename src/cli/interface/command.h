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

    std::vector<std::string> stringSplit(const std::string& str, char delim) {
        std::vector<std::string> elems;
        auto lastPos = str.find_first_not_of(delim, 0);
        auto pos = str.find_first_of(delim, lastPos);
        while (pos != std::string::npos || lastPos != std::string::npos) {
            elems.push_back(str.substr(lastPos, pos - lastPos));
            lastPos = str.find_first_not_of(delim, pos);
            pos = str.find_first_of(delim, lastPos);
        }
        return elems;
    }

    std::string& trim(std::string& str) {
        str.erase(0, str.find_first_not_of(" "));
        str.erase(str.find_last_not_of(" ") + 1);
        return str;
    }
    virtual ~Command() = default;
};

}

#endif // CHAKRA_CLI_COMMAND_H