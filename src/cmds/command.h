//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_COMMAND_H
#define CHAKRA_COMMAND_H

#include <sys/types.h>
#include <functional>
#include <glog/logging.h>
#include "utils/error.h"

namespace chakra::cmds{

class Command {
public:
    virtual void execute(char* req, size_t reqLen, void* data, std::function<utils::Error(char* resp, size_t respLen)> cbf){};
    virtual ~Command() = default;
};

}

#endif //CHAKRA_COMMAND_H
