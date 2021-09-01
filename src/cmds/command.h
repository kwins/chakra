//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_COMMAND_H
#define CHAKRA_COMMAND_H

#include <sys/types.h>
#include <functional>
#include <glog/logging.h>

namespace chakra::cmds{

class Command {
public:
    virtual void execute(char* req, size_t reqLen, void* data, std::function<void(char* resp, size_t respLen)> cbf) = 0;
    virtual ~Command() = default;
};

}

#endif //CHAKRA_COMMAND_H
