//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_COMMAND_H
#define CHAKRA_COMMAND_H

#include <sys/types.h>
#include <functional>
#include <glog/logging.h>
#include "error/err.h"
#include "types.pb.h"

namespace chakra::cmds {

class Command {
public:
    virtual void execute(char* req, size_t reqLen, void* data) = 0;
    void fillError(proto::types::Error& error, int32_t code, const std::string& errmsg);
    void fillError(proto::types::Error* error, int32_t code, const std::string& errmsg);
    virtual ~Command() = default;
};

}

#endif //CHAKRA_COMMAND_H
