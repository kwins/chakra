//
// Created by kwins on 2021/5/27.
//

#ifndef CHAKRA_COMMAND_NF_H
#define CHAKRA_COMMAND_NF_H
#include "command.h"
namespace chakra::cmds{

class CommandNF : public Command {
public:
    void execute(char *req, size_t len,void* data, std::function<utils::Error(char *, size_t)> cbf) override;
    ~CommandNF() override = default;
};

}



#endif //CHAKRA_COMMAND_NF_H
