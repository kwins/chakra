//
// Created by kwins on 2021/5/27.
//

#ifndef CHAKRA_COMMAND_POOL_H
#define CHAKRA_COMMAND_POOL_H
#include "utils/nocopy.h"
#include "types.pb.h"
#include "command.h"
#include "command_nf.h"

namespace chakra::cmds{

class CommandPool : public utils::UnCopyable {
public:
    CommandPool();
    static std::shared_ptr<CommandPool> get();
    void regCmd(proto::types::Type type, std::shared_ptr<Command> cmd);
    std::shared_ptr<Command> fetch(proto::types::Type type);

private:
    std::shared_ptr<CommandNF> cmdnf;
    std::unordered_map<proto::types::Type, std::shared_ptr<Command>> cmds{};
};

}



#endif //CHAKRA_COMMAND_POOL_H
