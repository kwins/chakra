//
// Created by kwins on 2021/9/26.
//

#ifndef CHAKRA_COMMAND_REPLICA_SYNC_RESPONSE_H
#define CHAKRA_COMMAND_REPLICA_SYNC_RESPONSE_H


#include "command.h"

namespace chakra::cmds {
class CommandReplicaSyncResponse : public Command {
public:
    void execute(char *req, size_t len, void *data,std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandReplicaSyncResponse() override = default;
};
}



#endif //CHAKRA_COMMAND_REPLICA_SYNC_RESPONSE_H
