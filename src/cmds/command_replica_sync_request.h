//
// Created by 王振奎 on 2021/7/11.
//

#ifndef CHAKRA_COMMAND_REPLICA_SYNC_REQUEST_H
#define CHAKRA_COMMAND_REPLICA_SYNC_REQUEST_H

#include "command.h"
#include "replica.pb.h"
#include "utils/error.h"
#include "replica/replica_link.h"

namespace chakra::cmds{
class CommandReplicaSyncRequest : public Command{
public:
    void execute(char *req, size_t reqLen, void *data, std::function<utils::Error(char *resp, size_t respLen)> cbf) override;
    ~CommandReplicaSyncRequest() override = default;
};
}




#endif //CHAKRA_COMMAND_REPLICA_SYNC_REQUEST_H
