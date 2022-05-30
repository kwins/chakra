//
// Created by 王振奎 on 2021/7/11.
//

#ifndef CHAKRA_COMMAND_REPLICA_SYNC_REQUEST_H
#define CHAKRA_COMMAND_REPLICA_SYNC_REQUEST_H

#include "command.h"
#include "replica.pb.h"
#include "replica/replica.h"

namespace chakra::cmds {
class CommandReplicaSyncRequest : public Command {
public:
    void execute(char *req, size_t reqLen, void *data) override;
    void fullSync(chakra::replica::Replicate::Link* link, proto::replica::SyncMessageRequest& request, proto::replica::SyncMessageResponse& response);
    ~CommandReplicaSyncRequest() override = default;
};
}




#endif //CHAKRA_COMMAND_REPLICA_SYNC_REQUEST_H
