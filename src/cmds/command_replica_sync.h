//
// Created by 王振奎 on 2021/7/11.
//

#ifndef CHAKRA_COMMAND_REPLICA_SYNC_H
#define CHAKRA_COMMAND_REPLICA_SYNC_H

#include "command.h"
#include "replica.pb.h"
#include "utils/error.h"
#include "replica/replica_link.h"

namespace chakra::cmds{
class CommandReplicaSync : public Command{
public:
    void execute(char *req, size_t reqLen, void *data, std::function<void(char *resp, size_t respLen)> reply) override;
    static utils::Error startBulk(chakra::replica::Link* link, proto::replica::SyncMessageRequest& request);
    ~CommandReplicaSync() override = default;
};
}




#endif //CHAKRA_COMMAND_REPLICA_SYNC_H
