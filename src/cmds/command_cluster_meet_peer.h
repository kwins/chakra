//
// Created by kwins on 2021/6/2.
//

#ifndef CHAKRA_COMMAND_CLUSTER_MEET_PEER_H
#define CHAKRA_COMMAND_CLUSTER_MEET_PEER_H
#include "command.h"

namespace chakra::cmds{
class CommandClusterMeetPeer : public Command{
public:
    void execute(char *req, size_t reqLen, void* data, std::function<error::Error(char *, size_t)> cbf) override;
    ~CommandClusterMeetPeer() override = default;
};

}



#endif //CHAKRA_COMMAND_CLUSTER_MEET_PEER_H
