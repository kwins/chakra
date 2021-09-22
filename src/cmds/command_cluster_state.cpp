//
// Created by kwins on 2021/9/18.
//

#include "command_cluster_state.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "cluster/cluster.h"

void chakra::cmds::CommandClusterState::execute(char *req, size_t reqLen, void *data,
                                                std::function<utils::Error(char *, size_t)> cbf) {
    proto::peer::StateMessageRequest stateMessageRequest;
    proto::peer::StateMessageResponse stateMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, stateMessageRequest, proto::types::P_STATE);
    if (!err.success()){
        chakra::net::Packet::fillError(stateMessageResponse.mutable_error(), err.getCode(), err.getMsg());
    } else {
        auto clsptr = chakra::cluster::Cluster::get();
        clsptr->stateDesc(*stateMessageResponse.mutable_state());
    }
    chakra::net::Packet::serialize(stateMessageResponse, proto::types::P_STATE, cbf);
}
