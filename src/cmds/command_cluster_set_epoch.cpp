//
// Created by kwins on 2021/7/16.
//

#include "command_cluster_set_epoch.h"
#include "net/packet.h"
#include "peer.pb.h"
#include "types.pb.h"
#include "cluster/cluster.h"

void chakra::cmds::CommandClusterSetEpoch::execute(char *req, size_t reqLen, void *data,
                                                   std::function<utils::Error(char *, size_t)> cbf) {

    proto::peer::EpochSetMessageRequest epochSetMessageRequest;
    proto::peer::EpochSetMessageResponse epochSetMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, epochSetMessageRequest, proto::types::P_SET_EPOCH);
    if (!err.success()){
        chakra::net::Packet::fillError(epochSetMessageResponse.mutable_error(), err.getCode(), err.getMsg());
    } else {
        auto myself = cluster::Cluster::get()->getMyself();
        if (epochSetMessageRequest.epoch() < 0){
            chakra::net::Packet::fillError(epochSetMessageResponse.mutable_error(), 1, "Invalid config epoch specified:" + std::to_string(epochSetMessageRequest.epoch()));
        } else if (cluster::Cluster::get()->size() > 1){
            chakra::net::Packet::fillError(epochSetMessageResponse.mutable_error(), 1, "The user can assign a config epoch only when the peer does not know any other node.");
        } else if (myself->getEpoch() != 0){
            chakra::net::Packet::fillError(epochSetMessageResponse.mutable_error(), 1, "Peer config epoch is already non-zero");
        } else {
            myself->setEpoch(epochSetMessageRequest.epoch());
        }
    }
    chakra::net::Packet::serialize(epochSetMessageResponse, proto::types::P_SET_EPOCH, cbf);
}
