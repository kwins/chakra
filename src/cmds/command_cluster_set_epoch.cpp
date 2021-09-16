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

    proto::peer::EpochMessage epochMessage;
    if (!chakra::net::Packet::deSerialize(req, reqLen, epochMessage, proto::types::P_SET_EPOCH).success()) return;

    proto::types::Error reply;
    auto myself = cluster::Cluster::get()->getMyself();
    if (epochMessage.epoch() < 0){
        chakra::net::Packet::fillError(reply, 1, "Invalid config epoch specified:" + std::to_string(epochMessage.epoch()));
    } else if (cluster::Cluster::get()->size() > 1){
        chakra::net::Packet::fillError(reply, 1, "The user can assign a config epoch only when the peer does not know any other node.");
    } else if (myself->getEpoch() != 0){
        chakra::net::Packet::fillError(reply, 1, "Peer config epoch is already non-zero");
    } else {
        myself->setEpoch(epochMessage.epoch());
    }

    chakra::net::Packet::serialize(reply, proto::types::P_SET_EPOCH, cbf);
}
