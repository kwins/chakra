//
// Created by kwins on 2021/7/16.
//

#include "command_cluster_set_epoch.h"
#include "net/packet.h"
#include "peer.pb.h"
#include "types.pb.h"
#include "cluster/cluster.h"

void chakra::cmds::CommandClusterSetEpoch::execute(char *req, size_t reqLen, void *data) {
    auto link = static_cast<chakra::cluster::Peer::Link*>(data);
    proto::peer::EpochSetMessageRequest epochSetMessageRequest;
    proto::peer::EpochSetMessageResponse epochSetMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, epochSetMessageRequest, proto::types::P_SET_EPOCH);
    if (err) {
        fillError(epochSetMessageResponse.mutable_error(), 1, err.what());
    } else {
        auto clsptr = cluster::Cluster::get();
        if (epochSetMessageRequest.increasing()) {
            clsptr->increasingMyselfEpoch();
        } else {
            if (epochSetMessageRequest.epoch() < 0) {
                fillError(epochSetMessageResponse.mutable_error(), 1, "invalid epoch specified");
            } else if (cluster::Cluster::get()->size() > 1 && !epochSetMessageRequest.increasing()) {
                fillError(epochSetMessageResponse.mutable_error(), 1, "the user can assign a config epoch only when the peer does not know any other node.");
            } else if (clsptr->getMyself()->getEpoch() != 0) {
                fillError(epochSetMessageResponse.mutable_error(), 1, "peer config epoch is already non-zero");
            } else {
                clsptr->getMyself()->setEpoch(epochSetMessageRequest.epoch());
                clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
            }
        }
    }
    link->asyncSendMsg(epochSetMessageResponse, proto::types::P_SET_EPOCH);
}
