//
// Created by kwins on 2021/5/28.
//

#include "command_cluster_meet.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "types.pb.h"
#include "cluster/cluster.h"
#include <glog/logging.h>

void chakra::cmds::CommandClusterMeet::execute(char *req, size_t len, void* data) {
    auto link = static_cast<chakra::cluster::Peer::Link*>(data);
    proto::peer::MeetMessageRequest meet;
    proto::peer::MeetMessageResponse meetMessageResponse;
    auto clsptr = chakra::cluster::Cluster::get();
    auto err = chakra::net::Packet::deSerialize(req, len, meet, proto::types::P_MEET);
    if (err) {
        chakra::net::Packet::fillError(meetMessageResponse.mutable_error(), 1, err.what());
    } else if (meet.ip().empty() || meet.port() <= 0) {
        chakra::net::Packet::fillError(meetMessageResponse.mutable_error(), 1, "ip or port empty");
    } else {
        DLOG(INFO) << "[cluster] meet message request: " << meet.DebugString();
        auto peer = clsptr->getPeer(meet.ip(), meet.port());
        if (peer) {
            chakra::net::Packet::fillError(meetMessageResponse.mutable_error(), 1, "peer has exist in cluster");
        } else {
            clsptr->addPeer(meet.ip(), meet.port());
        }
    }
    link->asyncSendMsg(meetMessageResponse, proto::types::Type::P_MEET);
}

