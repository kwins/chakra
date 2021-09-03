//
// Created by kwins on 2021/5/28.
//

#include "command_cluster_meet.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "types.pb.h"
#include "cluster/view.h"
#include <glog/logging.h>

void chakra::cmds::CommandClusterMeet::execute(char *req, size_t len, void* data, std::function<utils::Error(char *, size_t)> cbf) {
    proto::peer::MeetMessageRequest meet;
    proto::peer::MeetMessageResponse reply;
    auto err = chakra::net::Packet::deSerialize(req, len, meet, proto::types::P_MEET);
    if (!err.success()){
        chakra::net::Packet::fillError(*reply.mutable_error(), err.getCode(), err.getMsg());
        chakra::net::Packet::serialize(reply, proto::types::Type::P_MEET, cbf);
        return;
    }

    if (meet.ip().empty() || meet.port() <= 0){
        chakra::net::Packet::fillError(*reply.mutable_error(), 1, "Ip empty Or Bad Port");
    } else{
        chakra::cluster::View::get()->addPeer(meet.ip(), meet.port());
    }
    chakra::net::Packet::serialize(reply, proto::types::Type::P_MEET, cbf);
}

