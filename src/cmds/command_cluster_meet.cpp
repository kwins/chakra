//
// Created by kwins on 2021/5/28.
//

#include "command_cluster_meet.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "types.pb.h"
#include "cluster/view.h"
#include <glog/logging.h>

void chakra::cmds::CommandClusterMeet::execute(char *req, size_t len, void* data, std::function<void(char *, size_t)> cbf) {
    proto::peer::MeetMessageRequest meet;
    proto::peer::MeetMessageResponse reply;
    if (!chakra::net::Packet::deSerialize(req, len, meet)){
        chakra::net::Packet::fillError(*reply.mutable_error(), 1, "Message Parse Error");
        chakra::net::Packet::serialize(reply, proto::types::Type::P_MEET, cbf);
        return;
    }

    if (meet.ip().empty() || meet.port() <= 0){
        chakra::net::Packet::fillError(*reply.mutable_error(), 1, "Ip empty Or Bad Port");
    } else{
        chakra::net::Packet::fillError(*reply.mutable_error(), 0, "ok");
        chakra::cluster::View::get()->addPeer(meet.ip(), meet.port());
    }
    chakra::net::Packet::serialize(reply, proto::types::Type::P_MEET, cbf);
}

