//
// Created by kwins on 2021/7/2.
//

#include "command_replica_ping.h"
#include <replica/replica.h>
#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "cluster/cluster.h"

void chakra::cmds::CommandReplicaPing::execute(char *req, size_t len, void *data,
                                               std::function<error::Error(char *, size_t)> cbf) {

    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());
    auto clsptr = cluster::Cluster::get();

    proto::replica::PongMessage pongMessage;
    proto::replica::PingMessage pingMessage;
    auto err = chakra::net::Packet::deSerialize(req, len, pingMessage, proto::types::R_PING);
    if (!err.success()){
        chakra::net::Packet::fillError(pongMessage.mutable_error(), err.getCode(), err.getMsg());
    } else if (pingMessage.sender_name().empty()){
        chakra::net::Packet::fillError(pongMessage.mutable_error(), 1, "replica sender name is empty.");
    }else {
        // 回复Pong
        pongMessage.set_sender_name(clsptr->getMyself()->getName());
        link->setPeerName(pingMessage.sender_name());
        link->setState(chakra::replica::Replicate::Link::State::CONNECTED);
    }
    chakra::net::Packet::serialize(pongMessage, proto::types::R_PONG, cbf);
}
