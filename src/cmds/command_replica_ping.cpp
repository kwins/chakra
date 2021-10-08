//
// Created by kwins on 2021/7/2.
//

#include "command_replica_ping.h"
#include <replica/replica_link.h>
#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "cluster/cluster.h"

void chakra::cmds::CommandReplicaPing::execute(char *req, size_t len, void *data,
                                               std::function<error::Error(char *, size_t)> cbf) {

    auto link = static_cast<replica::Link*>(data);
    link->setLastInteractionMs(utils::Basic::getNowMillSec());

    proto::replica::PongMessage pongMessage;
    proto::replica::PingMessage pingMessage;
    auto err = chakra::net::Packet::deSerialize(req, len, pingMessage, proto::types::R_PING);
    if (!err.success()){
        chakra::net::Packet::fillError(pongMessage.mutable_error(), err.getCode(), err.getMsg());
    } else if (pingMessage.db_name().empty()){
        chakra::net::Packet::fillError(pongMessage.mutable_error(), 1, "replica dbname is empty.");
    }else {
        // 回复Pong
        pongMessage.set_sender_name(cluster::Cluster::get()->getMyself()->getName());
        link->setPeerName(pingMessage.sender_name());
        link->setDbName(pingMessage.db_name());
    }
    chakra::net::Packet::serialize(pongMessage, proto::types::R_PONG, cbf);
}
