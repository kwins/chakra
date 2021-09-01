//
// Created by kwins on 2021/7/2.
//

#include "command_replica_ping.h"
#include <replica/replica_link.h>
#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"

void chakra::cmds::CommandReplicaPing::execute(char *req, size_t len, void *data,
                                               std::function<void(char *, size_t)> reply) {

    auto link = static_cast<replica::Link*>(data);

    // 回复Pong
    proto::replica::PongMessage pongMessage;
    pongMessage.set_pong_ms(utils::Basic::getNowMillSec());
    pongMessage.set_my_name("");
    link->setLastInteractionMs(pongMessage.pong_ms());
    chakra::net::Packet::serialize(pongMessage, proto::types::R_PONG, reply);
}
