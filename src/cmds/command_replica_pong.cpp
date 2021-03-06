//
// Created by kwins on 2021/9/26.
//

#include "command_replica_pong.h"
#include "replica/replica.h"
#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "types.pb.h"

void chakra::cmds::CommandReplicaPong::execute(char *req, size_t len, void *data) {
    auto link = static_cast<chakra::replica::Replicate::Link*>(data);
    if (link->getState() != chakra::replica::Replicate::Link::State::CONNECTING) {
        LOG(ERROR) << "[replication] command replica pong handshake state is not SEND_PING but " << (int)link->getState();
        return;
    }

    link->setLastInteractionMs(utils::Basic::getNowMillSec());

    proto::replica::PongMessage pong;
    auto err = chakra::net::Packet::deSerialize(req, len, pong, proto::types::R_PONG);
    if (err) {
        LOG(ERROR) << "[replication] handshake deserilize request error " << err.what();
    } else if (pong.error().errcode() != 0) {
        LOG(ERROR) << "[replication] receive bad pong message " << pong.DebugString();
    } else {
        link->setState(chakra::replica::Replicate::Link::State::CONNECTED);
        link->setPeerName(pong.sender_name());
        LOG(INFO) << "[replication] receive PONG message " << pong.DebugString();
    }
}
