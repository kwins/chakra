//
// Created by kwins on 2021/9/26.
//

#include "command_replica_pong.h"
#include "replica/replica_link.h"
#include "replica.pb.h"
#include "utils/basic.h"
#include "net/packet.h"
#include "types.pb.h"
void chakra::cmds::CommandReplicaPong::execute(char *req, size_t len, void *data,
                                               std::function<utils::Error(char *, size_t)> cbf) {
    auto link = static_cast<chakra::replica::Link*>(data);
    if (link->getState() != chakra::replica::Link::State::RECEIVE_PONG){
        LOG(ERROR) << "command replica pong handshake state is not RECEIVE_PONG but " << (int)link->getState();
        return;
    }

    LOG(INFO) << "REPL state RECEIVE_PONG";
    link->setLastInteractionMs(utils::Basic::getNowMillSec());

    proto::replica::PongMessage pong;
    auto err = chakra::net::Packet::deSerialize(req, len, pong, proto::types::R_PONG);
    if (!err.success()){
        LOG(ERROR) << "replica handshake deserilize requesst error " << err.toString();
    } else {
        link->setPeerName(pong.sender_name());
        link->tryPartialReSync();
    }
}
