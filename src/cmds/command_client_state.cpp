//
// Created by kwins on 2021/9/18.
//

#include "command_client_state.h"
#include "peer.pb.h"
#include "net/packet.h"
#include <service/chakra.h>

void chakra::cmds::CommandClientState::execute(char *req, size_t reqLen, void *data) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(data);
    proto::peer::StateMessageRequest stateMessageRequest;
    proto::peer::StateMessageResponse stateMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, stateMessageRequest, proto::types::C_STATE);
    if (err) {
        chakra::net::Packet::fillError(stateMessageResponse.mutable_error(), 1, err.what());
    } else {
        DLOG(INFO) << "[cluster] state request: " << stateMessageRequest.DebugString();
        auto clsptr = chakra::cluster::Cluster::get();
        clsptr->stateDesc(*stateMessageResponse.mutable_state());
        DLOG(INFO) << "[cluster] state response: " << stateMessageResponse.DebugString();
    }
    link->asyncSendMsg(stateMessageResponse, proto::types::C_STATE);
}
