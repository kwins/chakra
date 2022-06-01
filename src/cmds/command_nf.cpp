//
// Created by kwins on 2021/5/27.
//

#include "command_nf.h"
#include "net/packet.h"
#include "types.pb.h"

void chakra::cmds::CommandNF::execute(char *req, size_t len,void* data) {
    proto::types::Error reply;
    fillError(reply, 1, "Server command not define");
    // TODO: how deal not found command ?
    // chakra::net::Packet::serialize(reply, proto::types::NF, cbf);
}
