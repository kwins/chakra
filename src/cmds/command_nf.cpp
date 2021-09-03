//
// Created by kwins on 2021/5/27.
//

#include "command_nf.h"
#include "net/packet.h"
#include "types.pb.h"

void chakra::cmds::CommandNF::execute(char *req, size_t len,void* data, std::function<utils::Error(char *, size_t)> cbf) {
    proto::types::Error reply;
    chakra::net::Packet::fillError(reply, 1, "Command Not Define");
    chakra::net::Packet::serialize(reply, proto::types::NF, cbf);
}
