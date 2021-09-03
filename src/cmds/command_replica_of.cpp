//
// Created by kwins on 2021/6/30.
//

#include "command_replica_of.h"
#include "replica.pb.h"
#include "replica/replica.h"
#include "net/packet.h"
#include "types.pb.h"
#include "cluster/view.h"

void
chakra::cmds::CommandReplicaOf::execute(char *req, size_t len, void *data, std::function<utils::Error(char *, size_t)> cbf) {
    proto::replica::ReplicaOfMessage replicaOf;
    if (!chakra::net::Packet::deSerialize(req, len, replicaOf, proto::types::R_REPLICA_OF).success()) return;

    auto replicaptr = chakra::replica::Replica::get();
    proto::types::Error reply;
    if (replicaOf.db_name().empty()){
        chakra::net::Packet::fillError(reply, 1, "DB name Or Ip empty Or Bad Port");
    } else {
        // TODO: sforce
        auto clusptr = cluster::View::get();
        // find all db peers
        auto peers = clusptr->getPeers(replicaOf.db_name());
        for(auto& peer: peers){
            replicaptr->setReplicateDB(replicaOf.db_name(), peer->getIp(), peer->getPort());
        }
    }
    chakra::net::Packet::serialize(reply, proto::types::R_REPLICA_OF, cbf);
}

