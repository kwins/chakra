//
// Created by kwins on 2021/6/30.
//

#include "command_replica_of.h"
#include "replica.pb.h"
#include "replica/replica.h"
#include "net/packet.h"
#include "types.pb.h"
#include "cluster/cluster.h"

void
chakra::cmds::CommandReplicaOf::execute(char *req, size_t len, void *data, std::function<utils::Error(char *, size_t)> cbf) {
    proto::replica::ReplicaOfRequest replicaOf;
    proto::replica::ReplicaOfResponse replicaOfResponse;
    auto err = chakra::net::Packet::deSerialize(req, len, replicaOf, proto::types::R_REPLICA_OF);
    if (!err.success()){
        chakra::net::Packet::fillError(replicaOfResponse.mutable_error(), err.getCode(), err.getMsg());
        chakra::net::Packet::serialize(replicaOfResponse, proto::types::R_REPLICA_OF, cbf);
        return;
    }

    LOG(INFO) << "replica of request:" << replicaOf.DebugString();

    auto replicaptr = chakra::replica::Replica::get();
    if (replicaOf.db_name().empty()){
        chakra::net::Packet::fillError(replicaOfResponse.mutable_error(), 1, "DB name Or Ip empty Or Bad Port");
    } else {
        // TODO: sforce
        auto clusptr = cluster::Cluster::get();
        // find all db peers
        auto peers = clusptr->getPeers(replicaOf.db_name());
        if (peers.empty()){
            chakra::net::Packet::fillError(replicaOfResponse.mutable_error(), 1, "Cluster haven’t db " + replicaOf.db_name());
        } else if (peers.size() == 1 && peers[0]->isMyself()){
            chakra::net::Packet::fillError(replicaOfResponse.mutable_error(), 1, "Cluster haven’t db " + replicaOf.db_name() + " except myself.");
        } else {
            for(auto& peer: peers){
                if (peer->isMyself()) continue;
                replicaptr->setReplicateDB(replicaOf.db_name(), peer->getIp(), peer->getPort());
            }
        }
    }
    chakra::net::Packet::serialize(replicaOfResponse, proto::types::R_REPLICA_OF, cbf);
}

