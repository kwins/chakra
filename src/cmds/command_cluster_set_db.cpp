//
// Created by kwins on 2021/7/16.
//

#include "command_cluster_set_db.h"
#include "net/packet.h"
#include "peer.pb.h"
#include "database/db_family.h"
#include "cluster/cluster.h"
#include "types.pb.h"

void chakra::cmds::CommandClusterSetDB::execute(char *req, size_t reqLen, void *data,
                                                std::function<utils::Error(char *, size_t)> cbf) {

    proto::peer::DBSetMessageRequest dbSetMessage;
    proto::peer::DBSetMessageResponse dbSetMessageResponse;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, dbSetMessage, proto::types::P_SET_DB);
    if (!err.success()){
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), err.getCode(), err.getMsg());
        chakra::net::Packet::serialize(dbSetMessageResponse, proto::types::P_SET_DB, cbf);
        return;
    }

    auto& dbptr = database::FamilyDB::get();
    auto clsptr = cluster::Cluster::get();
    auto myself = cluster::Cluster::get()->getMyself();

    if (dbSetMessage.dbs_size() == 0){
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "request db size is zero");
        chakra::net::Packet::serialize(dbSetMessageResponse, proto::types::P_SET_DB, cbf);
        return;
    }

    for (int i = 0; i < dbSetMessage.dbs_size(); ++i) {
        auto& db = dbSetMessage.dbs(i);
        if (dbptr.servedDB(db.name()) || myself->servedDB(db.name())){
            chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "db has been served:" + db.name());
            chakra::net::Packet::serialize(dbSetMessageResponse, proto::types::P_SET_DB, cbf);
            return;
        }
    }

    for (int i = 0; i < dbSetMessage.dbs_size(); ++i) {
        auto& db = dbSetMessage.dbs(i);
        dbptr.addDB(db.name());
        cluster::Peer::DB info;
        info.name = db.name();
        info.shard = db.shard();
        info.shardSize = db.shard_size();
        info.memory = db.memory();
        info.cached = db.cached();
        myself->setDB(db.name(), info);
    }

    int maxEpoch = clsptr->getMaxEpoch();
    clsptr->setCurrentEpoch(maxEpoch + 1);
    myself->setEpoch(maxEpoch + 1);
    clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG);
    chakra::net::Packet::serialize(dbSetMessageResponse, proto::types::P_SET_DB, cbf);
}
