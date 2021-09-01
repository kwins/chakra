//
// Created by kwins on 2021/7/16.
//

#include "command_cluster_set_db.h"
#include "net/packet.h"
#include "peer.pb.h"
#include "database/db_family.h"
#include "cluster/view.h"
#include "types.pb.h"

void chakra::cmds::CommandClusterSetDB::execute(char *req, size_t reqLen, void *data,
                                                std::function<void(char *, size_t)> cbf) {

    proto::peer::DBSetMessage dbSetMessage;
    if (!chakra::net::Packet::deSerialize(req, reqLen, dbSetMessage)) return;

    proto::types::Error reply;
    chakra::net::Packet::fillError(reply, 0, "success");

    auto dbptr = database::FamilyDB::get();
    auto myself = cluster::View::get()->getMyself();

    for (int i = 0; i < dbSetMessage.dbs_size(); ++i) {
        auto& db = dbSetMessage.dbs(i);
        if (dbptr->servedDB(db.name()) || myself->servedDB(db.name())){
            continue;
        }

        dbptr->addDB(db.name());

        cluster::Peer::DB info;
        info.name = db.name();
        info.shard = db.shard();
        info.shardSize = db.shard_size();
        info.memory = db.memory();
        info.cached = db.cached();
        myself->setDB(db.name(), info);
    }

    chakra::net::Packet::serialize(reply, proto::types::P_SET_DB, cbf);
}
