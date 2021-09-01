//
// Created by kwins on 2021/7/14.
//

#include "command_cluster_update_db.h"
#include "net/packet.h"
#include "peer.pb.h"
#include "cluster/peer.h"
#include "cluster/view.h"

void chakra::cmds::CommandPeerUpdateDB::execute(char *req, size_t reqLen, void *data,
                                                std::function<void(char *, size_t)> cbf) {
    proto::peer::DBMessage dbMessage;
    if (!chakra::net::Packet::deSerialize(req, reqLen, dbMessage)){
        return;
    }

    auto sender = cluster::View::get()->getPeer(dbMessage.sender().name());
    if (!sender){
        LOG(WARNING) << "Not found peer when update peer DB " << dbMessage.sender().name() << " host " << dbMessage.sender().ip() << ":" << dbMessage.sender().port();
        return;
    }
    auto peer = cluster::View::get()->getPeer(dbMessage.name());
    if (!peer) return;

    if (dbMessage.action() == proto::peer::DBAction::ADD){
        cluster::Peer::DB db;
        db.name = dbMessage.info().name();
        db.cached = dbMessage.info().cached();
        db.memory = dbMessage.info().memory();
        db.shardSize = dbMessage.info().shard_size();
        db.shard = dbMessage.info().shard();
        peer->setDB(dbMessage.name(), db);
    } else if (dbMessage.action() == proto::peer::DBAction::DEL){
        peer->removeDB(dbMessage.info().name());
    }
}
