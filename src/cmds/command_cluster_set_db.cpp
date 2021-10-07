//
// Created by kwins on 2021/7/16.
//

#include "command_cluster_set_db.h"
#include "net/packet.h"
#include "peer.pb.h"
#include "database/db_family.h"
#include "cluster/cluster.h"
#include "types.pb.h"
#include "replica/replica.h"

void chakra::cmds::CommandClusterSetDB::execute(char *req, size_t reqLen, void *data,
                                                std::function<utils::Error(char *, size_t)> cbf) {

    proto::peer::DBSetMessageRequest dbSetMessage;
    proto::peer::DBSetMessageResponse dbSetMessageResponse;
    auto clsptr = cluster::Cluster::get();
    auto& dbptr = database::FamilyDB::get();
    auto replicaptr = chakra::replica::Replica::get();

    auto err = chakra::net::Packet::deSerialize(req, reqLen, dbSetMessage, proto::types::P_SET_DB);
    if (!err.success()){
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), err.getCode(), err.getMsg());
    } else if (!clsptr->stateOK()){ /* 集群状态正常 */
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "Cluster state fail");
    }else if (dbSetMessage.db().name().empty() || dbSetMessage.db().cached() <= 0){
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "Bad request");
    } else if (dbptr.servedDB(dbSetMessage.db().name())
            || clsptr->getMyself()->servedDB(dbSetMessage.db().name())){ /* 当前节点没有处理请求的DB */
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "Cluster has served db " + dbSetMessage.db().name());
    } else {
        /* 找到集群中目前正在处理此DB的节点, 开始复制流程 */
        auto info = dbSetMessage.db();
        auto peers = clsptr->getPeers(dbSetMessage.db().name());
        if (!peers.empty()){ // 非空至少有两个
            for(auto& peer: peers){
                if (peer->isMyself()) continue;
                replicaptr->setReplicateDB(dbSetMessage.db().name(), peer->getIp(), peer->getPort() + 1);
                LOG(INFO) << "Cluster have been " << dbSetMessage.db().name()
                          << " db before set it, start replica from ("
                          << peer->getIp() << ":" << peer->getPort() + 1
                          << ") procedure.";
            }
            info.set_state(proto::peer::MetaDB_State_INIT);
        } else {
            info.set_state(proto::peer::MetaDB_State_ONLINE);
        }

        clsptr->setMyselfDB(info);
        LOG(INFO) << "Cluster set db " << dbSetMessage.db().name()
                  << " state " << proto::peer::MetaDB_State_Name(info.state());
    }
    chakra::net::Packet::serialize(dbSetMessageResponse, proto::types::P_SET_DB, cbf);
}
