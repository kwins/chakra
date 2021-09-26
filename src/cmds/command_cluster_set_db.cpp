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
    }else if (dbptr.servedDB(dbSetMessage.db().name()) || clsptr->getMyself()->servedDB(dbSetMessage.db().name())){ /* 当前节点没有处理请求的DB */
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "Cluster has served db " + dbSetMessage.db().name());
    } else {
        /* 找到集群中目前正在处理此DB的节点, 开始复制流程 */
        auto peers = clsptr->getPeers(dbSetMessage.db().name());
        if (!peers.empty()){
            for(auto& peer: peers){
                if (peer->isMyself()) continue;
                replicaptr->setReplicateDB(dbSetMessage.db().name(), peer->getIp(), peer->getPort() + 1);
                LOG(INFO) << "Cluster have been " << dbSetMessage.db().name()
                          << " db before set it, start replica from ("
                          << peer->getIp() << ":" << peer->getPort() + 1
                          << ") procedure.";
            }
        } else { /* 新的DB */
            dbptr.addDB(dbSetMessage.db().name(), dbSetMessage.db().cached());
            cluster::Peer::DB info;
            info.name = dbSetMessage.db().name();
            info.shard = dbSetMessage.db().shard();
            info.shardSize = dbSetMessage.db().shard_size();
            info.memory = dbSetMessage.db().memory();
            info.cached = dbSetMessage.db().cached();
            clsptr->getMyself()->setDB(dbSetMessage.db().name(), info);

            int maxEpoch = clsptr->getMaxEpoch();
            if (maxEpoch >= clsptr->getCurrentEpoch()){
                clsptr->setCurrentEpoch(maxEpoch + 1);
            } else {
                clsptr->setCurrentEpoch(clsptr->getCurrentEpoch() + 1);
            }
            clsptr->getMyself()->setEpoch(maxEpoch + 1);
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }
    chakra::net::Packet::serialize(dbSetMessageResponse, proto::types::P_SET_DB, cbf);
}
