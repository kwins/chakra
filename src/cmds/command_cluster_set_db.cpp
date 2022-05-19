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
                                                std::function<error::Error(char *, size_t)> cbf) {

    proto::peer::DBSetMessageRequest dbSetMessage;
    proto::peer::DBSetMessageResponse dbSetMessageResponse;
    auto clsptr = cluster::Cluster::get();
    auto dbptr = database::FamilyDB::get();
    auto replicaptr = chakra::replica::Replicate::get();
    auto myself = clsptr->getMyself();

    auto err = chakra::net::Packet::deSerialize(req, reqLen, dbSetMessage, proto::types::P_SET_DB);
    if (err) {
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, err.what());
    } else if (!clsptr->stateOK()){ /* 集群状态正常 */
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "Cluster state fail");
    } else if (dbSetMessage.db().name().empty() || dbSetMessage.db().cached() <= 0){ /* 请求参数正常 */
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "Bad request");
    } else if (dbptr->servedDB(dbSetMessage.db().name())
            || myself->servedDB(dbSetMessage.db().name())){ /* 当前节点没有处理请求的DB */
        chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "Cluster has been served db " + dbSetMessage.db().name());
    } else {
        /* 找到集群中目前正在处理此DB的节点,创建新的DB 或者 开始复制流程 */
        auto info = dbSetMessage.db();
        auto peers = clsptr->getPeers(dbSetMessage.db().name());
        int num = 0;
        for (auto& peer : peers) {
            if (replicaptr->replicatedDB(peer->getName(), dbSetMessage.db().name()) ||
                    !peer->connected())
                num++;
        }

        if (num > 0 && num == peers.size()) { /* 有这个DB的所有后节点都已经复制了 */
            chakra::net::Packet::fillError(dbSetMessageResponse.mutable_error(), 1, "All peers has been served db or down" + dbSetMessage.db().name());
        } else {
            if (!peers.empty()) { /* 因为是多主，所以需要复制所有节点 */
                for(auto& peer: peers) {
                    if (replicaptr->replicatedDB(peer->getName(), dbSetMessage.db().name()))
                        continue;
                    if (!peer->connected()) continue;
                    auto err = replicaptr->setReplicateDB(peer->getName(), dbSetMessage.db().name(), 
                                    peer->getIp(), peer->getReplicatePort());
                    if (!err) {
                        LOG(INFO) << "Replicate db " << dbSetMessage.db().name() << " FROM " << peer->getName();
                    }
                }
                info.set_state(proto::peer::MetaDB_State_INIT);
            } else { /* 说明是一个新的 DB，需要自己复制自己 */
                replicaptr->setReplicateDB(myself->getName(), dbSetMessage.db().name(), 
                            myself->getIp(), myself->getReplicatePort());
                info.set_state(proto::peer::MetaDB_State_ONLINE);
                LOG(INFO) << "Replicate db " << dbSetMessage.db().name() << " FROM myself " << myself->getName();
            }

            dbptr->addDB(info.name(), info.cached());
            clsptr->updateMyselfDB(info); /* 更新集群信息 */
            replicaptr->dumpReplicateStates(); /* 更新复制信息 */
            LOG(INFO) << "Cluster set db " << dbSetMessage.db().name()
                    << " state " << proto::peer::MetaDB_State_Name(info.state());
        }
    }
    chakra::net::Packet::serialize(dbSetMessageResponse, proto::types::P_SET_DB, cbf);
}
