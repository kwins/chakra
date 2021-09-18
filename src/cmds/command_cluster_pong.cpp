//
// Created by kwins on 2021/6/2.
//

#include "command_cluster_pong.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "cluster/cluster.h"
#include "cluster/peer.h"
#include "utils/basic.h"

void chakra::cmds::CommandClusterPong::execute(char *req, size_t len, void* data, std::function<utils::Error(char *, size_t)> reply) {
    proto::peer::GossipMessage gossip;
    if (!chakra::net::Packet::deSerialize(req, len, gossip, proto::types::P_PONG).success()) return;

    auto clsptr = cluster::Cluster::get();
    auto sender = clsptr->getPeer(gossip.sender().name());
    if (sender && !sender->isHandShake()){
        // 更新纪元
        if (sender->getEpoch() < gossip.sender().config_epoch()){
            sender->updateSelf(gossip.sender());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }

        if (clsptr->getCurrentEpoch() < gossip.sender().current_epoch()){
            clsptr->setCurrentEpoch(gossip.sender().current_epoch());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }

    if (!gossip.sender().data().empty()){
        auto peer = clsptr->renamePeer(gossip.sender().data(), gossip.sender().name());
        if (peer){
            peer->delFlag(cluster::Peer::FLAG_HANDSHAKE);
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
            LOG(INFO) << "Handshake with peer " << gossip.sender().name() << " completed.";
        }
    }

    if (sender){
        sender->setLastPongRecv(utils::Basic::getNowMillSec());
        sender->setLastPingSend(0);
        if (sender->isPfail() || sender->isFail()){
            if (sender->isPfail()){
                sender->delFlag(cluster::Peer::FLAG_PFAIL);
                LOG(INFO) << "*** Receive PONG from " << sender->getName() <<  " remove PFAIL flag.";
            } else if (sender->isFail()){
                sender->delFlag(cluster::Peer::FLAG_FAIL);
                LOG(INFO) << "*** Receive PONG from " << sender->getName() << " remove FAIL flag.";
            }
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }
    clsptr->processGossip(gossip);
}

