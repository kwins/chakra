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

    uint64_t todo = 0;
    std::shared_ptr<cluster::Peer> sender = cluster::Cluster::get()->getPeer(gossip.sender().name());
    if (sender && !sender->isHandShake()){
        // 更新纪元
        if (sender->getEpoch() < gossip.sender().config_epoch())
            sender->setEpoch(gossip.sender().config_epoch());

        if (cluster::Cluster::get()->getCurrentEpoch() < gossip.sender().current_epoch()){
            cluster::Cluster::get()->setCurrentEpoch(gossip.sender().current_epoch());
            todo |= (cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }

    if (!gossip.sender().data().empty()){
        auto peer = cluster::Cluster::get()->renamePeer(gossip.sender().data(), gossip.sender().name());
        if (peer){
            peer->delFlag(cluster::Peer::FLAG_HANDSHAKE);
            todo |= (cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
            LOG(INFO) << "Handshake with peer " << gossip.sender().name() << " completed.";
        }
    }

    sender = cluster::Cluster::get()->getPeer(gossip.sender().name());
    if (sender){
        sender->setLastPongRecv(utils::Basic::getNowMillSec());
        sender->setLastPingSend(0);
        if (sender->isPfail() || sender->isFail()){
            if (sender->isPfail()){
                sender->delFlag(cluster::Peer::FLAG_PFAIL);
                LOG(INFO) << "%% " << sender->getName() <<  " remove PFAIL flag.";
            } else if (sender->isFail()){
                sender->delFlag(cluster::Peer::FLAG_FAIL);
                LOG(INFO) << "%%" << sender->getName() << " remove FAIL flag.";
            }
            todo |= (cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }

    if (todo) cluster::Cluster::get()->setCronTODO(todo);
}

