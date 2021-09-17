//
// Created by kwins on 2021/6/2.
//

#include "command_cluster_ping.h"
#include "peer.pb.h"
#include "cluster/cluster.h"
#include "net/packet.h"

void chakra::cmds::CommandClusterPing::execute(char *req, size_t len, void* data, std::function<utils::Error(char *, size_t)> reply) {
    proto::peer::GossipMessage gossip;
    if (!chakra::net::Packet::deSerialize(req, len, gossip, proto::types::P_PING).success()) return;

    LOG(INFO) << "Receive ping message " << gossip.DebugString();
    auto clsptr = cluster::Cluster::get();
    std::shared_ptr<cluster::Peer> sender = clsptr->getPeer(gossip.sender().name());
    if (sender && !sender->isHandShake()){
        // 更新对 sender 节点的认知
        LOG(INFO) << "recv ping sender->getEpoch()=" << sender->getEpoch() << " gossip.sender().config_epoch()=" << gossip.sender().config_epoch();
        if (sender->getEpoch() < gossip.sender().config_epoch()){
            sender->updateSelf(gossip.sender());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }

        if (clsptr->getCurrentEpoch() < gossip.sender().current_epoch()){
            clsptr->setCurrentEpoch(gossip.sender().current_epoch());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }

    clsptr->processGossip(gossip);

    proto::peer::GossipMessage pong;
    cluster::Cluster::get()->buildGossipMessage(pong);
    chakra::net::Packet::serialize(pong, proto::types::P_PONG, reply);
}