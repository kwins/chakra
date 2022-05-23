//
// Created by kwins on 2021/6/2.
//

#include "command_cluster_meet_peer.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "cluster/cluster.h"

void chakra::cmds::CommandClusterMeetPeer::execute(char *req, size_t reqLen, void* data, std::function<error::Error(char *, size_t)> cbf) {
    proto::peer::GossipMessage gossip;
    auto err = chakra::net::Packet::deSerialize(req, reqLen, gossip, proto::types::P_MEET_PEER);
    if (err) return;
    LOG(INFO) << "Meet peer message received: " << gossip.DebugString();
    auto clsptr = cluster::Cluster::get();
    clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG);

    std::shared_ptr<cluster::Peer> sender = cluster::Cluster::get()->getPeer(gossip.sender().name());
    if (sender && !sender->isHandShake()) { // 更新纪元
        if (sender->getEpoch() < gossip.sender().config_epoch()) {
            sender->updateSelf(gossip.sender());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }

        if (clsptr->getCurrentEpoch() < gossip.sender().current_epoch()) {
            clsptr->setCurrentEpoch(gossip.sender().current_epoch());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }

    if (!sender) {
        clsptr->addPeer(gossip.sender().ip(), gossip.sender().port());
        LOG(INFO) << "Meet message received name=" << gossip.sender().name() << ",ip=" << gossip.sender().ip() << ",port=" << gossip.sender().port();
    }

    // 分析并取出消息中的 gossip 节点信息, 非sender
    clsptr->processGossip(gossip);

    // 向目标节点返回一个 PONG
    // 会带上当前节点的 name
    proto::peer::GossipMessage pong;
    clsptr->buildGossipMessage(pong, gossip.sender().data());
    chakra::net::Packet::serialize(pong, proto::types::P_PONG, cbf);
}
