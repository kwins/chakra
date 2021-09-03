//
// Created by kwins on 2021/6/2.
//

#include "command_cluster_ping.h"
#include "peer.pb.h"
#include "cluster/view.h"
#include "net/packet.h"

void chakra::cmds::CommandClusterPing::execute(char *req, size_t len, void* data, std::function<utils::Error(char *, size_t)> reply) {
    proto::peer::GossipMessage gossip;
    if (!chakra::net::Packet::deSerialize(req, len, gossip, proto::types::P_PING).success()) return;

    uint64_t todo = 0;
    auto clsptr = cluster::View::get();
    std::shared_ptr<cluster::Peer> sender = clsptr->getPeer(gossip.sender().name());
    if (sender && !sender->isHandShake()){
        // 更新纪元
        if (sender->getEpoch() < gossip.sender().config_epoch())
            sender->setEpoch(gossip.sender().config_epoch());

        if (cluster::View::get()->getCurrentEpoch() < gossip.sender().current_epoch()){
            cluster::View::get()->setCurrentEpoch(gossip.sender().current_epoch());
            todo |= (cluster::View::FLAG_SAVE_CONFIG | cluster::View::FLAG_UPDATE_STATE);
        }
    }

    clsptr->processGossip(gossip);

    proto::peer::GossipMessage pong;
    cluster::View::get()->buildGossipMessage(pong);
    chakra::net::Packet::serialize(pong, proto::types::P_PONG, reply);

    if (todo) cluster::View::get()->setCronTODO(todo);
}