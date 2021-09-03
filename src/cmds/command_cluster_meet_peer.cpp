//
// Created by kwins on 2021/6/2.
//

#include "command_cluster_meet_peer.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "cluster/view.h"

void chakra::cmds::CommandClusterMeetPeer::execute(char *req, size_t reqLen, void* data, std::function<utils::Error(char *, size_t)> cbf) {
    proto::peer::GossipMessage gossip;
    if (!chakra::net::Packet::deSerialize(req, reqLen, gossip, proto::types::P_MEET_PEER).success()) return;

    uint64_t todo = 0;
    todo |= cluster::View::FLAG_SAVE_CONFIG;

    std::shared_ptr<cluster::Peer> sender = cluster::View::get()->getPeer(gossip.sender().name());
    if (sender && !sender->isHandShake()){
        LOG(INFO) << "## Peer meet FIND exsit peer " << sender->getName();
        // 更新纪元
        if (sender->getEpoch() < gossip.sender().config_epoch())
            sender->setEpoch(gossip.sender().config_epoch());

        if (cluster::View::get()->getCurrentEpoch() < gossip.sender().current_epoch()){
            cluster::View::get()->setCurrentEpoch(gossip.sender().current_epoch());
            todo |= (cluster::View::FLAG_SAVE_CONFIG | cluster::View::FLAG_UPDATE_STATE);
        }
    }

    if (!sender){
        cluster::View::get()->addPeer(gossip.sender().ip(), gossip.sender().port());
        LOG(INFO) << "Meet packet received, name=" << gossip.sender().name() << ",ip=" << gossip.sender().ip() << ",name=" << gossip.sender().port();
    }

    // 分析并取出消息中的 gossip 节点信息, 非sender
    cluster::View::get()->processGossip(gossip);

    // 向目标节点返回一个 PONG
    // 会带上当前节点的 name
    proto::peer::GossipMessage pong;
    cluster::View::get()->buildGossipMessage(pong, gossip.sender().data());
    chakra::net::Packet::serialize(pong, proto::types::P_PONG, cbf);

    if (todo) cluster::View::get()->setCronTODO(todo);
}
