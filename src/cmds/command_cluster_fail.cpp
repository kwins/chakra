//
// Created by kwins on 2021/6/3.
//

#include "command_cluster_fail.h"
#include "cluster/cluster.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "cluster/peer.h"
#include "utils/basic.h"

void chakra::cmds::CommandClusterFail::execute(char *req, size_t len, void *data, std::function<utils::Error(char *, size_t)> reply) {
    proto::peer::FailMessage failMessage;
    if (!chakra::net::Packet::deSerialize(req, len, failMessage, proto::types::P_FAIL).success()) return;

    LOG(INFO) << "Receive fail message " << failMessage.DebugString();

    auto clsptr = cluster::Cluster::get();
    auto sender = cluster::Cluster::get()->getPeer(failMessage.sender().name());
    if (sender && !sender->isHandShake()){
        // 更新纪元
        if (sender->getEpoch() < failMessage.sender().config_epoch()){
            sender->updateSelf(failMessage.sender());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }

        if (clsptr->getCurrentEpoch() < failMessage.sender().current_epoch()){
            clsptr->setCurrentEpoch(failMessage.sender().current_epoch());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }

    if (sender){
        LOG(INFO) << "FAIL mesage sender is" << sender->getName();
        auto failing = clsptr->getPeer(failMessage.fail_peer_name());
        if (failing && !failing->isMyself() && !failing->isFail()){
            LOG(WARNING) << "FAIL message received from " << failMessage.sender().name() << " about " << failMessage.fail_peer_name();
           failing->delFlag(cluster::Peer::FLAG_PFAIL);
           failing->setFlag(cluster::Peer::FLAG_FAIL);
           failing->setFailTime(utils::Basic::getNowMillSec());
           clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
       }
    } else {
        LOG(WARNING) << "Ignoring FAIL message from unknonw peer " << failMessage.sender().name() << " about " << failMessage.fail_peer_name();
    }
}

