//
// Created by kwins on 2021/6/3.
//

#include "command_cluster_fail.h"
#include "cluster/cluster.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "cluster/peer.h"
#include "utils/basic.h"

void chakra::cmds::CommandClusterFail::execute(char *req, size_t len, void *data) {
    auto link = static_cast<chakra::cluster::Peer::Link*>(data);
    proto::peer::FailMessage failMessage;
    auto err = chakra::net::Packet::deSerialize(req, len, failMessage, proto::types::P_FAIL);
    if (err) return;
    DLOG(INFO) << "[cluster] fail message request: " << failMessage.DebugString();
    auto clsptr = cluster::Cluster::get();
    auto sender = clsptr->getPeer(failMessage.sender().name());
    if (sender && !sender->isHandShake()) {
        // 更新纪元
        if (sender->getEpoch() < failMessage.sender().config_epoch()) {
            sender->updateSelf(failMessage.sender());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }

        if (clsptr->getCurrentEpoch() < failMessage.sender().current_epoch()) {
            clsptr->setCurrentEpoch(failMessage.sender().current_epoch());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
        }
    }

    if (sender) {
        auto failing = clsptr->getPeer(failMessage.fail_peer_name());
        if (failing && !failing->isMyself() && !failing->isFail()){
            failing->delFlag(cluster::Peer::FLAG_PFAIL);
            failing->setFlag(cluster::Peer::FLAG_FAIL);
            failing->setFailTime(utils::Basic::getNowMillSec());
            clsptr->setCronTODO(cluster::Cluster::FLAG_SAVE_CONFIG | cluster::Cluster::FLAG_UPDATE_STATE);
       } else {
           LOG(INFO) << "[cluster] fail message ignored from " << failMessage.sender().name();
       }
    } else {
        LOG(WARNING) << "[cluster] message from unknonw peer " << failMessage.sender().name() << " about " << failMessage.fail_peer_name();
    }
}

